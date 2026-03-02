"""
Insurance Claims Processing Pipeline for Airflow.

This DAG runs every 15 minutes and handles the full end-to-end processing:
- Checks that all infrastructure is healthy
- Produces a batch of simulated insurance claims
- Validates them, scores for fraud, and enriches with data
- Generates reports on how things went
- Stores everything to MinIO

Results are viewable in the Airflow UI and Grafana dashboard.
"""

import json
import time
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add project root to path so we can import our src modules
sys.path.insert(0, '/opt/airflow')

# Sensible defaults for all tasks in this pipeline
default_args = {
    'owner': 'insurance-pipeline-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}


# ==================== TASK FUNCTIONS ====================

def check_infrastructure_health(**context):
    """
    Check that all the critical components are up and running.
    
    Hits each service (Kafka, Postgres, MinIO, etc) and makes sure they respond.
    Fails the pipeline if anything critical is down.
    """
    from src.observability.health import health_monitor
    from src.observability.tracing import init_tracing

    init_tracing("airflow-health-check")
    report = health_monitor.get_health_report()

    overall = report['overall_status']
    components = report['components']

    healthy_count = sum(1 for c in components.values() if c.get('status') == 'healthy')
    total_count = len(components)

    context['ti'].xcom_push(key='health_report', value=report)
    context['ti'].xcom_push(key='healthy_components', value=healthy_count)

    print(f"Infrastructure Health: {overall}")
    print(f"Components: {healthy_count}/{total_count} healthy")
    for name, comp in components.items():
        print(f"  - {name}: {comp.get('status', 'unknown')} ({comp.get('latency_ms', 0):.1f}ms)")

    # Stop the pipeline if critical stuff is broken
    critical_components = ['kafka_broker', 'postgres_db']
    for cc in critical_components:
        if cc in components and components[cc].get('status') == 'down':
            raise Exception(f"Critical component {cc} is DOWN. Cannot proceed.")

    return overall


def produce_claims_batch(**context):
    """
    Generate a batch of fake insurance claims and send them to Kafka.
    
    Creates 50 random claims (or whatever CLAIMS_BATCH_SIZE is set to)
    and publishes them to the raw_claims Kafka topic.
    """
    from src.producers.claims_producer import InsuranceClaimsProducer
    from src.observability.tracing import init_tracing

    init_tracing("airflow-claims-producer")
    batch_size = int(os.getenv('CLAIMS_BATCH_SIZE', '50'))

    producer = InsuranceClaimsProducer()
    produced_ids = producer.produce_batch(count=batch_size)
    producer.close()

    context['ti'].xcom_push(key='produced_claim_ids', value=produced_ids)
    context['ti'].xcom_push(key='batch_size', value=len(produced_ids))

    print(f"Produced {len(produced_ids)} claims to Kafka")
    return len(produced_ids)


def run_stream_processing(**context):
    """
    Run the Kafka stream processor on the current batch.
    
    Takes whatever claims came from the producer and runs them through
    the full validation → fraud scoring → enrichment pipeline.
    Times out after 2 minutes so it doesn't hang.
    """
    from src.processors.stream_processor import InsuranceClaimsStreamProcessor
    from src.observability.tracing import init_tracing

    init_tracing("airflow-stream-processor")
    batch_size = context['ti'].xcom_pull(key='batch_size', task_ids='produce_claims')
    if not batch_size:
        batch_size = 50

    processor = InsuranceClaimsStreamProcessor()
    # Process with timeout to avoid hanging
    processor.run(max_messages=batch_size, timeout_seconds=120)

    print(f"Stream processing completed for {batch_size} claims")
    return batch_size


def generate_lineage_report(**context):
    """
    Print out the data lineage graph and statistics.
    
    Shows which systems have processed which claims,
    how many errors happened, and overall success rates.
    """
    from src.lineage.tracker import lineage_tracker
    from src.observability.tracing import init_tracing

    init_tracing("airflow-lineage-reporter")

    graph = lineage_tracker.get_full_lineage_graph()
    stats = graph.get('statistics', {})

    print("=" * 60)
    print("PIPELINE LINEAGE REPORT")
    print("=" * 60)
    print(f"Total Lineage Events:  {stats.get('total_lineage_events', 0)}")
    print(f"Successful Events:     {stats.get('successful_events', 0)}")
    print(f"Error Events:          {stats.get('error_events', 0)}")
    print(f"Success Rate:          {stats.get('success_rate', 0)}%")
    print(f"Pipeline Coverage:     {stats.get('pipeline_coverage_percent', 0)}%")
    print(f"Active Edges:          {stats.get('active_edges', 0)} / {stats.get('total_edges', 0)}")
    print("=" * 60)

    print("\nEdge Statistics:")
    for edge in graph.get('edges', []):
        print(f"  {edge['source']} → {edge['target']}: "
              f"{edge['record_count']} records, "
              f"{edge['avg_latency_ms']}ms avg, "
              f"{edge['error_count']} errors")

    context['ti'].xcom_push(key='lineage_stats', value=stats)
    return stats


def generate_claims_summary(**context):
    """
    Build a report of all claims processed so far.
    
    Counts how many are done, rejected, flagged for fraud.
    Also calculates totals and averages for the UI to display.
    """
    from src.lineage.models import get_session, ProcessedClaim
    from src.observability.tracing import init_tracing
    from sqlalchemy import func

    init_tracing("airflow-summary-generator")
    session = get_session()

    try:
        total = session.query(func.count(ProcessedClaim.claim_id)).scalar() or 0
        completed = session.query(func.count(ProcessedClaim.claim_id)).filter(
            ProcessedClaim.status == 'completed'
        ).scalar() or 0
        rejected = session.query(func.count(ProcessedClaim.claim_id)).filter(
            ProcessedClaim.status.in_(['validation_failed', 'rejected'])
        ).scalar() or 0
        flagged = session.query(func.count(ProcessedClaim.claim_id)).filter(
            ProcessedClaim.status == 'flagged_fraud'
        ).scalar() or 0
        total_amount = session.query(func.sum(ProcessedClaim.claim_amount)).scalar() or 0
        avg_fraud = session.query(func.avg(ProcessedClaim.fraud_score)).scalar() or 0

        summary = {
            'total_claims': total,
            'completed': completed,
            'rejected': rejected,
            'flagged_fraud': flagged,
            'total_amount': float(total_amount),
            'avg_fraud_score': float(avg_fraud),
            'completion_rate': round(completed / total * 100, 2) if total > 0 else 0,
            'generated_at': datetime.utcnow().isoformat(),
        }

        print("=" * 60)
        print("CLAIMS PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total Claims:      {total}")
        print(f"Completed:         {completed} ({summary['completion_rate']}%)")
        print(f"Rejected:          {rejected}")
        print(f"Fraud Flagged:     {flagged}")
        print(f"Total Amount:      ${total_amount:,.2f}")
        print(f"Avg Fraud Score:   {avg_fraud:.4f}")
        print("=" * 60)

        context['ti'].xcom_push(key='summary', value=summary)
        return summary

    finally:
        session.close()


def store_batch_report_to_s3(**context):
    """
    Write a summary report to MinIO with everything that happened this batch.
    
    Bundles up the claims summary, lineage stats, and infrastructure health
    into a single JSON file timestamped with the run ID.
    """
    import boto3
    from botocore.client import Config as BotoConfig

    summary = context['ti'].xcom_pull(key='summary', task_ids='generate_summary')
    lineage_stats = context['ti'].xcom_pull(key='lineage_stats', task_ids='lineage_report')
    health = context['ti'].xcom_pull(key='health_report', task_ids='check_infrastructure')

    report = {
        'report_type': 'batch_processing',
        'generated_at': datetime.utcnow().isoformat(),
        'dag_run_id': context['run_id'],
        'claims_summary': summary,
        'lineage_statistics': lineage_stats,
        'infrastructure_health': health,
    }

    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin123'),
        config=BotoConfig(signature_version='s3v4'),
        region_name='us-east-1'
    )

    key = f"reports/{datetime.utcnow().strftime('%Y/%m/%d')}/batch_report_{context['run_id']}.json"
    bucket = os.getenv('MINIO_BUCKET', 'insurance-claims-lake')

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(report, indent=2, default=str).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"Report stored: s3://{bucket}/{key}")
    except Exception as e:
        print(f"Warning: Could not store report to S3: {e}")


# ==================== DAG DEFINITION ====================

with DAG(
    dag_id='insurance_claims_pipeline',
    default_args=default_args,
    description='End-to-end insurance claims processing pipeline with observability and lineage',
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['insurance', 'claims', 'pipeline', 'observability', 'lineage'],
    doc_md="""
    # Insurance Claims Pipeline
    
    Runs every 15 minutes to process a batch of insurance claims.
    
    **What it does:**
    1. Checks infrastructure - Makes sure Kafka, Postgres, MinIO, etc are up
    2. Produces claims - Generates 50 test claims
    3. Stream processing - Runs them through validation → fraud detection → enrichment
    4. Lineage report - Shows what happened and which systems touched which claims
    5. Summary - Counts completed, rejected, flagged for fraud
    6. Store report - Saves everything to MinIO for archival
    """
) as dag:

    check_infra = PythonOperator(
        task_id='check_infrastructure',
        python_callable=check_infrastructure_health,
        provide_context=True,
    )

    produce_claims = PythonOperator(
        task_id='produce_claims',
        python_callable=produce_claims_batch,
        provide_context=True,
    )

    stream_process = PythonOperator(
        task_id='stream_processing',
        python_callable=run_stream_processing,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    lineage_report = PythonOperator(
        task_id='lineage_report',
        python_callable=generate_lineage_report,
        provide_context=True,
    )

    summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_claims_summary,
        provide_context=True,
    )

    store_report = PythonOperator(
        task_id='store_report_to_s3',
        python_callable=store_batch_report_to_s3,
        provide_context=True,
    )

    # Define task dependencies
    check_infra >> produce_claims >> stream_process >> [lineage_report, summary]
    [lineage_report, summary] >> store_report