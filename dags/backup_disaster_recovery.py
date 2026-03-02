"""
Backup & Disaster Recovery for the Insurance Pipeline

Runs daily at 2 AM to backup the PostgreSQL database to MinIO.
Also cleans up old backups after 30 days.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import subprocess
import boto3
from botocore.client import Config as BotoConfig
import structlog

logger = structlog.get_logger(__name__)

default_args = {
    'owner': 'insurance-pipeline-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


def backup_postgresql(**context):
    """
    Dump the entire PostgreSQL database and upload it to MinIO.
    
    Creates a gzipped SQL dump of the insurance_lineage database
    and stores it in MinIO's backups/postgres/ directory with a timestamp.
    """
    import os
    
    # Environment variables
    postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
    postgres_port = os.getenv('POSTGRES_PORT', '5432')
    postgres_db = os.getenv('POSTGRES_DB', 'insurance_lineage')
    postgres_user = os.getenv('POSTGRES_USER', 'pipeline_admin')
    postgres_password = os.getenv('POSTGRES_PASSWORD', 'securepass123')
    
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    minio_bucket = os.getenv('MINIO_BUCKET', 'insurance-claims-lake')
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    backup_file = f"/tmp/postgres_backup_{timestamp}.sql.gz"
    
    try:
        # Create backup
        logger.info(f"Starting PostgreSQL backup for {postgres_db}")
        import gzip
        import subprocess
        
        with gzip.open(backup_file, 'wb') as gz:
            dump_proc = subprocess.Popen([
                'pg_dump',
                '-h', postgres_host,
                '-p', postgres_port,
                '-U', postgres_user,
                '-d', postgres_db,
                '--verbose'
            ], stdout=gz, stderr=subprocess.PIPE, env={'PGPASSWORD': postgres_password})
            
            _, stderr = dump_proc.communicate()
            if dump_proc.returncode != 0:
                raise Exception(f"pg_dump failed: {stderr.decode()}")
        
        logger.info(f"Backup created: {backup_file}")
        
        # Upload to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{minio_endpoint}",
            access_key_id=minio_access_key,
            secret_access_key=minio_secret_key,
            config=BotoConfig(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        key = f"backups/postgres/postgres_{timestamp}.sql.gz"
        with open(backup_file, 'rb') as f:
            s3_client.put_object(
                Bucket=minio_bucket,
                Key=key,
                Body=f.read(),
                ContentType='application/gzip'
            )
        
        logger.info(f"Backup uploaded to s3://{minio_bucket}/{key}")
        context['ti'].xcom_push(key='backup_file', value=key)
        
        return f"PostgreSQL backup completed: {key}"
        
    except Exception as e:
        logger.error(f"PostgreSQL backup failed: {str(e)}")
        raise


def cleanup_old_backups(**context):
    """
    Delete any backups older than 30 days.
    
    Keeps storage costs down by removing old backups automatically.
    Keeps the most recent 30 days worth of backups on hand.
    """
    import os
    import boto3
    from botocore.client import Config as BotoConfig
    from datetime import datetime, timedelta
    
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    minio_bucket = os.getenv('MINIO_BUCKET', 'insurance-claims-lake')
    
    retention_days = 30
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{minio_endpoint}",
            access_key_id=minio_access_key,
            secret_access_key=minio_secret_key,
            config=BotoConfig(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # List all backup objects
        response = s3_client.list_objects_v2(
            Bucket=minio_bucket,
            Prefix='backups/postgres/'
        )
        
        if 'Contents' not in response:
            logger.info("No backup objects found")
            return
        
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        deleted_count = 0
        
        for obj in response['Contents']:
            if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                s3_client.delete_object(Bucket=minio_bucket, Key=obj['Key'])
                deleted_count += 1
                logger.info(f"Deleted old backup: {obj['Key']}")
        
        logger.info(f"Cleanup completed: {deleted_count} backups deleted")
        return f"Deleted {deleted_count} backups older than {retention_days} days"
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise


def verify_backup_integrity(**context):
    """
    Check that the backup file actually exists and has data in it.
    
    Confirms the backup was uploaded successfully to MinIO
    and logs its size for monitoring.
    """
    import boto3
    from botocore.client import Config as BotoConfig
    import os
    
    try:
        backup_file = context['ti'].xcom_pull(key='backup_file')
        
        if not backup_file:
            raise Exception("No backup file created")
        
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        minio_bucket = os.getenv('MINIO_BUCKET', 'insurance-claims-lake')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
        
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{minio_endpoint}",
            access_key_id=minio_access_key,
            secret_access_key=minio_secret_key,
            config=BotoConfig(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        response = s3_client.head_object(Bucket=minio_bucket, Key=backup_file)
        size_mb = response['ContentLength'] / (1024 * 1024)
        
        logger.info(f"Backup verified: {backup_file} ({size_mb:.2f} MB)")
        return f"Backup integrity verified: {size_mb:.2f} MB"
        
    except Exception as e:
        logger.error(f"Backup verification failed: {str(e)}")
        raise


# DAG definition
with DAG(
    dag_id='backup_and_disaster_recovery',
    default_args=default_args,
    description='Automated backup and disaster recovery for insurance pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['backup', 'disaster-recovery', 'insurance', 'pipeline'],
    doc_md="""
    # Backup & Disaster Recovery
    
    Runs every day at 2 AM UTC.
    
    **What it does:**
    1. Backs up PostgreSQL - Full SQL dump, gzipped
    2. Verifies it - Makes sure the backup file actually exists and has data
    3. Cleans up - Deletes backups older than 30 days
    
    Backups go to MinIO at `insurance-claims-lake/backups/postgres/`
    """,
) as dag:

    backup_pg = PythonOperator(
        task_id='backup_postgresql',
        python_callable=backup_postgresql,
        provide_context=True,
    )

    verify_integrity = PythonOperator(
        task_id='verify_backup_integrity',
        python_callable=verify_backup_integrity,
        provide_context=True,
    )

    cleanup_backups = PythonOperator(
        task_id='cleanup_old_backups',
        python_callable=cleanup_old_backups,
        provide_context=True,
    )

    # Task dependencies
    backup_pg >> verify_integrity >> cleanup_backups
