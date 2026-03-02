#!/usr/bin/env python3
"""
Master pipeline runner script.
Starts all components: producer, stream processor, and observability API.
"""

import sys
import os
import time
import threading
import signal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config
from src.observability.tracing import init_tracing
from src.observability.metrics import start_metrics_server

import structlog
logger = structlog.get_logger(__name__)

shutdown_event = threading.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def run_producer(batch_size=20, interval=10):
    """Run the claims producer in a loop."""
    from src.producers.claims_producer import InsuranceClaimsProducer

    producer = InsuranceClaimsProducer()
    batch_num = 0

    while not shutdown_event.is_set():
        batch_num += 1
        logger.info(f"Producing batch #{batch_num} ({batch_size} claims)")
        produced = producer.produce_batch(count=batch_size)
        logger.info(f"Batch #{batch_num} complete: {len(produced)} claims produced")
        shutdown_event.wait(timeout=interval)

    producer.close()
    logger.info("Producer stopped")


def run_processor():
    """Run the stream processor."""
    from src.processors.stream_processor import InsuranceClaimsStreamProcessor

    processor = InsuranceClaimsStreamProcessor()

    def check_shutdown():
        while not shutdown_event.is_set():
            time.sleep(1)
        processor.stop()

    shutdown_thread = threading.Thread(target=check_shutdown, daemon=True)
    shutdown_thread.start()

    processor.run(timeout_seconds=3600)
    logger.info("Stream processor stopped")


def run_api():
    """Run the observability API."""
    import uvicorn
    from src.api.main import app

    try:
        uvicorn.run(
            app,
            host=config.observability.api_host,
            port=config.observability.api_port,
            log_level="info",
        )
    except OSError as e:
        if "address already in use" in str(e).lower():
            logger.warning("API port already in use, skipping local API startup (Docker API should be running)")
        else:
            raise


def run_health_monitor():
    """Periodically check and log pipeline health."""
    from src.observability.health import health_monitor
    from src.lineage.models import get_session, PipelineHealthSnapshot

    while not shutdown_event.is_set():
        try:
            report = health_monitor.get_health_report()
            logger.info("Health check",
                        overall=report['overall_status'],
                        components={k: v['status'] for k, v in report['components'].items()
                                    if v.get('last_heartbeat')})

            # Store health snapshot
            session = get_session()
            try:
                for name, comp in report['components'].items():
                    if comp.get('last_heartbeat'):
                        snapshot = PipelineHealthSnapshot(
                            component_name=name,
                            status=comp['status'],
                            latency_ms=comp.get('latency_ms', 0),
                            throughput_per_sec=comp.get('throughput', 0),
                            error_rate=comp.get('error_rate', 0),
                        )
                        session.add(snapshot)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.warning("Health snapshot storage failed", error=str(e))
            finally:
                session.close()

        except Exception as e:
            logger.error("Health monitor error", error=str(e))

        shutdown_event.wait(timeout=30)

    logger.info("Health monitor stopped")


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║   Insurance Claims Pipeline - Observability Platform        ║
║   ──────────────────────────────────────────────────────    ║
║   Components:                                               ║
║     • Claims Producer (Kafka)                               ║
║     • Stream Processor (Validate → Fraud → Enrich)          ║
║     • Observability API (FastAPI :8080)                     ║
║     • Health Monitor                                        ║
║     • Metrics Server (Prometheus :8000)                     ║
║                                                              ║
║   Dashboards:                                               ║
║     • Grafana:       http://localhost:3000                   ║
║     • Jaeger:        http://localhost:16686                  ║
║     • Prometheus:    http://localhost:9090                   ║
║     • MinIO:         http://localhost:9001                   ║
║     • Airflow:       http://localhost:8081                   ║
║     • Pipeline API:  http://localhost:8080                   ║
║     • Dashboard:     http://localhost:8080/dashboard         ║
║     • Lineage:       http://localhost:8080/lineage/visualize ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Initialize tracing
    init_tracing("insurance-pipeline-master")

    # Start metrics server on alternative port to avoid conflicts
    try:
        start_metrics_server(8001)
    except OSError:
        logger.warning("Could not start metrics server, metrics available via API")

    threads = []

    # Start API in a thread (skip if already running in Docker)
    api_thread = threading.Thread(target=run_api, name="api-thread", daemon=True)
    api_thread.start()
    threads.append(api_thread)
    time.sleep(2)

    # Start health monitor
    health_thread = threading.Thread(target=run_health_monitor, name="health-thread", daemon=True)
    health_thread.start()
    threads.append(health_thread)

    # Start stream processor
    processor_thread = threading.Thread(target=run_processor, name="processor-thread", daemon=True)
    processor_thread.start()
    threads.append(processor_thread)
    time.sleep(2)

    # Start producer
    producer_thread = threading.Thread(target=run_producer,
                                       kwargs={'batch_size': 15, 'interval': 8},
                                       name="producer-thread", daemon=True)
    producer_thread.start()
    threads.append(producer_thread)

    logger.info("All components started. Press Ctrl+C to stop.")

    # Wait for shutdown
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1)
    except KeyboardInterrupt:
        pass

    logger.info("Shutting down all components...")
    shutdown_event.set()

    for t in threads:
        t.join(timeout=10)

    logger.info("Pipeline stopped. Goodbye!")


if __name__ == "__main__":
    main()