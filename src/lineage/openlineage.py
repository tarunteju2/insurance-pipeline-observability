"""
OpenLineage Standard Protocol Engine
Implements Linux Foundation OpenLineage 1.0 event models and facets for pipeline metadata.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field


class ProducerFacet(BaseModel):
    name: str = "insurance-pipeline-observability"
    version: str = "1.0.0"
    url: str = "https://github.com/tarunteju2/insurance-pipeline-observability"


class Job(BaseModel):
    namespace: str = "insurance.claims.pipeline"
    name: str
    facets: Dict[str, Any] = Field(default_factory=dict)


class Run(BaseModel):
    runId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    facets: Dict[str, Any] = Field(default_factory=dict)


class DatasetFacet(BaseModel):
    name: str
    namespace: str = "s3://insurance-claims-lake"
    facets: Dict[str, Any] = Field(default_factory=dict)


class OpenLineageRunEvent(BaseModel):
    """
    Compliant OpenLineage 1.0 RunEvent model for job execution lineage.
    """
    eventType: str  # START, RUNNING, COMPLETE, FAIL, ABORT
    eventTime: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    run: Run
    job: Job
    inputs: List[DatasetFacet] = Field(default_factory=list)
    outputs: List[DatasetFacet] = Field(default_factory=list)
    producer: str = "https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json"
    schemaURL: str = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"


class OpenLineageEmitter:
    """
    Emits standardized OpenLineage events for claims pipeline operations.
    """

    def __init__(self, namespace: str = "insurance.claims.pipeline"):
        self.namespace = namespace

    def create_run_event(
        self,
        job_name: str,
        event_type: str,
        run_id: Optional[str] = None,
        inputs: Optional[List[Dict[str, Any]]] = None,
        outputs: Optional[List[Dict[str, Any]]] = None,
        data_quality_metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generates an OpenLineage RunEvent dictionary.
        """
        run = Run(runId=run_id or str(uuid.uuid4()))
        if data_quality_metrics:
            run.facets["dataQualityMetrics"] = data_quality_metrics

        job = Job(namespace=self.namespace, name=job_name)

        input_datasets = [
            DatasetFacet(
                name=inp.get("name", "kafka.raw-claims"),
                namespace=inp.get("namespace", "kafka://localhost:9092"),
                facets=inp.get("facets", {}),
            )
            for inp in (inputs or [])
        ]

        output_datasets = [
            DatasetFacet(
                name=out.get("name", "postgres.processed_claims"),
                namespace=out.get("namespace", "postgresql://postgres:5432/insurance_lineage"),
                facets=out.get("facets", {}),
            )
            for out in (outputs or [])
        ]

        event = OpenLineageRunEvent(
            eventType=event_type,
            run=run,
            job=job,
            inputs=input_datasets,
            outputs=output_datasets,
        )

        return event.model_dump()
