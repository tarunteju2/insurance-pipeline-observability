"""
SQLAlchemy models for data lineage tracking.
"""

from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, BigInteger, Text,
    DateTime, ForeignKey, JSON, create_engine
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from src.config import config

Base = declarative_base()


class LineageNode(Base):
    __tablename__ = 'lineage_nodes'

    node_id = Column(String(255), primary_key=True)
    node_type = Column(String(50), nullable=False)
    node_name = Column(String(255), nullable=False)
    description = Column(Text)
    component = Column(String(100))
    kafka_topic = Column(String(255))
    meta_data = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class LineageEdge(Base):
    __tablename__ = 'lineage_edges'

    edge_id = Column(Integer, primary_key=True, autoincrement=True)
    source_node_id = Column(String(255), ForeignKey('lineage_nodes.node_id'))
    target_node_id = Column(String(255), ForeignKey('lineage_nodes.node_id'))
    transform_type = Column(String(100))
    description = Column(Text)
    record_count = Column(BigInteger, default=0)
    bytes_processed = Column(BigInteger, default=0)
    avg_latency_ms = Column(Float, default=0)
    error_count = Column(BigInteger, default=0)
    meta_data = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    last_active_at = Column(DateTime, default=datetime.utcnow)

    source_node = relationship("LineageNode", foreign_keys=[source_node_id])
    target_node = relationship("LineageNode", foreign_keys=[target_node_id])


class LineageEvent(Base):
    __tablename__ = 'lineage_events'

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    edge_id = Column(Integer, ForeignKey('lineage_edges.edge_id'))
    trace_id = Column(String(64))
    span_id = Column(String(32))
    claim_id = Column(String(255))
    event_type = Column(String(50))
    record_count = Column(Integer, default=1)
    latency_ms = Column(Float)
    status = Column(String(20), default='success')
    error_message = Column(Text)
    meta_data = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    edge = relationship("LineageEdge")


class ProcessedClaim(Base):
    __tablename__ = 'processed_claims'

    claim_id = Column(String(255), primary_key=True)
    policy_number = Column(String(100), nullable=False)
    claimant_name = Column(String(255))
    claim_type = Column(String(50))
    claim_amount = Column(Float)
    date_of_loss = Column(String(20))
    date_filed = Column(String(20))
    description = Column(Text)
    status = Column(String(50), default='submitted')
    provider_name = Column(String(255))
    diagnosis_code = Column(String(20))
    vehicle_vin = Column(String(20))
    property_address = Column(Text)
    fraud_score = Column(Float, default=0.0)
    risk_level = Column(String(20), default='low')
    validation_errors = Column(JSON, default=[])
    enrichment_data = Column(JSON, default={})
    processing_metadata = Column(JSON, default={})
    trace_id = Column(String(64))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PipelineHealthSnapshot(Base):
    __tablename__ = 'pipeline_health_snapshots'

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    component_name = Column(String(100))
    status = Column(String(20))
    latency_ms = Column(Float)
    throughput_per_sec = Column(Float)
    error_rate = Column(Float)
    queue_depth = Column(Integer)
    meta_data = Column(JSON, default={})
    snapshot_at = Column(DateTime, default=datetime.utcnow)


def get_engine():
    return create_engine(config.postgres.connection_string, pool_pre_ping=True)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()