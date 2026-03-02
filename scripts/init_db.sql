-- Insurance Pipeline Observability Database Schema

-- Create the lineage database (if running as root, else Airflow DB is created via POSTGRES_DB env var)
SELECT 'CREATE DATABASE insurance_lineage' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'insurance_lineage')\gexec

-- Connect to insurance_lineage
\c insurance_lineage;

-- Store data pipeline nodes (sources, transforms, sinks)
CREATE TABLE IF NOT EXISTS lineage_nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    node_type VARCHAR(50) NOT NULL,  -- source, transform, sink
    node_name VARCHAR(255) NOT NULL,
    description TEXT,
    component VARCHAR(100),  -- kafka, processor, s3, postgres
    kafka_topic VARCHAR(255),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store transformations between nodes
CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id SERIAL PRIMARY KEY,
    source_node_id VARCHAR(255) REFERENCES lineage_nodes(node_id),
    target_node_id VARCHAR(255) REFERENCES lineage_nodes(node_id),
    transform_type VARCHAR(100),  -- ingest, validate, score, enrich, store
    description TEXT,
    record_count BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    avg_latency_ms FLOAT DEFAULT 0,
    error_count BIGINT DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Record individual claim transformations through the pipeline
CREATE TABLE IF NOT EXISTS lineage_events (
    event_id SERIAL PRIMARY KEY,
    edge_id INTEGER REFERENCES lineage_edges(edge_id),
    trace_id VARCHAR(64),
    span_id VARCHAR(32),
    claim_id VARCHAR(255),
    event_type VARCHAR(50),  -- record_processed, batch_completed, error
    record_count INTEGER DEFAULT 1,
    latency_ms FLOAT,
    status VARCHAR(20) DEFAULT 'success',  -- success, failure, partial
    error_message TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================== CLAIMS TABLES ====================

CREATE TABLE IF NOT EXISTS processed_claims (
    claim_id VARCHAR(255) PRIMARY KEY,
    policy_number VARCHAR(100) NOT NULL,
    claimant_name VARCHAR(255),
    claim_type VARCHAR(50),
    claim_amount DECIMAL(15, 2),
    date_of_loss DATE,
    date_filed DATE,
    description TEXT,
    status VARCHAR(50) DEFAULT 'submitted',
    provider_name VARCHAR(255),
    diagnosis_code VARCHAR(20),
    vehicle_vin VARCHAR(20),
    property_address TEXT,
    fraud_score FLOAT DEFAULT 0.0,
    risk_level VARCHAR(20) DEFAULT 'low',
    validation_errors JSONB DEFAULT '[]',
    enrichment_data JSONB DEFAULT '{}',
    processing_metadata JSONB DEFAULT '{}',
    trace_id VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================== PIPELINE HEALTH ====================

CREATE TABLE IF NOT EXISTS pipeline_health_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    component_name VARCHAR(100),
    status VARCHAR(20),                        -- healthy, degraded, down
    latency_ms FLOAT,
    throughput_per_sec FLOAT,
    error_rate FLOAT,
    queue_depth INTEGER,
    metadata JSONB DEFAULT '{}',
    snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================== INDEXES ====================

CREATE INDEX idx_lineage_events_trace ON lineage_events(trace_id);
CREATE INDEX idx_lineage_events_claim ON lineage_events(claim_id);
CREATE INDEX idx_lineage_events_created ON lineage_events(created_at);
CREATE INDEX idx_processed_claims_status ON processed_claims(status);
CREATE INDEX idx_processed_claims_type ON processed_claims(claim_type);
CREATE INDEX idx_processed_claims_fraud ON processed_claims(fraud_score);
CREATE INDEX idx_pipeline_health_component ON pipeline_health_snapshots(component_name, snapshot_at);

-- ==================== SEED LINEAGE NODES ====================

INSERT INTO lineage_nodes (node_id, node_type, node_name, description, component, kafka_topic) VALUES
('src_claims_ingestion', 'source', 'Claims Ingestion', 'Raw insurance claims from agents, portals, and call centers', 'kafka', 'insurance.claims.raw'),
('tx_validation', 'transform', 'Claims Validator', 'Validates claim data integrity, required fields, and business rules', 'processor', 'insurance.claims.validated'),
('tx_fraud_detection', 'transform', 'Fraud Detector', 'Scores claims for potential fraud using rule-based engine', 'processor', 'insurance.claims.scored'),
('tx_enrichment', 'transform', 'Claims Enricher', 'Enriches claims with policy details, risk ratings, and geo data', 'processor', 'insurance.claims.enriched'),
('sink_s3_datalake', 'sink', 'S3 Data Lake', 'Processed claims stored in MinIO/S3 data lake', 's3', NULL),
('sink_postgres', 'sink', 'Claims Database', 'Final processed claims in PostgreSQL', 'postgres', NULL),
('sink_dlq', 'sink', 'Dead Letter Queue', 'Failed claims for manual review', 'kafka', 'insurance.claims.dlq')
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO lineage_edges (source_node_id, target_node_id, transform_type, description) VALUES
('src_claims_ingestion', 'tx_validation', 'ingest', 'Raw claims sent for validation'),
('tx_validation', 'tx_fraud_detection', 'validate', 'Validated claims sent for fraud scoring'),
('tx_validation', 'sink_dlq', 'reject', 'Invalid claims sent to dead letter queue'),
('tx_fraud_detection', 'tx_enrichment', 'score', 'Scored claims sent for enrichment'),
('tx_enrichment', 'sink_s3_datalake', 'store', 'Enriched claims stored in S3'),
('tx_enrichment', 'sink_postgres', 'store', 'Enriched claims stored in PostgreSQL');