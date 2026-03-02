#!/bin/bash
#
# Backup script for the insurance claims database.
# Dumps PostgreSQL, compresses it, uploads to MinIO, and cleans up old backups.
#

set -e

# Configuration
POSTGRES_HOST=${POSTGRES_HOST:-postgres}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_DB=${POSTGRES_DB:-insurance_lineage}
POSTGRES_USER=${POSTGRES_USER:-pipeline_admin}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-securepass123}

MINIO_ENDPOINT=${MINIO_ENDPOINT:-"localhost:9000"}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin123}
MINIO_BUCKET=${MINIO_BUCKET:-insurance-claims-lake}

BACKUP_DIR="/tmp/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/postgres_${POSTGRES_DB}_${TIMESTAMP}.sql.gz"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting PostgreSQL backup...${NC}"
echo "Timestamp: $TIMESTAMP"
echo "Database: $POSTGRES_DB"
echo "Uploading to: s3://${MINIO_BUCKET}/backups/postgres/"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Dump the database
echo -e "\n${BLUE}[1/4] Dumping database...${NC}"
PGPASSWORD=$POSTGRES_PASSWORD pg_dump \
  -h "$POSTGRES_HOST" \
  -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --no-password \
  --verbose \
  --format=plain | gzip > "$BACKUP_FILE"

if [ $? -eq 0 ]; then
  BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
  echo -e "${GREEN}✓ Backup created: $BACKUP_SIZE${NC}"
else
  echo -e "${RED}✗ Backup failed${NC}"
  exit 1
fi

# Upload to MinIO
echo -e "\n${BLUE}[2/4] Uploading to MinIO...${NC}"
aws s3 cp "$BACKUP_FILE" \
  "s3://${MINIO_BUCKET}/backups/postgres/$(basename $BACKUP_FILE)" \
  --endpoint-url "http://${MINIO_ENDPOINT}" \
  --access-key "$MINIO_ACCESS_KEY" \
  --secret-key "$MINIO_SECRET_KEY" \
  --region us-east-1 \
  --no-progress

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Uploaded to S3${NC}"
else
  echo -e "${RED}✗ Upload failed${NC}"
  exit 1
fi

# Test the backup can be read
echo -e "\n${BLUE}[3/4] Checking backup...${NC}"
gunzip -t "$BACKUP_FILE" > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Backup looks good${NC}"
else
  echo -e "${RED}✗ Backup is corrupted${NC}"
  exit 1
fi

# Remove old local backups
echo -e "\n${BLUE}[4/4] Removing old backups...${NC}"
find "$BACKUP_DIR" -name "postgres_${POSTGRES_DB}_*.sql.gz" -mtime +30 -delete
LOCAL_BACKUPS=$(ls -1 "$BACKUP_DIR" | grep "postgres_${POSTGRES_DB}_" | wc -l)
echo -e "${GREEN}✓ Cleaned up old backups (kept ${LOCAL_BACKUPS})${NC}"

# Done
echo -e "\n${GREEN}=== Backup Complete ===${NC}"
echo "File: $(basename $BACKUP_FILE)"
echo "Size: $BACKUP_SIZE"
echo "Location: s3://${MINIO_BUCKET}/backups/postgres/"
echo "When: $TIMESTAMP"
