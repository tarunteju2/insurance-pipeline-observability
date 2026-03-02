# Backup & Disaster Recovery

Here's how we're protecting the data and making sure we can recover if anything goes wrong.

## The Goal

We want to keep data safe and be able to get things running again quickly if there's a disaster. Here's what we're aiming for:
- **RPO (Recovery Point Objective):** 15 minutes - max data we'll lose
- **RTO (Recovery Time Objective):** 1 hour - max time to get back online
- **Data Retention:** 30 days of backups
- **Backups:** Daily, automated
- **Testing:** Weekly



## PostgreSQL Database Backups

We dump the entire database every day at 2 AM and compress it with gzip. Takes about 50 MB per backup, we keep 30 days worth (so around 1.5 GB total). It's just plain SQL so it's human-readable and works anywhere.

The database gets dumped from Postgres, compressed, and sent to MinIO:
```
PostgreSQL → pg_dump → gzip → MinIO
            (insurance-claims-lake/backups/postgres/)
```

The Airflow DAG handles this automatically, but if you need to do it manually:
```bash
#!/bin/bash
PGPASSWORD=securepass123 pg_dump -h postgres -U pipeline_admin \
  -d insurance_lineage | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz

# Upload to S3
aws s3 cp backup_*.sql.gz s3://insurance-claims-lake/backups/postgres/ \
  --endpoint-url http://localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin123
```

Restores take about 5-10 minutes depending on size.

## MinIO/S3 Data Lake Backups

MinIO stores all the claims files (raw, validated, scored, enriched). We set up lifecycle policies so old files get archived automatically - current stuff stays around indefinitely, anything older than 90 days gets moved to cheaper storage, and really old stuff (>1 year) gets deleted.

We back up these buckets:
- `insurance-claims-lake/raw/` - Raw ingestion
- `insurance-claims-lake/validated/` - Validated claims
- `insurance-claims-lake/scored/` - Fraud-scored claims
- `insurance-claims-lake/enriched/` - Final processed claims
- `insurance-claims-lake/backups/` - The PostgreSQL backups

## Prometheus & Grafana

Prometheus keeps 30 days of metrics locally. We also take weekly snapshots and throw them in MinIO for longer-term storage.

Grafana dashboards are all version-controlled in git. The dashboard JSON and provisioning configs are in `grafana/provisioning/`. If we need to rebuild, we just spin up the Docker container and it'll load everything automatically.

---

## If Something Goes Wrong

### PostgreSQL Recovery

If the database gets corrupted or we lose data:

1. Find the backup you want:
   ```bash
   aws s3 ls s3://insurance-claims-lake/backups/postgres/ \
     --endpoint-url http://localhost:9000
   ```

2. Download it:
   ```bash
   aws s3 cp s3://insurance-claims-lake/backups/postgres/postgres_XXXXXX.sql.gz . \
     --endpoint-url http://localhost:9000
   ```

3. Drop the old database and create a fresh one:
   ```bash
   psql -h postgres -U postgres -c "DROP DATABASE insurance_lineage;"
   psql -h postgres -U postgres -c "CREATE DATABASE insurance_lineage OWNER pipeline_admin;"
   ```

4. Restore from the backup:
   ```bash
   gunzip -c postgres_XXXXXX.sql.gz | psql -h postgres -U pipeline_admin -d insurance_lineage
   ```

5. Verify it worked:
   ```bash
   psql -h postgres -U pipeline_admin -d insurance_lineage \
     -c "SELECT COUNT(*) FROM processed_claims;"
   ```

Should take 5-10 minutes depending on size.

### MinIO Data Recovery

Oops, deleted something? Here's what to do:

1. Check if versioning is enabled:
   ```bash
   aws s3api list-object-versions \
     --bucket insurance-claims-lake \
     --prefix enriched/ \
     --endpoint-url http://localhost:9000
   ```

2. Restore a specific file:
   ```bash
   aws s3api copy-object \
     --bucket insurance-claims-lake \
     --copy-source insurance-claims-lake/enriched/file.json?versionId=XXXXX \
     --key enriched/file.json \
     --endpoint-url http://localhost:9000
   ```

3. Or grab everything from a backup:
   - Get the backup snapshot from the `backups/` directory
   - Copy all objects back

Takes less than 5 minutes usually.

### Full Meltdown Recovery

Everything's down? This is what we do:

1. Tear down and rebuild:
   ```bash
   docker compose down -v
   docker compose up -d
   ```

2. Restore the database (follow PostgreSQL recovery above)

3. Restore MinIO data if needed (follow that procedure above)

4. Check that the observability API is healthy:
   ```bash
   curl http://localhost:8082/health
   ```

5. Restart the pipeline:
   ```bash
   python scripts/run_pipeline.py
   ```

Total time: about 30-60 minutes.

---

## Testing Our Backups

We test weekly to make sure backups actually work. Every Monday at 3 AM:
1. Restore the latest backup to a separate test database
2. Run some data validation checks to make sure everything's there
3. Verify referential integrity (foreign keys, all that good stuff)
4. Clean up the test database

Here's a quick test query to check if a backup is good:
```sql
SELECT 
  COUNT(*) as total_claims,
  (SELECT COUNT(DISTINCT claim_id) FROM processed_claims) as unique_claims,
  MAX(created_at) as latest_claim,
  MIN(created_at) as oldest_claim
FROM processed_claims;
```

---

## When Disasters Happen

### Who Does What

- **On-Call Engineer:** Spots the problem, starts the response
- **Platform Lead:** Calls the shots, coordinates everything
- **Database Admin:** Actually runs the restore commands
- **Incident Commander:** Keeps track of progress, tells people what's happening

### Timeline

| Time | What Happens | Who |
|------|--------------|-----|
| T+0 | Problem detected | DevOps |
| T+5m | "OK, this is real" | Platform Lead |
| T+10m | Start recovery | DB Admin |
| T+30m | Database back up | DB Admin |
| T+45m | Pipeline running again | Platform Team |
| T+60m | Everything's healthy | DevOps |

### Talking About It

When something breaks:
1. Message in #incidents on Slack
2. Email the on-call team
3. Update the status page

During recovery, send updates every 15 min on Slack. When it's fixed, we do a post-incident review and write up what happened within 48 hours.

---

## Security & Compliance

### Data Protection

- **GDPR:** If someone asks to delete their data, we respect that in our backups too
- **HIPAA:** If there's any health info, backups get encrypted
- **SOC 2:** We log all backup operations for audits

### Protecting Backups

**Encryption:**
- Backups encrypted at rest on MinIO
- TLS/HTTPS for moving data around
- Secrets stored securely

**Access:**
- Only certain people can touch backups
- We log every access
- MFA required if you need to restore something in an emergency

---

## What We've Got, What's Next

### Done
- ✅ PostgreSQL backups every day to MinIO
- ✅ Airflow DAG running backups automatically
- ✅ Old backups cleaned up (30-day retention)
- ✅ Integrity checks in place
- ✅ Manual recovery procedures documented
- ✅ MinIO lifecycle policies configured

### Still To Do
- ⏳ Automated weekly restore testing
- ⏳ Encryption on backups at rest
- ⏳ Multi-region backup (for the future)
- ⏳ Slack/PagerDuty notifications when backups finish
- ⏳ Quarterly disaster recovery drills (automated)
- ⏳ Dashboard to watch RTO/RPO metrics

---

## Monitoring & Alerts

We track backup health with these Prometheus metrics:
- `backup_success_total` - How many backups worked
- `backup_duration_seconds` - How long the backup took
- `backup_size_bytes` - Size of the backup
- `backup_last_run_seconds` - How long since last backup

Alert if:
- ⚠️ No backup in 26 hours (warning)
- 🔴 No backup in 48 hours (critical - page someone)
- ⚠️ Backup took longer than 30 minutes
- ⚠️ Backup bigger than 500 MB

Slack alerts go to #backup-alerts, with a mention to the on-call engineer if it's critical.

---

## Cost

Storage costs are negligible right now. We're using about 1.5 GB per month of backup data, which costs maybe $0.04 at normal S3 rates. MinIO is local though, so it's actually free.

To keep costs down:
- Archive backups older than 90 days
- Remove duplicate objects
- We already compress with gzip (gets us about 70% reduction)

---

## More Info

- PostgreSQL docs: https://www.postgresql.org/docs/current/backup.html
- MinIO backup guide: https://min.io/docs/minio/linux/operations/backup-restore.html
- The Airflow DAG: `dags/backup_disaster_recovery.py`
- The backup script: `scripts/backup_postgres.sh`

---

## Technical Details

When we dump PostgreSQL, we use these flags:
- `--format=plain` - Standard SQL format (works everywhere)
- `--verbose` - Gives us detailed output if something goes wrong
- `--no-password` - Uses the PGPASSWORD environment variable
- Then compress with gzip (drops the size by about 70%)

We keep all backups for 30 days, then automatically delete older ones. You can manually archive anything you want to keep longer for compliance reasons.
