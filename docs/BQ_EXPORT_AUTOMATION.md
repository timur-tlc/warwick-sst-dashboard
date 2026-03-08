# BigQuery Export Automation — Implementation Report

**Date:** 2026-03-08
**Author:** Claude (with Timur)

## Overview

Implemented automated weekly Athena → BigQuery export for the Warwick SST dashboard. Previously, all BigQuery exports were manual (`python export_to_bigquery.py`), which meant the Looker Studio report would go stale until someone remembered to re-run the script. The report was 6 days behind when we started.

## Architecture

```
EventBridge Schedule (Sun 21:00 UTC = Mon 08:00 AEST)
  → Step Functions state machine
    ├── Lambda("sessions")  ─┐
    ├── Lambda("events")    ─┤  parallel
    └── Lambda("items")     ─┘
    → SNS email notification (success or failure)
```

- **One Lambda function** (`warwick-weave-sst-bq-export`) invoked 3 times with different input
- **Container image** deployed to ECR (pandas + pyarrow + google-cloud-bigquery exceed Lambda layer size limits)
- **GCP credentials** stored in AWS Secrets Manager (`warwick/gcp-service-account`)
- **All infrastructure managed by OpenTofu** in `bq-export.tf` — synced with remote S3 state

## Components Created

| Component | Name | Details |
|-----------|------|---------|
| ECR Repository | `warwick-weave-sst-bq-export` | Container image, keeps last 3 images |
| Lambda Function | `warwick-weave-sst-bq-export` | 3GB memory, 3GB ephemeral `/tmp`, 15min timeout |
| Step Functions | `warwick-weave-sst-bq-export` | Parallel execution + SNS notification |
| EventBridge Schedule | `warwick-weave-sst-bq-export-weekly` | `cron(0 21 ? * SUN *)` (Mon 08:00 AEST) |
| SNS Topic | `warwick-weave-sst-bq-export-notifications` | Email to timur@thelightscollective.agency |
| IAM Roles | 3 roles | Lambda, Step Functions, EventBridge Scheduler |
| Secrets Manager | `warwick/gcp-service-account` | GCP service account JSON key |
| GCP Service Account | `warwickdatatransfer@...` | Reused existing SA with BigQuery admin |

## Challenges Encountered

### 1. CloudFormation vs OpenTofu Conflict (Averted)

Initially started building a CloudFormation template for deployment. The user caught this before any deployment — the Warwick AWS account (`025066271340`) is fully managed by OpenTofu with remote state in S3 + DynamoDB locks. Deploying via CloudFormation would have created resources outside OpenTofu's knowledge, causing state drift and potential naming conflicts on the next `tofu apply`.

**Resolution:** Scrapped the CloudFormation template and added all resources to the existing OpenTofu config in `bq-export.tf`.

### 2. Docker Image Manifest Format

First Docker build used `docker build --platform linux/amd64`, which produced an OCI image index (manifest list) with attestation manifests. Lambda rejected this with "image manifest, config or layer media type not supported".

**Resolution:** Rebuilt with `docker buildx build --platform linux/amd64 --provenance=false --output type=docker` to produce a single-platform Docker manifest without attestations.

### 3. Step Functions `States.Format` with `jsonencode`

The Step Functions state machine definition used `jsonencode()` in HCL with `States.Format` intrinsic functions containing `\\n` newline escapes. `jsonencode` double-escaped these to `\\\\n`, which Step Functions rejected as invalid JSONPath.

**Resolution:** Simplified the notification messages to plain strings without newlines. The success message is a static string; the failure message uses `$.Cause` directly as the message.

### 4. Events Table — Disk Space Exhaustion (Run 1)

First test run: the events Lambda downloaded the Athena CSV (~1.5GB) to `/tmp` but only had 1024MB ephemeral storage. Failed with `OSError: [Errno 28] No space left on device`.

**Resolution:** Increased ephemeral storage from 1024MB to 3072MB in the Lambda configuration.

### 5. Events Table — Out of Memory (Run 2)

Second test run: the CSV downloaded successfully to `/tmp` (3GB storage was enough), but loading the entire 3.7M-row CSV into a pandas DataFrame exhausted the 3GB Lambda memory. Failed with `Runtime.OutOfMemory`.

The pipeline was: download 1.5GB CSV → read into pandas (~3GB in memory) → upload to BigQuery. Total memory needed: ~4.5GB, but Lambda only had 3GB.

**Resolution:** Rewrote the events export to use **chunked processing**:
1. Download CSV to `/tmp` (disk, not memory)
2. Read CSV in chunks of 500,000 rows using `pd.read_csv(chunksize=500_000)`
3. First chunk uses `WRITE_TRUNCATE`, subsequent chunks use `WRITE_APPEND`
4. Each chunk is freed from memory after upload (`del chunk`)

This keeps peak memory usage to ~1 chunk (~500MB) instead of the entire dataset.

### 6. Step Functions Reporting Success on Lambda Failure

The Parallel state's `Catch` block caught the Lambda OOM error and routed to `NotifyFailure` → SNS, but the overall execution status showed `SUCCEEDED` (because the Catch handler completed successfully). This meant sessions and items were refreshed but events silently wasn't.

This is a known Step Functions behavior — a caught error is a handled error, not a failure. In the final working version, all three branches succeed, so this is no longer an issue. For future robustness, the notification could include row counts to verify completeness.

### 7. EventBridge Scheduler `tags` Not Supported

The `aws_scheduler_schedule` resource in the AWS provider version used by the project doesn't support the `tags` argument. OpenTofu plan failed with "An argument named tags is not expected here".

**Resolution:** Removed the `tags` block from the schedule resource.

## Test Results

Three test executions:

| Run | Sessions | Events | Items | Duration | Notes |
|-----|----------|--------|-------|----------|-------|
| 1 | 234,874 | FAILED | 14,670 | 1m 47s | Events: disk space (1GB `/tmp`) |
| 2 | 234,874 | FAILED | 14,670 | ~2m | Events: OOM (3GB memory, full load) |
| 3 | 234,874 | 3,695,785 | 14,670 | ~5m | All succeeded (chunked events) |

Final BigQuery state after successful run:

| Table | Rows | Date Range |
|-------|------|-----------|
| sessions | 234,874 | 2025-12-12 to 2026-03-07 |
| events | 3,695,785 | 2025-12-12 to 2026-03-07 |
| items | 14,670 | 2025-12-20 to 2026-03-07 |

## Cost Estimate

| Component | Monthly Cost |
|-----------|-------------|
| Lambda (3x invocations/week, ~5min total at 3GB) | ~$0.03 |
| Step Functions (state transitions) | ~$0.01 |
| Athena (3 queries/week, ~6GB scanned each) | ~$0.60 |
| Secrets Manager (1 secret) | $0.40 |
| ECR (image storage ~300MB) | ~$0.03 |
| SNS (4 emails/month) | Free tier |
| **Total** | **~$1.07/month** |

## Files

| File | Location | Purpose |
|------|----------|---------|
| `lambda/handler.py` | `warwick-sst-dashboard` | Lambda function code |
| `lambda/Dockerfile` | `warwick-sst-dashboard` | Container image definition |
| `lambda/requirements.txt` | `warwick-sst-dashboard` | Python dependencies |
| `bq-export.tf` | `warwick-sst-infrastructure` | OpenTofu resource definitions |

## Manual Operations

- **Trigger ad-hoc refresh:** `aws stepfunctions start-execution --profile warwick --region ap-southeast-2 --state-machine-arn arn:aws:states:ap-southeast-2:025066271340:stateMachine:warwick-weave-sst-bq-export --input '{}'`
- **Update Lambda code:** Rebuild image, push to ECR, then `aws lambda update-function-code --function-name warwick-weave-sst-bq-export --image-uri <ecr-uri>:latest`
- **SNS confirmation:** Must click confirmation link in email for notifications to work

## Future Considerations

- **Data growth:** Events table grows ~500K rows/month. At 3GB memory with 500K-chunk processing, the Lambda should handle up to ~20M rows. Beyond that, consider BigQuery Transfer Service (S3 → BQ directly).
- **Daylight saving:** Schedule is fixed UTC. Monday 08:00 AEST shifts to 07:00 AEDT. Acceptable for a weekly sync.
- **Idempotency:** `WRITE_TRUNCATE` (first chunk) makes each run idempotent — safe to re-run.
