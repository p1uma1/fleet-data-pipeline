#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

EXPENSES="${1:-/opt/project/data/batch/expenses_20260927.csv.example}"
REPORT_DATE="${2:-2026-09-27}"

docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4 \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/project/streaming/jobs/batch_profitability.py \
  --expenses-path "$EXPENSES" \
  --report-date "$REPORT_DATE"
