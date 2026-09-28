$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))

$expenses = if ($args[0]) { $args[0] } else { "/opt/project/data/batch/expenses_20260927.csv.example" }
$reportDate = if ($args[1]) { $args[1] } else { "2026-09-27" }

docker compose exec spark /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4 `
  --conf spark.sql.shuffle.partitions=4 `
  /opt/project/streaming/jobs/batch_profitability.py `
  --expenses-path $expenses `
  --report-date $reportDate
