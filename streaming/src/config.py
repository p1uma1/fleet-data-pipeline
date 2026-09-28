"""Runtime configuration for Spark jobs. Values come from the environment / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:9092")
    telemetry_topic: str = _env("TELEMETRY_TOPIC", "fleet.telemetry")
    alerts_topic: str = _env("ALERTS_TOPIC", "fleet.alerts")
    kafka_starting_offsets: str = _env("KAFKA_STARTING_OFFSETS", "latest")

    jdbc_url: str = _env("JDBC_URL", "jdbc:postgresql://localhost:5432/fleet")
    database_url: str = _env(
        "DATABASE_URL", "postgresql://fleet:fleet@localhost:5432/fleet"
    )
    postgres_user: str = _env("POSTGRES_USER", "fleet")
    postgres_password: str = _env("POSTGRES_PASSWORD", "fleet")

    checkpoint_dir: str = _env("CHECKPOINT_DIR", "checkpoints")
    trigger_interval: str = _env("TRIGGER_INTERVAL", "10 seconds")
    watermark: str = _env("WATERMARK", "2 minutes")
    metrics_window: str = _env("METRICS_WINDOW", "1 minute")
    idle_threshold_minutes: float = _env_float("IDLE_THRESHOLD_MINUTES", 5.0)
    no_data_threshold_batches: int = _env_int("NO_DATA_THRESHOLD_BATCHES", 12)

    expenses_path: str = _env(
        "EXPENSES_PATH", "data/batch/expenses_20260927.csv.example"
    )
    report_date: str = _env("REPORT_DATE", "2026-09-27")

    spark_app_name: str = _env("SPARK_APP_NAME", "fleet-stream-analytics")


settings = Settings()
