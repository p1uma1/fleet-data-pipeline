"""Spark streaming query listener for pipeline observability."""

from __future__ import annotations

from pyspark.sql.streaming import StreamingQueryListener

from src.logging_utils import get_logger, log_event

logger = get_logger(__name__, stage="observability")


class FleetQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):  # type: ignore[no-untyped-def]
        log_event(
            logger,
            "query_started",
            query_id=str(event.id),
            name=event.name,
        )

    def onQueryProgress(self, event):  # type: ignore[no-untyped-def]
        progress = event.progress
        duration = {}
        if hasattr(progress, "durationMs") and progress.durationMs:
            duration = dict(progress.durationMs)
        log_event(
            logger,
            "query_progress",
            query_name=progress.name,
            batch_id=progress.batchId,
            input_rows=progress.numInputRows,
            duration_ms=duration.get("triggerExecution"),
            sources=[
                getattr(s, "description", None)
                for s in (progress.sources or [])
            ],
        )

    def onQueryTerminated(self, event):  # type: ignore[no-untyped-def]
        log_event(
            logger,
            "query_terminated",
            query_id=str(event.id),
            exception=str(event.exception) if event.exception else None,
        )
