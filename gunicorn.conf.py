"""Gunicorn Configuration."""
import os

from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def when_ready(server):
    """Run metrics server on separate port."""
    GunicornPrometheusMetrics.start_http_server_when_ready(int(os.getenv("METRICS_PORT", "8765")))


def child_exit(server, worker):
    """Properly exit when metrics server stops."""
    GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)
