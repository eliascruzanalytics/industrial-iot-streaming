"""
Industrial IoT Streaming Data Pipeline - Consumers Module

Contains 5 specialized streaming consumers:
1. AlertConsumer: Real-time critical alerts & emergency notifications
2. MetricsConsumer: Real-time operational metrics & running window aggregations
3. ArchiveConsumer: Data Lake bronze/raw archival storage
4. DlqConsumer: Data Quality & Quarantine (DLQ) monitoring & error metrics
5. MaintenanceConsumer: Predictive maintenance & equipment lifecycle tracking
"""

from .alert_consumer import AlertConsumer
from .metrics_consumer import MetricsConsumer
from .archive_consumer import ArchiveConsumer
from .dlq_consumer import DlqConsumer
from .maintenance_consumer import MaintenanceConsumer

__all__ = [
    "AlertConsumer",
    "MetricsConsumer",
    "ArchiveConsumer",
    "DlqConsumer",
    "MaintenanceConsumer",
]
