"""sologsb scheduler service package."""
from .common import MonitorError, MonitorError as ManagerApiError  # noqa: F401  (re-export)
from .version import APP_VERSION as __version__

__all__ = ["MonitorError", "ManagerApiError", "__version__"]
