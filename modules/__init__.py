# modules/__init__.py
from .camera import CameraManager
from .detector import KegDetector, QRDetector
from .database import DatabaseManager
from .api_sender import APISender
from .utils import setup_logging, manage_storage, save_default_keg_count
from .process_worker import submit_batch, shutdown, get_active_tasks
from .advanced import AdvancedQRDetector, run_advanced_detection
from .recovery import recover_system, check_database_integrity  # NEW
from .reports import ReportGenerator  # NEW

__all__ = [
    'CameraManager',
    'KegDetector', 
    'QRDetector',
    'DatabaseManager',
    'APISender',
    'AdvancedQRDetector',
    'setup_logging',
    'manage_storage',
    'save_default_keg_count',
    'submit_batch',
    'shutdown',
    'get_active_tasks',
    'run_advanced_detection',
    'recover_system',  # NEW
    'check_database_integrity',  # NEW
    'ReportGenerator'  # NEW
]