# config.py
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
SAVE_FOLDER = BASE_DIR / "keg_frames"
DB_PATH = BASE_DIR / "keg_detection.db"
MODELS_DIR = BASE_DIR / "models"
LOGS_DIR = BASE_DIR / "logs"
BATCH_MEMORY_FILE = BASE_DIR / "last_batch.txt"

# Create directories
SAVE_FOLDER.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Camera Configuration for Real-Time
CAMERA_CONFIG = {
    'type': 'v4l2',
    'device': 10,  # /dev/video10
    'width': 1920,
    'height': 1080,
    'fps': 30,
}

# Keg Types and Pallet Configuration
KEG_TYPES = {
    "30L": {
        "EUR Pallet": 6,
        "Industrial Pallet": 8
    },
    "20L Slim": {
        "EUR Pallet": 15,
        "Industrial Pallet": 20
    },
    "Other": {
        "EUR Pallet": 6,
        "Industrial Pallet": 8
    }
}

# Pallet types
PALLET_TYPES = ["EUR Pallet", "Industrial Pallet"]

# Default settings
DEFAULT_KEG_TYPE = "30L"
DEFAULT_PALLET_TYPE = "EUR Pallet"
DEFAULT_KEG_COUNT = 6  # Changed from expression to fixed value for utils.save_default_keg_count()
MAX_KEG_COUNT = 20
MIN_KEG_COUNT = 1
STABILITY_THRESHOLD = 5  # Increased for better stability

# FOV Validation
FOV_ENABLED = True
FOV_BOUNDARY_RATIO = 0.9
MIN_OCCLUSION_THRESHOLD = 0.7

# Camera identification for cloud
CAMERA_NAME = "ICAM-540"  
CAMERA_MAC_ID = "3C:6D:66:01:5A:F0"  
CAMERA_SERIAL = "icam-540"

# API Configuration
API_ENDPOINT = "http://143.110.186.93:5001/api/kegs/fillingareaupdatecamera"
BEER_TYPES_ENDPOINT = "http://143.110.186.93:5001/api/kegs/cam/beer-types"
API_TIMEOUT = 10
API_MAX_RETRIES = 3

# Add SSL bypass for development
SSL_VERIFY = False  # Set to True in production

# Enable payload hash for integrity
ENABLE_PAYLOAD_HASH = True

# Cloud sync settings
CLOUD_CONFIG_ENDPOINT = f"{API_ENDPOINT}/api/current-config"
CLOUD_SYNC_INTERVAL = 30

# Model Configuration
KEG_MODEL_PATH = MODELS_DIR / "best.pt"
QR_MODEL_PATH = MODELS_DIR / "model_qr" / "best.pt"
KEG_CONF_THRESHOLD = 0.1
QR_CONF_THRESHOLD = 0.4

# Advanced QR Detection
TILE_SIZE = (1280, 960)
OVERLAP_RATIO = 0.2
SCALE_FACTORS = [1.0, 1.2, 1.5]
MIN_CROP_SIZE = 50
MIN_UPSCALE_SIZE = 100

# Retry Configuration
RETRY_MAX_ATTEMPTS = 3
RETRY_BACKOFF_MINUTES = [1, 2, 4, 8, 16]
RETRY_CHECK_INTERVAL = 60
NETWORK_CHECK_INTERVAL = 30

# Alarm System Configuration
ALARM_BLINK_INTERVAL = 0.5
ENABLE_PHYSICAL_ALERTS = False

# Application Settings
MAX_FOLDER_SIZE_MB = 500

# Color Scheme for HMI
COLOR_SCHEME = {
    'bg_light': (1, 1, 1, 1),
    'panel_bg': (0.95, 0.95, 0.95, 1),
    'highlight': (0, 0.3, 0.6, 1),
    'text_dark': (0, 0, 0, 1),
    'alert_red': (0.8, 0.2, 0.2, 1),
    'status_green': (0.2, 0.7, 0.3, 1),
    'status_orange': (0.9, 0.6, 0.2, 1)
}