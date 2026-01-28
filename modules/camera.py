# modules/camera.py
import cv2
import logging
import numpy as np
from config import CAMERA_CONFIG

log = logging.getLogger(__name__)

class CameraManager:
    """Manages the camera lifecycle (start, stop, frame reading)."""
    
    def __init__(self, config=None):
        # Use provided config or default to the module-level CAMERA_CONFIG
        self.config = config if config is not None else CAMERA_CONFIG
        self.cap = None
        self.is_running = False

    def start(self):
        """Initializes and opens the camera based on the current configuration."""
        if self.is_running and self.cap is not None:
            self.logger.warning("Camera is already running.")
            return True

        cam_type = self.config.get('type', 'v4l2').lower()

        try:
            # 1. V4L2 (USB Webcams)
            if cam_type == 'v4l2':
                device = self.config.get('device', 0)
                self.cap = cv2.VideoCapture(device, cv2.CAP_V4L2)
                self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.config.get('width', 1920))
                self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.config.get('height', 1080))
                self.cap.set(cv2.CAP_PROP_FPS, self.config.get('fps', 30))
                log.info(f"[CAMERA] Started V4L2: {device}")

            # 2. RTSP (IP Cameras)
            elif cam_type == 'rtsp':
                url = self.config.get('rtsp_url') 
                self.cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
                log.info(f"[CAMERA] Started RTSP: {url}")

            # 3. CSI (Jetson/Pi Camera Module)
            elif cam_type == 'csi':
                # Use default values if not explicitly set in config
                sensor_id = self.config.get('sensor_id', 0)
                width = self.config.get('width', 1920)
                height = self.config.get('height', 1080)
                fps = self.config.get('fps', 30)

                gst_pipeline = (
                    f"nvarguscamerasrc sensor-id={sensor_id} ! "
                    f"video/x-raw(memory:NVMM), width={width}, height={height}, "
                    f"format=NV12, framerate={fps}/1 ! "
                    f"nvvidconv ! video/x-raw, format=BGRx ! videoconvert ! video/x-raw, format=BGR ! appsink"
                )
                self.cap = cv2.VideoCapture(gst_pipeline, cv2.CAP_GSTREAMER)
                log.info(f"[CAMERA] Started CSI: Sensor {sensor_id}")

            # 4. File (Video testing)
            elif cam_type == 'file':
                file_path = self.config.get('file_path', 'sample.mp4')
                self.cap = cv2.VideoCapture(file_path)
                log.info(f"[CAMERA] Started FILE: {file_path}")

            # 5. Test (Dummy black frame)
            elif cam_type == 'test':
                self.cap = self._create_dummy_cap()
                log.info("[CAMERA] Started TEST mode")

            else:
                raise ValueError(f"Unknown Camera Type: {cam_type}")

            # Final Check
            if not self.cap.isOpened():
                raise RuntimeError("Camera failed to open (isOpened=False)")
            
            self.is_running = True
            return True

        except Exception as e:
            log.error(f"Critical Camera Error during start: {e}")
            self.cap = None
            self.is_running = False
            return False

    def get_frame(self):
        """Reads a single frame from the active camera."""
        if not self.is_running or self.cap is None:
            return False, None
        
        ret, frame = self.cap.read()
        
        # Auto-loop for video files
        if not ret and self.config.get('type') == 'file':
            self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            return self.cap.read()
            
        return ret, frame

    def stop(self):
        """Releases the camera resource."""
        if self.cap:
            self.cap.release()
        self.cap = None
        self.is_running = False
        log.info("[CAMERA] Stopped")

    def _create_dummy_cap(self):
        """Helper to create a fake camera object for testing."""
        class TestCap:
            def read(self): 
                return True, np.zeros((self.config.get('height', 1080), self.config.get('width', 1920), 3), np.uint8)
            def isOpened(self): return True
            def release(self): pass
            def set(self, prop, val): pass
            def get(self, prop): return 0
        return TestCap()