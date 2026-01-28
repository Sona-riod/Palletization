# modules/detector.py
import cv2
import numpy as np
import torch
from ultralytics import YOLO
from pyzbar.pyzbar import decode, ZBarSymbol
from qreader import QReader
from config import KEG_MODEL_PATH, QR_MODEL_PATH, KEG_CONF_THRESHOLD, QR_CONF_THRESHOLD

device = 0 if torch.cuda.is_available() else 'cpu'
use_half = torch.cuda.is_available()  # FP16 for speed on GPU

print(f"[DETECTOR] Loading models on: {device.upper()}")

try:
    keg_model = YOLO(str(KEG_MODEL_PATH))
    qr_model = YOLO(str(QR_MODEL_PATH))
    # Initialize QReader (Heavy model, load once)
    qreader = QReader(model_size='s', min_confidence=0.5) 
except Exception as e:
    print(f"[CRITICAL] Error loading models: {e}")
    raise e

def warmup_models():
    print("[DETECTOR] Warming up models...")
    dummy = np.zeros((640, 640, 3), dtype=np.uint8)
    try:
        # Warmup Keg
        keg_model.predict(dummy, conf=0.1, verbose=False, imgsz=640, half=use_half, device=device)
        # Warmup QR
        qr_model.predict(dummy, conf=0.1, verbose=False, imgsz=640, half=use_half, device=device)
        print("[DETECTOR] Warmup complete.")
    except Exception as e:
        print(f"[DETECTOR] Warmup non-critical error: {e}")

# Run warmup immediately on import
warmup_models()

class BaseDetector:
    def __init__(self):
        self.device = device
        self.use_half = use_half

    def preprocess(self, frame):
       
        return frame


class KegDetector(BaseDetector):
    def __init__(self):
        super().__init__()
        self.model = keg_model  

    def detect(self, frame):
        """
        Detects kegs and returns a dict with count and boxes for compatibility.
        """
        results = self.model.track(
            frame,
            conf=KEG_CONF_THRESHOLD,
            persist=True,  
            verbose=False,
            imgsz=640,
            half=self.use_half,
            device=self.device
        )
        
        ids = set()
        boxes = None
        if results and results[0].boxes and results[0].boxes.id is not None:
            # Extract IDs safely
            id_list = results[0].boxes.id.cpu().numpy().astype(int)
            ids.update(id_list)
            boxes = results[0].boxes  # Retain boxes for cropping in process_worker
        
        count = len(ids)
        return {'count': count, 'ids': ids, 'boxes': boxes}


class QRDetector(BaseDetector):
    def __init__(self):
        super().__init__()
        self.model = qr_model   
        self.reader = qreader   
    def _resize_crop(self, crop, max_size=300):
        """Resizes crop to speed up decoding if it's too huge."""
        h, w = crop.shape[:2]
        if max(h, w) <= max_size:
            return crop
        scale = max_size / max(h, w)
        return cv2.resize(crop, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

    def detect_and_decode(self, frame):
        """
        1. Detects QR bounding box using YOLO.
        2. Crops and tries QReader (AI).
        3. Fallback to Pyzbar (Standard).
        """
        # 1. Detect QR location
        results = self.model.predict(
            frame,
            conf=QR_CONF_THRESHOLD,
            verbose=False,
            imgsz=640,
            half=self.use_half,
            device=self.device
        )

        decoded_objects = []
        total_boxes = 0

        if results and results[0].boxes:
            boxes = results[0].boxes.xyxy.cpu().numpy() 
            total_boxes = len(boxes)

            for box in boxes:
                x1, y1, x2, y2 = map(int, box)
                
                # Safety check for frame boundaries
                h, w = frame.shape[:2]
                x1, y1 = max(0, x1), max(0, y1)
                x2, y2 = min(w, x2), min(h, y2)

                crop = frame[y1:y2, x1:x2]
                if crop.size == 0: continue

                # Optimization: Resize heavy crops
                crop_opt = self._resize_crop(crop)
                
                # Preprocess for barcode readers
                gray = cv2.cvtColor(crop_opt, cv2.COLOR_BGR2GRAY)

                current_text = None

                try:
                    qreader_texts = self.reader.detect_and_decode(image=gray)
                    for text in qreader_texts:
                        if text:
                            current_text = text
                            break
                except Exception:
                    pass

                if not current_text:
                    try:
                        pyzbar_res = decode(gray, symbols=[ZBarSymbol.QRCODE])
                        for obj in pyzbar_res:
                            text = obj.data.decode("utf-8")
                            if text:
                                current_text = text
                                break
                    except Exception:
                        pass
                
                if current_text:
                    decoded_objects.append({'data': current_text, 'bbox': (x1, y1, x2, y2)})

        return decoded_objects, total_boxes 

def detect_kegs(frame):
    """Top-level wrapper for keg detection."""
    return KegDetector().detect(frame)


def detect_qr_standard(frame):  
    """Detect QR codes in full frame using standard method."""
    qr_detector = QRDetector()
    decoded_data = []
    total_boxes = 0
    
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    try:
        qreader_texts = qr_detector.reader.detect_and_decode(image=gray)
        decoded_data.extend([text for text in qreader_texts if text])
    except:
        pass
    try:
        pyzbar_res = decode(gray, symbols=[ZBarSymbol.QRCODE])
        decoded_data.extend([obj.data.decode("utf-8") for obj in pyzbar_res if obj.data])
    except:
        pass
    total_boxes = 1  # Full frame
    return list(set(decoded_data)), total_boxes


def detect_qr_advanced(image_path):
    """Wrapper for advanced detection - Uses standalone AdvancedQRDetector."""
    from .advanced import AdvancedQRDetector  
    detector = AdvancedQRDetector()  
    return detector.detect_advanced(image_path)  


def detect_composition(frame):
    """Placeholder: Detect beer composition (e.g., via another YOLO or clustering)."""
    # TODO: Implement real composition detection 
    keg_count = detect_kegs(frame)['count']
    return {'Unknown': keg_count}  