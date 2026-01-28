#!/usr/bin/env python3
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import cv2
import numpy as np
import json
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime
import requests
import uuid
import sqlite3

# Kivy Imports
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.togglebutton import ToggleButton
from kivy.uix.progressbar import ProgressBar
from kivy.uix.image import Image
from kivy.uix.spinner import Spinner
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.uix.modalview import ModalView
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.graphics import Color, Rectangle, Line
from kivy.graphics.texture import Texture
import logging

# Custom Modules
from modules.camera import CameraManager
from modules.detector import KegDetector, QRDetector
from modules.database import DatabaseManager
from modules.api_sender import APISender
from modules.process_worker import submit_batch
from modules.utils import setup_logging, create_timestamp, save_last_batch, load_last_batch
from config import (
    CAMERA_CONFIG, DEFAULT_KEG_COUNT, MAX_KEG_COUNT, KEG_TYPES, SAVE_FOLDER,
    MIN_KEG_COUNT, STABILITY_THRESHOLD, COLOR_SCHEME,
    FOV_ENABLED, FOV_BOUNDARY_RATIO,
    CLOUD_CONFIG_ENDPOINT, CLOUD_SYNC_INTERVAL, CAMERA_MAC_ID
)

# Setup logging
logger = setup_logging()

# Color Constants
COLOR_BG_DARK = (0.1, 0.1, 0.1, 1)
COLOR_PANEL_BG = (0.15, 0.15, 0.15, 1)
COLOR_HIGHLIGHT = (0, 0.4, 0.8, 1)
COLOR_TEXT_LIGHT = (0.9, 0.9, 0.9, 1)
COLOR_ALERT_RED = (0.8, 0.2, 0.2, 1)
COLOR_STATUS_GREEN = (0.2, 0.8, 0.2, 1)
COLOR_STATUS_ORANGE = (1, 0.6, 0, 1)
COLOR_STATUS_BLUE = (0.2, 0.5, 0.8, 1)
COLOR_BUTTON_NORMAL = (0.3, 0.3, 0.3, 1)

def hex_color(rgb_tuple):
    """Convert RGB tuple to hex color string"""
    return f'#{int(rgb_tuple[0]*255):02x}{int(rgb_tuple[1]*255):02x}{int(rgb_tuple[2]*255):02x}{int(rgb_tuple[3]*255):02x}'

class ToastMessage(ModalView):
    """Toast-like popup for brief user messages"""
    def __init__(self, message, msg_type="info", duration=3, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (0.6, None)
        self.height = 60
        self.pos_hint = {'center_x': 0.5, 'top': 0.95}
        self.background_color = [0, 0, 0, 0]
        self.auto_dismiss = False
        
        # Choose color based on type
        if msg_type == "success":
            bg_color = COLOR_STATUS_GREEN
        elif msg_type == "error":
            bg_color = COLOR_ALERT_RED
        elif msg_type == "warning":
            bg_color = COLOR_STATUS_ORANGE
        else:
            bg_color = COLOR_STATUS_BLUE
        
        container = BoxLayout(padding=10)
        with container.canvas.before:
            Color(*bg_color)
            self.rect = Rectangle(pos=container.pos, size=container.size)
        container.bind(pos=lambda *x: setattr(self.rect, 'pos', container.pos))
        container.bind(size=lambda *x: setattr(self.rect, 'size', container.size))
        
        label = Label(
            text=message,
            font_size='14sp',
            bold=True,
            color=(1, 1, 1, 1)
        )
        container.add_widget(label)
        self.add_widget(container)
        
        # Auto-dismiss after duration
        Clock.schedule_once(lambda dt: self.dismiss(), duration)


class ConfirmationModal(ModalView):
    """Modal for user confirmations"""
    def __init__(self, title, message, on_confirm, on_cancel=None, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (0.6, 0.35)
        self.background_color = hex_color((0, 0, 0, 0.9))
        self.on_confirm_callback = on_confirm
        self.on_cancel_callback = on_cancel
        self.auto_dismiss = False
        
        layout = BoxLayout(orientation='vertical', padding=20, spacing=15)
        
        # Title
        title_label = Label(
            text=title,
            size_hint_y=None,
            height=30,
            font_size='18sp',
            bold=True,
            color=hex_color(COLOR_HIGHLIGHT)
        )
        
        # Message
        message_label = Label(
            text=message,
            size_hint_y=None,
            height=60,
            font_size='14sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='center',
            text_size=(380, None)
        )
        
        # Buttons
        btn_layout = BoxLayout(size_hint_y=None, height=45, spacing=15)
        
        cancel_btn = Button(
            text='CANCEL',
            font_size='14sp',
            background_color=hex_color((0.4, 0.4, 0.4, 1)),
            background_normal='',
            on_press=self.cancel
        )
        
        confirm_btn = Button(
            text='CONFIRM',
            font_size='14sp',
            bold=True,
            background_color=hex_color(COLOR_STATUS_GREEN),
            background_normal='',
            on_press=self.confirm
        )
        
        btn_layout.add_widget(cancel_btn)
        btn_layout.add_widget(confirm_btn)
        
        layout.add_widget(title_label)
        layout.add_widget(message_label)
        layout.add_widget(btn_layout)
        
        self.add_widget(layout)
    
    def confirm(self, instance):
        self.dismiss()
        if self.on_confirm_callback:
            self.on_confirm_callback()
    
    def cancel(self, instance):
        self.dismiss()
        if self.on_cancel_callback:
            self.on_cancel_callback()


class CountModal(ModalView):
    def __init__(self, on_confirm, current_count, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (0.5, 0.45)
        self.background_color = hex_color((0, 0, 0, 0.85))
        self.on_confirm = on_confirm
        
        layout = BoxLayout(orientation='vertical', padding=20, spacing=10)
        
        # Title
        title = Label(
            text='Set Target Keg Count',
            size_hint_y=None,
            height=40,
            font_size='18sp',
            bold=True,
            color=hex_color(COLOR_HIGHLIGHT)
        )
        
        # Input field
        input_box = BoxLayout(orientation='horizontal', size_hint_y=None, height=50, spacing=10)
        self.input_field = TextInput(
            text=str(current_count),
            multiline=False,
            font_size='20sp',
            halign='center',
            background_color=hex_color((0.2, 0.2, 0.2, 1)),
            foreground_color=hex_color(COLOR_TEXT_LIGHT),
            cursor_color=hex_color(COLOR_HIGHLIGHT),
            size_hint_x=0.7
        )
        
        # Quick +/- buttons
        controls = GridLayout(cols=1, rows=2, spacing=5, size_hint_x=0.3)
        
        def increment(dt):
            self.clear_error()
            try:
                val = int(self.input_field.text) + 1
                if val <= MAX_KEG_COUNT:
                    self.input_field.text = str(val)
                else:
                    self.show_error(f"Maximum is {MAX_KEG_COUNT}")
            except:
                self.show_error("Enter a valid number")
                
        def decrement(dt):
            self.clear_error()
            try:
                val = int(self.input_field.text) - 1
                if val >= MIN_KEG_COUNT:
                    self.input_field.text = str(val)
                else:
                    self.show_error(f"Minimum is {MIN_KEG_COUNT}")
            except:
                self.show_error("Enter a valid number")
        
        plus_btn = Button(
            text='+',
            font_size='20sp',
            background_color=hex_color(COLOR_STATUS_GREEN),
            on_press=increment
        )
        
        minus_btn = Button(
            text='−',
            font_size='20sp',
            background_color=hex_color(COLOR_ALERT_RED),
            on_press=decrement
        )
        
        controls.add_widget(plus_btn)
        controls.add_widget(minus_btn)
        
        input_box.add_widget(self.input_field)
        input_box.add_widget(controls)
        
        # Error message label
        self.error_label = Label(
            text='',
            size_hint_y=None,
            height=25,
            font_size='12sp',
            color=hex_color(COLOR_ALERT_RED)
        )
        
        # Action buttons
        btn_layout = BoxLayout(size_hint_y=None, height=40, spacing=10)
        cancel_btn = Button(
            text='Cancel',
            background_color=hex_color((0.4, 0.4, 0.4, 1)),
            on_press=lambda x: self.dismiss()
        )
        ok_btn = Button(
            text='Set Count',
            background_color=hex_color(COLOR_HIGHLIGHT),
            on_press=self.confirm
        )
        
        btn_layout.add_widget(cancel_btn)
        btn_layout.add_widget(ok_btn)
        
        layout.add_widget(title)
        layout.add_widget(input_box)
        layout.add_widget(self.error_label)
        layout.add_widget(btn_layout)
        
        self.add_widget(layout)
    
    def show_error(self, message):
        self.error_label.text = f" {message}"
    
    def clear_error(self):
        self.error_label.text = ''
    
    def confirm(self, instance):
        self.clear_error()
        try:
            count = int(self.input_field.text)
            if count < MIN_KEG_COUNT:
                self.show_error(f"Count must be at least {MIN_KEG_COUNT}")
                return
            if count > MAX_KEG_COUNT:
                self.show_error(f"Count cannot exceed {MAX_KEG_COUNT}")
                return
            self.on_confirm(count)
            self.dismiss()
        except ValueError:
            self.show_error("Please enter a valid number")

class BatchModal(ModalView):
    def __init__(self, on_confirm, current_batch, **kwargs):
        super().__init__(**kwargs)
        self.size_hint = (0.6, 0.55)
        self.background_color = hex_color((0, 0, 0, 0.85))
        self.on_confirm = on_confirm
        
        layout = BoxLayout(orientation='vertical', padding=20, spacing=12)
        
        # Title
        title = Label(
            text='Batch Details',
            size_hint_y=None,
            height=30,
            font_size='18sp',
            bold=True,
            color=hex_color(COLOR_HIGHLIGHT)
        )
        
        # Batch Input
        batch_label = Label(text='Batch Number:', size_hint_y=None, height=20, halign='left', text_size=(400, None))
        self.input_field = TextInput(
            text=current_batch if current_batch else 'BATCH-',
            multiline=False,
            font_size='18sp',
            halign='center',
            write_tab=False,
            background_color=hex_color((0.2, 0.2, 0.2, 1)),
            foreground_color=hex_color(COLOR_TEXT_LIGHT),
            cursor_color=hex_color(COLOR_HIGHLIGHT),
            size_hint_y=None,
            height=40
        )
        
        # Format hint label
        format_hint = Label(
            text='Format: BATCH-XXX',
            size_hint_y=None,
            height=20,
            font_size='11sp',
            color=hex_color(COLOR_STATUS_BLUE),
            halign='center'
        )
        
        # Error message label
        self.error_label = Label(
            text='',
            size_hint_y=None,
            height=25,
            font_size='12sp',
            color=hex_color(COLOR_ALERT_RED)
        )
        
        # Action buttons
        btn_layout = BoxLayout(size_hint_y=None, height=40, spacing=10)
        cancel_btn = Button(
            text='Cancel',
            background_color=hex_color((0.4, 0.4, 0.4, 1)),
            on_press=lambda x: self.dismiss()
        )
        ok_btn = Button(
            text='SET BATCH',
            font_size='14sp',
            bold=True,
            background_color=hex_color(COLOR_HIGHLIGHT),
            on_press=self.confirm
        )
        
        btn_layout.add_widget(cancel_btn)
        btn_layout.add_widget(ok_btn)
        
        layout.add_widget(title)
        layout.add_widget(batch_label)
        layout.add_widget(self.input_field)
        layout.add_widget(format_hint)
        layout.add_widget(self.error_label)
        layout.add_widget(btn_layout)
        
        self.add_widget(layout)
    
    def show_error(self, message):
        self.error_label.text = f" {message}"
    
    def clear_error(self):
        self.error_label.text = ''
    
    def confirm(self, instance):
        import re
        self.clear_error()
        batch = self.input_field.text.strip()
        
        if not batch:
            self.show_error("Batch number is required!")
            return
        
        # Check for lowercase letters - reject them
        if batch != batch.upper():
            self.show_error("Use UPPERCASE only! (e.g., BATCH-001)")
            return
        
        # Validate format: BATCH-XXX where XXX is only numbers
        pattern = r'^BATCH-\d+$'
        if not re.match(pattern, batch):
            self.show_error("Invalid format! Use BATCH-XXX (numbers only)")
            return
        
        # Extract the number part and validate it's not empty
        number_part = batch.replace('BATCH-', '')
        if not number_part or int(number_part) <= 0:
            self.show_error("Enter a valid batch number (e.g., BATCH-001)")
            return
        
        self.on_confirm(batch)
        self.dismiss()

class SimpleKegHMI(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.orientation = 'vertical'
        self.padding = [10, 10, 10, 10]
        self.spacing = 10
        
        Window.clearcolor = COLOR_BG_DARK
        
        # Initialize components
        self.camera = None
        self.database = DatabaseManager()
        self.api_sender = APISender()
        self.keg_detector = KegDetector()
        self.qr_detector = QRDetector()
        
        # Async Detection Setup
        self.detector_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="LiveDetector")
        self.current_detection_task = None
        self.latest_qr_results = []
        
        # Track last captured QR codes to detect same kegs
        self.last_captured_qr_set = set()
        
        # System state
        self.detection_active = False
        self.required_keg_count = DEFAULT_KEG_COUNT
        self.current_count = 0
        self.prev_count = 0
        self.stability_counter = 0
        self.is_auto_mode = False  # Start with manual mode by default
        self.processing = False
        self.beer_types = ["Loading..."]  # Wait for API
        self.beer_type_map = {} # Mapping name -> id


        # Load last batch number
        self.last_batch_number = load_last_batch()
        
        # Logs
        self.workflow_logs = []
        
        # Build UI
        self.build_ui()
        
        # Start systems
        self.start_detection_system()
        
        # Schedule tasks
        Clock.schedule_interval(self.update_frame, 1.0 / 20.0)  # Reduced for performance
        Clock.schedule_interval(self.check_network, 30)
        Clock.schedule_interval(self.update_filling_date, 60)  # Update filling date every minute
        Clock.schedule_once(self.recover_batches, 1)
        Clock.schedule_once(lambda dt: self.sync_cloud(), 2)
        Clock.schedule_once(self.fetch_beer_types, 3)
    
    def build_ui(self):
        """Build simplified and clean UI layout"""
        
        # Top row: Title and status
        top_bar = BoxLayout(size_hint_y=None, height=40, spacing=10)
        
        self.title_label = Label(
            text='[b]KEG COUNTING SYSTEM[/b]',
            markup=True,
            font_size='20sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        
        self.system_status = Label(
            text='[b]● READY[/b]',
            markup=True,
            font_size='14sp',
            color=hex_color(COLOR_STATUS_GREEN),
            halign='right'
        )
        
        top_bar.add_widget(self.title_label)
        top_bar.add_widget(self.system_status)
        
        # Main content area
        main_content = BoxLayout(orientation='horizontal', spacing=10)
        
        # Left panel - Camera (60%)
        left_panel = BoxLayout(orientation='vertical', size_hint_x=0.6, spacing=10)
        
        # Camera preview with border
        cam_container = BoxLayout(padding=2)
        with cam_container.canvas.before:
            Color(*COLOR_BG_DARK)
            Rectangle(pos=cam_container.pos, size=cam_container.size)
            Color(0.3, 0.3, 0.3, 1)
            Line(rectangle=(cam_container.pos[0], cam_container.pos[1], 
                          cam_container.size[0], cam_container.size[1]), width=1.5)
        
        # Fix deprecated properties
        self.preview_image = Image()
        self.preview_image.fit_mode = 'contain'  # Use fit_mode instead of allow_stretch and keep_ratio
        cam_container.add_widget(self.preview_image)
        left_panel.add_widget(cam_container)
        
        # Detection status below camera
        status_box = GridLayout(cols=2, rows=1, spacing=5, size_hint_y=None, height=40)
        
        # Target count
        target_label = Label(
            text='TARGET COUNT:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        self.target_display = Label(
            text=str(self.required_keg_count),
            font_size='24sp',
            bold=True,
            color=hex_color(COLOR_HIGHLIGHT),
            halign='right'
        )
        
        # Detected count removed as per user request
        # Keeping self.current_display as dummy to avoid breaking update logic?
        # Better to just update logic or create a hidden/dummy label if referenced elsewhere.
        # Searching indicates it IS referenced in update_ui.
        
        # Let's check where self.current_display is used.
        # It is updated in update_ui loop. 
        # I will create a dummy object or just remove the reference later?
        # Safest is to keep the variable but not add it to layout, 
        # OR create a dummy Label that isn't added.
        
        self.current_display = Label(opacity=0) # Dummy
        
        status_box.add_widget(target_label)
        status_box.add_widget(self.target_display)
        # status_box.add_widget(detected_label) # Removed
        # status_box.add_widget(self.current_display) # Removed from layout
        
        left_panel.add_widget(status_box)
        
        # Right panel - Controls (40%)
        right_panel = BoxLayout(orientation='vertical', size_hint_x=0.4, spacing=10)
        
        # Mode selector
        mode_box = BoxLayout(orientation='vertical', size_hint_y=None, height=60, spacing=5)
        mode_label = Label(
            #text='OPERATION MODE:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            size_hint_y=None,
            height=20
        )
        
        mode_buttons = BoxLayout(spacing=5)
        self.auto_btn = ToggleButton(
            text='AUTO',
            group='mode',
            state='normal',
            font_size='14sp',
            background_normal='',
            background_color=hex_color(COLOR_BUTTON_NORMAL)
        )
        self.auto_btn.bind(on_press=self.set_auto_mode)
        
        self.manual_btn = ToggleButton(
            text='MANUAL',
            group='mode',
            state='down',
            font_size='14sp',
            background_normal='',
            background_color=hex_color(COLOR_HIGHLIGHT)
        )
        self.manual_btn.bind(on_press=self.set_manual_mode)
        
        mode_buttons.add_widget(self.auto_btn)
        mode_buttons.add_widget(self.manual_btn)
        
        mode_box.add_widget(mode_label)
        mode_box.add_widget(mode_buttons)
        right_panel.add_widget(mode_box)
        
        # Configuration panel
        config_box = GridLayout(cols=2, rows=4, spacing=5, size_hint_y=None, height=200)
        
        # Beer type
        beer_label = Label(
            text='BEER TYPE:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        self.beer_display = Spinner(
            text='Loading...',
            values=[],
            font_size='14sp',
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal='',
            option_cls=lambda **kwargs: Button(
                **kwargs, 
                size_hint_y=None, 
                height=40, 
                background_color=hex_color(COLOR_PANEL_BG),
                background_normal=''
            )
        )
        self.beer_display.bind(text=self.on_beer_type_select)
        
        # Target count button
        count_label = Label(
            text='TARGET COUNT:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        self.count_button = Button(
            text=str(self.required_keg_count),
            font_size='14sp',
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal='',
            on_press=self.change_count
        )
        
        # Batch input - Use last batch number
        batch_label = Label(
            text='BATCH NO:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        self.batch_display = Button(
            text=self.last_batch_number,
            font_size='14sp',
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal='',
            on_press=self.edit_batch
        )
        
        config_box.add_widget(beer_label)
        config_box.add_widget(self.beer_display)
        config_box.add_widget(count_label)
        config_box.add_widget(self.count_button)
        config_box.add_widget(batch_label)
        config_box.add_widget(self.batch_display)
        
        # Filling Date/Time display (auto-populated, styled like other buttons)
        filling_label = Label(
            text='FILLING DATE:',
            font_size='12sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            halign='left'
        )
        self.filling_date_display = Button(
            text=datetime.now().strftime('%d-%m-%Y %H:%M'),
            font_size='14sp',
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal=''
        )
        config_box.add_widget(filling_label)
        config_box.add_widget(self.filling_date_display)
        
        right_panel.add_widget(config_box)
        
        # Status indicator
        status_box = BoxLayout(orientation='vertical', spacing=5, size_hint_y=None, height=60)
        
        self.status_label = Label(
            text='',
            font_size='14sp',
            bold=True,
            color=hex_color(COLOR_TEXT_LIGHT),
            size_hint_y=None,
            height=30
        )
        
        # Duplicate batch warning label
        self.duplicate_warning = Label(
            text='',
            font_size='12sp',
            color=hex_color(COLOR_STATUS_ORANGE),
            size_hint_y=None,
            height=20
        )
        
        status_box.add_widget(self.status_label)
        status_box.add_widget(self.duplicate_warning)
        
        # STATUS DISPLAY AREA - For showing pallet ID (reduced height)
        status_display_box = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            height=70,
            spacing=2,
            padding=[5, 2, 5, 2]
        )

        # Success Message (for pallet creation)
        self.success_display = Label(
            text='',
            font_size='13sp',
            bold=True,
            color=hex_color(COLOR_STATUS_GREEN),
            size_hint_y=None,
            height=22,
            halign='center',
            text_size=(380, None)
        )

        # Pallet ID Display
        self.pallet_display = Label(
            text='',
            font_size='14sp',
            bold=True,
            color=hex_color(COLOR_HIGHLIGHT),
            size_hint_y=None,
            height=28,
            halign='center',
            text_size=(380, None)
        )

        # Batch Info
        self.batch_info_label = Label(
            text='',
            font_size='11sp',
            color=hex_color(COLOR_TEXT_LIGHT),
            size_hint_y=None,
            height=18,
            halign='center',
            text_size=(380, None)
        )

        status_display_box.add_widget(self.success_display)
        status_display_box.add_widget(self.pallet_display)
        status_display_box.add_widget(self.batch_info_label)

        # Add pallet display box first
        right_panel.add_widget(status_display_box)
        
        # Add status_box (Target Achieved) right before action buttons - closer to SYNC/LOGS
        right_panel.add_widget(status_box)

        # Initialize as empty
        self.clear_status_display()
        
        # Action buttons
        action_grid = GridLayout(cols=2, rows=2, spacing=5, size_hint_y=None, height=100)
        
        sync_btn = Button(
            text='SYNC',
            font_size='12sp',
            background_color=hex_color(COLOR_STATUS_BLUE),
            background_normal='',
            on_press=lambda x: self.sync_cloud()
        )
        
        logs_btn = Button(
            text='LOGS',
            font_size='12sp',
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal='',
            on_press=self.show_logs
        )
        
        self.capture_btn = Button(
            text='CAPTURE',
            font_size='14sp',
            bold=True,
            background_color=hex_color(COLOR_BUTTON_NORMAL),
            background_normal='',
            disabled=True,
            on_press=self.force_capture
        )
        
        exit_btn = Button(
            text='EXIT',
            font_size='12sp',
            background_color=hex_color(COLOR_ALERT_RED),
            background_normal='',
            on_press=self.confirm_exit
        )
        
        action_grid.add_widget(sync_btn)
        action_grid.add_widget(logs_btn)
        action_grid.add_widget(self.capture_btn)
        action_grid.add_widget(exit_btn)
        
        right_panel.add_widget(action_grid)
        
        # Network status
        network_box = BoxLayout(size_hint_y=None, height=30)
        self.network_status = Label(
            text='● ONLINE',
            font_size='12sp',
            color=hex_color(COLOR_STATUS_GREEN),
            halign='left'
        )
        
        network_box.add_widget(self.network_status)
        right_panel.add_widget(network_box)
        
        main_content.add_widget(left_panel)
        main_content.add_widget(right_panel)
        
        # Add everything to main layout
        self.add_widget(top_bar)
        self.add_widget(main_content)
        
        # Add initial log
        self.add_log("System started - Manual mode")
    
    def show_pallet_created(self, pallet_id, batch_id):
        """Show pallet creation confirmation in the UI display"""
        print(f"\n[SHOW_PALLET_CREATED] Displaying pallet: {pallet_id} for batch: {batch_id}")
        # Format the text
        self.success_display.text = 'PALLET CREATED'
        self.pallet_display.text = pallet_id
        self.batch_info_label.text = f'Batch: {batch_id}'
        
        # Make it visible
        self.success_display.opacity = 1
        self.pallet_display.opacity = 1
        self.batch_info_label.opacity = 1
        
        print(f"[SHOW_PALLET_CREATED] UI updated - success_display: '{self.success_display.text}'")
        print(f"[SHOW_PALLET_CREATED] UI updated - pallet_display: '{self.pallet_display.text}'")
        
        # Auto-clear after 30 seconds
        Clock.schedule_once(lambda dt: self.clear_status_display(), 30)

    def show_status_message(self, message, msg_type="info"):
        """Show a status message in the display area"""
        if msg_type == "success":
            color = COLOR_STATUS_GREEN
        elif msg_type == "error":
            color = COLOR_ALERT_RED
        else:
            color = COLOR_STATUS_BLUE
        
        self.success_display.text = message
        self.success_display.color = hex_color(color)
        self.pallet_display.text = ''
        self.batch_info_label.text = ''
        
        # Make it visible
        self.success_display.opacity = 1
        self.pallet_display.opacity = 0
        self.batch_info_label.opacity = 0
        
        # Auto-clear after 10 seconds
        Clock.schedule_once(lambda dt: self.clear_status_display(), 10)

    def clear_status_display(self):
        """Clear the status display area"""
        self.success_display.text = ''
        self.pallet_display.text = ''
        self.batch_info_label.text = ''
        self.success_display.opacity = 0.5
        self.pallet_display.opacity = 0.5
        self.batch_info_label.opacity = 0.5
    
    def fetch_beer_types(self, dt=None):
        """Fetch beer types from cloud API (Background Thread)"""
        threading.Thread(target=self._fetch_beer_types_thread, daemon=True).start()

    def _fetch_beer_types_thread(self):
        try:
            beer_types = self.api_sender.get_beer_types()
            if beer_types:
                Clock.schedule_once(lambda dt: self._update_beer_types(beer_types))
            else:
                self.add_log("Using default beer types")
        except Exception as e:
            self.add_log(f"Failed to fetch beer types: {str(e)[:50]}")

    def _update_beer_types(self, beer_types_data):
        # Handle list of dicts [{'name': '...', 'id': '...'}]
        self.beer_type_map = {}
        names = []
        
        # Preserve current selection
        current_selection = self.beer_display.text
        
        for item in beer_types_data:
            if isinstance(item, dict):
                name = item.get('name', 'Unknown')
                # Cloud uses _id, fallback to id, then name
                bid = item.get('_id', item.get('id', name))
                self.beer_type_map[name] = bid
                names.append(name)
            else:
                # Fallback for strings
                name = str(item)
                self.beer_type_map[name] = name
                names.append(name)
                
        self.beer_types = names if names else ["Lager"]
        
        # Update spinner values
        self.beer_display.values = self.beer_types
        
        # Restore selection if it exists in new list, otherwise default to first
        if current_selection in self.beer_types:
            self.beer_display.text = current_selection
        else:
            self.beer_display.text = self.beer_types[0]
            
        self.add_log(f"Beer types loaded: {len(names)} types")
    
    def set_auto_mode(self, instance):
        self.is_auto_mode = True
        self.auto_btn.background_color = hex_color(COLOR_HIGHLIGHT)
        self.manual_btn.background_color = hex_color(COLOR_BUTTON_NORMAL)
        self.add_log("Auto mode: Syncing with cloud...")
        self.sync_cloud()
    
    def set_manual_mode(self, instance):
        self.is_auto_mode = False
        self.manual_btn.background_color = hex_color(COLOR_HIGHLIGHT)
        self.auto_btn.background_color = hex_color(COLOR_BUTTON_NORMAL)
        self.add_log("Manual mode: Set keg count manually")
    
    def on_beer_type_select(self, spinner, text):
        """Handle beer type selection"""
        self.add_log(f"Beer type selected: {text}")
    
    def update_filling_date(self, dt=None):
        """Update the filling date display"""
        self.filling_date_display.text = datetime.now().strftime('%d-%m-%Y %H:%M')
    
    def change_count(self, instance):
        """Open modal to change target keg count"""
        modal = CountModal(self.set_count, self.required_keg_count)
        modal.open()
    
    def set_count(self, count):
        """Set the target keg count"""
        self.required_keg_count = count
        self.count_button.text = str(count)
        self.target_display.text = str(count)
        self.add_log(f"Target count set to: {count}")
        self.show_toast(f"Target count set to {count}", "success")
    
    def edit_batch(self, instance):
        """Edit batch number"""
        modal = BatchModal(self.set_batch, self.batch_display.text)
        modal.open()
    
    def set_batch(self, batch):
        """Set batch number and save it"""
        self.batch_display.text = batch
        self.last_batch_number = batch
        save_last_batch(batch)
        log_msg = f"Batch: {batch}"
        self.add_log(log_msg)
        self.show_toast(f"Batch number set to: {batch}", "success")
        
        # Clear duplicate warning when batch number changes
        self.duplicate_warning.text = ''
        self.status_label.text = 'Waiting for kegs...'
        self.status_label.color = hex_color(COLOR_TEXT_LIGHT)

    def increment_batch_number(self):
        """Auto-increment batch number after success"""
        current = self.batch_display.text
        # Try to find trailing number
        import re
        match = re.search(r'(\d+)$', current)
        if match:
            num_str = match.group(1)
            num_len = len(num_str)
            new_num = int(num_str) + 1
            new_batch = current[:match.start()] + str(new_num).zfill(num_len)
            self.set_batch(new_batch)
        else:
            # Append -1 if no number
            self.set_batch(current + "-1")
    
    def sync_cloud(self):
        """Sync configuration with cloud (Non-blocking)"""
        # Avoid starting multiple sync threads
        if hasattr(self, 'syncing') and self.syncing:
            self.add_log("Sync already in progress...")
            return

        if self.is_auto_mode:
            self.syncing = True
            self.add_log("Syncing with cloud...")
            threading.Thread(target=self._sync_thread, daemon=True).start()
        else:
            self.add_log("Manual mode - using local config")
    
    def _sync_thread(self):
        """Background thread for cloud sync"""
        try:
            # Try to get configuration from cloud
            mac_address = CAMERA_MAC_ID
            if not mac_address or mac_address == "3C:6D:66:01:5A:F0":
                # Get actual MAC address
                mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) 
                               for elements in range(0, 2*6, 2)][::-1])
                mac_address = mac.upper()
            
            payload = {"macId": mac_address}
            
            # Use configured endpoint
            endpoints = [CLOUD_CONFIG_ENDPOINT]
            
            success = False
            for endpoint in endpoints:
                try:
                    response = requests.post(
                        endpoint,
                        json=payload,
                        timeout=5,
                        verify=False
                    )
                    if response.status_code == 200:
                        data = response.json()
                        # Parse configuration from response
                        keg_type = data.get("keg_type", "30L")
                        count = KEG_TYPES.get(keg_type, {}).get('EUR Pallet', DEFAULT_KEG_COUNT)
                        
                        # Apply updates on main thread
                        Clock.schedule_once(lambda dt: self._apply_sync_success(count, keg_type))
                        success = True
                        break
                except:
                    continue
            
            if not success:
                 Clock.schedule_once(lambda dt: self._apply_sync_fail("Cloud sync failed: No valid response"))

        except Exception as e:
            err_msg = str(e)
            Clock.schedule_once(lambda dt: self._apply_sync_fail(f"Sync error: {err_msg[:50]}"))
        finally:
            self.syncing = False

    def _apply_sync_success(self, count, keg_type):
        """Update UI with sync results (Main Thread)"""
        self.required_keg_count = count
        self.count_button.text = str(count)
        self.target_display.text = str(count)
        self.add_log(f"Cloud sync: {count} {keg_type} kegs")
        
    def _apply_sync_fail(self, error_msg):
        """Log sync failure (Main Thread)"""
        self.add_log(error_msg)
    
    def update_frame(self, dt):
        """Update camera frame and process detection"""
        if not self.detection_active or not self.camera:
            # Show camera not initialized
            try:
                # Create black frame (placeholder size 640x480)
                img_h, img_w = 480, 640
                vis_frame = np.zeros((img_h, img_w, 3), dtype=np.uint8)
                
                # Draw text
                text = "Camera Not Initialized"
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 1.0
                thickness = 2
                text_size = cv2.getTextSize(text, font, font_scale, thickness)[0]
                text_x = (img_w - text_size[0]) // 2
                text_y = (img_h + text_size[1]) // 2
                
                # Red text
                cv2.putText(vis_frame, text, (text_x, text_y), font, font_scale, (0, 0, 255), thickness)
                
                # Update preview
                buf = cv2.flip(vis_frame, 0).tobytes()
                texture = Texture.create(size=(img_w, img_h), colorfmt='bgr')
                texture.blit_buffer(buf, colorfmt='bgr', bufferfmt='ubyte')
                self.preview_image.texture = texture
            except Exception:
                pass
            return

        if self.processing:
            return
        
        ret, frame = self.camera.get_frame()
        if ret and frame is not None:
            # Update preview
            # Create visualization frame
            vis_frame = frame.copy()
            
            # Detect and draw QR codes
            try:
                # Async Detection Logic - "Latest Frame Only" Strategy

                # 1. Check if ANY previous detection finished
                if self.current_detection_task and self.current_detection_task.done():
                    try:
                        # Get results (list of dicts, total_boxes)
                        results = self.current_detection_task.result()
                        if results:
                            self.latest_qr_results = results[0] # Index 0 is the list of QRs
                    except Exception as e:
                        pass # Squelch errors to keep UI alive
                    finally:
                        self.current_detection_task = None

                # 2. Submit new detection if idle
                # Only submit if we are not currently processing a frame
                if self.current_detection_task is None:
                    # IMPORTANT: Pass a COPY of the frame to the thread to avoid memory race conditions
                    # detector handles resizing internally
                    self.current_detection_task = self.detector_executor.submit(
                        self.qr_detector.detect_and_decode, 
                        vis_frame.copy()
                    )

                # 3. Draw LATEST known results (Visual Feedback)
                # This happens at clock speed (UI FPS), decoupled from detection speed
                for qr in self.latest_qr_results:
                    x1, y1, x2, y2 = qr['bbox']
                    # Draw green boundary
                    cv2.rectangle(vis_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    
                    # Draw text label background
                    label = qr['data'][:10] + '...' if len(qr['data']) > 10 else qr['data']
                    t_size = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)[0]
                    cv2.rectangle(vis_frame, (x1, y1-20), (x1+t_size[0], y1), (0, 255, 0), -1)
                    cv2.putText(vis_frame, label, (x1, y1-5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)

            except Exception as e:
                pass
                # self.add_log(f"QR Vis Error: {str(e)[:20]}")

            # Update preview with visualization frame
            buf = cv2.flip(vis_frame, 0).tobytes()
            texture = Texture.create(size=(vis_frame.shape[1], vis_frame.shape[0]), colorfmt='bgr')
            texture.blit_buffer(buf, colorfmt='bgr', bufferfmt='ubyte')
            texture.flip_vertical()
            self.preview_image.texture = texture
            
            # Process frame for keg detection
            self.process_frame(frame)
    
    def process_frame(self, frame):
        """Process frame for keg detection"""
        try:
            detection = self.keg_detector.detect(frame)
            new_count = detection['count']
            
            # Check stability
            if new_count == self.prev_count:
                self.stability_counter = min(self.stability_counter + 1, STABILITY_THRESHOLD)
            else:
                self.stability_counter = max(self.stability_counter - 2, 0)
            
            self.prev_count = new_count
            self.current_count = new_count
            
            # Update UI
            self.update_display()
            
            # Get effective count (best of keg or QR detection)
            qr_count = len(self.latest_qr_results) if hasattr(self, 'latest_qr_results') else 0
            effective_count = max(self.current_count, qr_count)
            
            # Auto-capture when stable, count matches rules, and not empty
            if (self.is_auto_mode and
                self.stability_counter >= STABILITY_THRESHOLD and 
                not self.processing and 
                not getattr(self, 'auto_confirm_pending', False) and
                effective_count > 0 and
                effective_count <= self.required_keg_count):
                self.show_auto_capture_confirmation(frame)
                
        except Exception as e:
            self.add_log(f"Detect error: {str(e)[:50]}")
    
    def update_display(self):
        """Update all UI displays"""
        # Update count displays
        self.current_display.text = str(self.current_count)
        
        # Get QR count from live detection
        qr_count = len(self.latest_qr_results) if hasattr(self, 'latest_qr_results') else 0
        
        # Use the better of keg count or QR count for target status
        effective_count = max(self.current_count, qr_count)
        
        # Update colors based on status
        if self.processing:
            self.current_display.color = hex_color(COLOR_STATUS_ORANGE)
            self.system_status.text = '[b]● PROCESSING[/b]'
            self.system_status.color = hex_color(COLOR_STATUS_ORANGE)
            self.status_label.text = "Processing batch..."
            self.status_label.color = hex_color(COLOR_STATUS_ORANGE)
        else:
            # Update status label based on detected vs target count
            # Show effective count vs target in simple format
            if effective_count < self.required_keg_count:
                self.status_label.text = f"Target Not Achieved ({effective_count}/{self.required_keg_count})"
                self.status_label.color = hex_color(COLOR_STATUS_ORANGE)
                self.system_status.text = '[b]● DETECTING[/b]'
                self.system_status.color = hex_color(COLOR_STATUS_BLUE)
            elif effective_count == self.required_keg_count:
                self.status_label.text = f"Target Achieved ({effective_count}/{self.required_keg_count})"
                self.status_label.color = hex_color(COLOR_STATUS_GREEN)
                self.system_status.text = '[b]● READY TO CAPTURE[/b]'
                self.system_status.color = hex_color(COLOR_STATUS_GREEN)
            else:  # effective_count > required_keg_count
                self.status_label.text = f"WARNING: Count exceeds target! ({effective_count}/{self.required_keg_count})"
                self.status_label.color = hex_color(COLOR_ALERT_RED)
                self.system_status.text = '[b]● CHECK COUNT[/b]'
                self.system_status.color = hex_color(COLOR_ALERT_RED)
            
        # Button Logic - Override for Manual Mode
        if not self.processing:
            if not self.is_auto_mode:
                # Manual Mode: Enable capture ONLY if we see kegs OR QR codes
                if effective_count > 0:
                    self.capture_btn.disabled = False
                    self.capture_btn.background_color = hex_color(COLOR_HIGHLIGHT)
                else:
                    self.capture_btn.disabled = True
                    self.capture_btn.background_color = hex_color(COLOR_BUTTON_NORMAL)
            else:
                # Auto Mode: Capture button always disabled - use auto-trigger only
                self.capture_btn.disabled = True
                self.capture_btn.background_color = hex_color(COLOR_BUTTON_NORMAL)

        # Progress bar removed - stability counter still tracked internally for auto-capture
    
    def show_auto_capture_confirmation(self, frame):
        """Show confirmation dialog for auto-capture (AUTO mode)"""
        # Prevent multiple dialogs
        if getattr(self, 'auto_confirm_pending', False):
            return
        
        # Check cooldown - prevent rapid re-triggering after cancel or capture
        if getattr(self, 'auto_capture_cooldown', False):
            return
        
        # Check if same kegs are still under camera (compare QR codes)
        current_qr_set = set(qr['data'] for qr in self.latest_qr_results) if self.latest_qr_results else set()
        if current_qr_set and self.last_captured_qr_set:
            # Check overlap - if more than 50% match, same kegs are still there
            overlap = len(current_qr_set & self.last_captured_qr_set)
            if overlap > 0 and overlap >= len(current_qr_set) * 0.5:
                # Same kegs still under camera - don't trigger
                self.duplicate_warning.text = "Same kegs detected - waiting for new pallet"
                self.duplicate_warning.color = hex_color(COLOR_STATUS_ORANGE)
                return
        
        # Validate beer type
        if self.beer_display.text in ['Loading...', '', None]:
            self.show_toast("Please select a beer type first!", "error")
            return
        
        # Validate batch number
        batch = self.batch_display.text.strip()
        if not batch or batch == 'BATCH-':
            self.show_toast("Please enter a valid batch number!", "error")
            return
        
        # Check for duplicate batch number
        is_duplicate, prev_session = self.database.is_batch_number_sent(batch)
        if is_duplicate:
            # In auto mode: show warning and do NOT show popup
            # Operator needs to change batch number manually
            self.duplicate_warning.text = f"DUPLICATE: {batch} already sent!"
            self.duplicate_warning.color = hex_color(COLOR_ALERT_RED)
            self.status_label.text = "Change batch number to continue"
            self.status_label.color = hex_color(COLOR_STATUS_ORANGE)
            self.stability_counter = 0  # Reset to avoid re-trigger immediately
            self.add_log(f"Duplicate batch {batch} - auto-skipped")
            # Don't show popup, just return
            return
        
        # Clear any previous duplicate warning
        self.duplicate_warning.text = ''
        
        self.auto_confirm_pending = True
        beer_type = self.beer_display.text
        # Use target count (user-entered) for confirmation, not detected count
        # Detection triggers the capture, but target count represents expected kegs in pallet
        message = f"Auto-capture {self.required_keg_count} kegs?\n\nBeer Type: {beer_type}\nBatch: {batch}"
        
        # Store frame for capture
        self._pending_frame = frame.copy()
        
        def start_cooldown():
            """Start cooldown timer to prevent immediate re-trigger"""
            self.auto_capture_cooldown = True
            self.add_log("Auto-capture cooldown: 10 seconds")
            # Clear cooldown after 10 seconds
            Clock.schedule_once(lambda dt: setattr(self, 'auto_capture_cooldown', False), 10)
        
        def on_confirm():
            self.auto_confirm_pending = False
            if hasattr(self, '_pending_frame') and self._pending_frame is not None:
                # Save current QR codes for same-keg detection
                self.last_captured_qr_set = set(qr['data'] for qr in self.latest_qr_results) if self.latest_qr_results else set()
                self.show_toast("Capturing kegs...", "info")
                self.trigger_capture(self._pending_frame)
                self._pending_frame = None
                # Start cooldown after successful capture
                start_cooldown()
        
        def on_cancel():
            self.auto_confirm_pending = False
            self._pending_frame = None
            self.stability_counter = 0  # Reset stability
            # Start cooldown so popup doesn't immediately reappear
            start_cooldown()
            self.show_toast("Cancelled - waiting 10 seconds before next auto-capture", "info")
        
        modal = ConfirmationModal(
            title="Confirm Auto-Capture",
            message=message,
            on_confirm=on_confirm,
            on_cancel=on_cancel
        )
        modal.open()
    
    def trigger_capture(self, frame):
        """Trigger batch capture"""
        print("\n" + "="*60)
        print("TRIGGER_CAPTURE STARTED")
        print("="*60)
        
        if self.processing:
            print("  -> BLOCKED: Already processing")
            return
        
        # Clear any previous status display
        self.clear_status_display()
        
        beer_name = self.beer_display.text
        # Resolve ID from name
        beer_id = self.beer_type_map.get(beer_name, beer_name)
        
        batch = self.batch_display.text
        
        # Store user's batch for display in callbacks
        self.current_batch_number = batch
        
        print(f"  Beer Name: {beer_name}")
        print(f"  Beer ID: {beer_id}")
        print(f"  Batch: {batch}")
        print(f"  Required Keg Count: {self.required_keg_count}")
        print(f"  Current Count: {self.current_count}")
        
        if not batch:
            print("  -> ERROR: Please enter batch number!")
            self.add_log("Please enter batch number!")
            return
        
        print(f"\n[CAPTURE] Triggered! Beer: {beer_name} (ID: {beer_id}) | Batch: {batch}")
        self.processing = True
        self.add_log(f"Capturing {self.current_count} kegs...")
        
        # Save frame
        image_name = f"batch_{create_timestamp()}.jpg"
        frame_path = str(SAVE_FOLDER / image_name)
        print(f"  -> Saving frame to: {frame_path}")
        cv2.imwrite(frame_path, frame)
        print(f"  -> Frame saved successfully")
        
        # Generate session ID
        session_id = f"BATCH_{self.database.get_next_batch_number():04d}"
        print(f"  -> Session ID: {session_id}")
        
        def process_capture():
            print(f"\n{'='*60}")
            print(f"PROCESS_CAPTURE THREAD STARTED")
            print(f"{'='*60}")
            print(f"  Session ID: {session_id}")
            print(f"  Frame Path: {frame_path}")
            print(f"  Image Name: {image_name}")
            print(f"  Required Count: {self.required_keg_count}")
            print(f"  Beer Type ID: {beer_id}")
            print(f"  Batch: {batch}")
            print(f"  -> Calling submit_batch...")
            
            try:
                # Capture exact filling timestamp
                filling_timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                print(f"  Filling Date: {filling_timestamp}")
                
                submit_batch(
                    frame_path, image_name, session_id, self.required_keg_count,
                    beer_type=beer_id, 
                    batch=batch,
                    filling_date=filling_timestamp
                )
                print(f"  -> submit_batch completed")
                Clock.schedule_once(lambda dt: self.complete_capture(session_id), 3)
            except Exception as e:
                print(f"  -> ERROR in submit_batch: {e}")
                import traceback
                traceback.print_exc()
                self.add_log(f"Capture error: {str(e)[:50]}")
                Clock.schedule_once(lambda dt: self.capture_failed(), 0)
        
        print("  -> Starting process_capture thread...")
        threading.Thread(target=process_capture, daemon=True).start()
        print("  -> Thread started")
    
    def complete_capture(self, session_id):
        """Handle successful capture completion"""
        self.processing = False
        self.stability_counter = 0
        
        # Use user's batch number for display
        user_batch = getattr(self, 'current_batch_number', session_id)
        self.show_status_message(f"Sent: {user_batch}", "success")
        self.show_toast(f"Batch {user_batch} submitted successfully!", "success")
        
        self.add_log(f"Batch {user_batch} submitted")
        Clock.schedule_once(lambda dt: self.check_status(session_id, user_batch), 3)
    
    def capture_failed(self):
        """Handle capture failure"""
        self.processing = False
        self.stability_counter = 0
        self.add_log("Capture failed - check logs")
        self.show_toast("Capture Failed! Check logs for details.", "error")
    
    def check_status(self, session_id, user_batch=None):
        """Check batch status and show pallet confirmation"""
        # Use user_batch for display, session_id for DB lookup
        display_batch = user_batch or session_id
        print(f"\n[CHECK_STATUS] Checking status for: {session_id} (display: {display_batch})")
        status = self.database.get_batch_status(session_id)
        print(f"[CHECK_STATUS] Batch status: {status}")
        
        if status == 'api_failed':
            self.add_log(f"Batch {display_batch} failed to send")
            self.show_status_message(f"Batch Failed: {display_batch}", "error")
            self.show_toast(f"API Error: Batch {display_batch} failed!", "error", 5)
        elif status == 'api_sent':
            # Get response to extract Pallet ID
            response_json = self.database.get_batch_response(session_id)
            print(f"[CHECK_STATUS] API Response from DB: {response_json}")
            
            # Auto-increment batch number for next run
            self.increment_batch_number()
            
            if response_json:
                try:
                    data = json.loads(response_json)
                    print(f"[CHECK_STATUS] Parsed JSON: {data}")
                    pallet_id = data.get('paletteId') or data.get('palletId') or data.get('pallet_id') or data.get('id')
                    print(f"[CHECK_STATUS] Extracted Pallet ID: {pallet_id}")
                    
                    if pallet_id:
                        # Show in UI display prominently with user's batch
                        print(f"[CHECK_STATUS] Calling show_pallet_created({pallet_id}, {display_batch})")
                        self.show_pallet_created(pallet_id, display_batch)
                        # Also log it
                        self.add_log(f"Pallet Created: {pallet_id}")
                        self.show_toast(f"Pallet Created: {pallet_id}", "success", 5)
                    else:
                        print("[CHECK_STATUS] No pallet_id found, showing success message")
                        # Just show success message with user's batch
                        self.show_status_message(f"Batch {display_batch} Sent", "success")
                except Exception as e:
                    print(f"[CHECK_STATUS] Error parsing response: {e}")
                    self.show_status_message(f"Batch {display_batch} Sent", "success")
            else:
                print("[CHECK_STATUS] No response_json from DB")
                self.show_status_message(f"Batch {display_batch} Sent", "success")
    
    def force_capture(self, instance):
        """Manual capture trigger with confirmation dialog"""
        print("\n" + "="*60)
        print("CAPTURE BUTTON CLICKED")
        print("="*60)
        print(f"  Processing: {self.processing}")
        print(f"  Current Count: {self.current_count}")
        print(f"  Required Count: {self.required_keg_count}")
        
        # Validate inputs first
        if self.processing:
            self.show_toast("Already processing a batch. Please wait.", "warning")
            return
        
        if self.current_count <= 0:
            self.show_toast("No kegs detected! Cannot capture.", "error")
            return
        
        # Validate beer type
        if self.beer_display.text in ['Loading...', '', None]:
            self.show_toast("Please select a beer type first!", "error")
            return
        
        # Validate batch number
        batch = self.batch_display.text.strip()
        if not batch:
            self.show_toast("Please enter a valid batch number!", "error")
            return
        
        # Show confirmation dialog
        # Check for duplicate batch number
        is_duplicate, prev_session = self.database.is_batch_number_sent(batch)
        duplicate_warning = ""
        if is_duplicate:
            duplicate_warning = "\n\n⚠ WARNING: This batch was already sent!"
            self.duplicate_warning.text = f"⚠ DUPLICATE: {batch}"
            self.duplicate_warning.color = hex_color(COLOR_STATUS_ORANGE)
        else:
            self.duplicate_warning.text = ''
        
        # Use target count (user-entered) for confirmation, not detected count
        beer_type = self.beer_display.text
        message = f"Capture {self.required_keg_count} kegs?\n\nBeer Type: {beer_type}\nBatch: {batch}{duplicate_warning}"
        
        def on_confirm():
            print("  -> Confirmation received, getting frame...")
            ret, frame = self.camera.get_frame()
            print(f"  -> Frame returned: ret={ret}, frame is None={frame is None}")
            if ret and frame is not None:
                print("  -> Calling trigger_capture...")
                self.show_toast("Capturing kegs...", "info")
                self.trigger_capture(frame)
            else:
                print("  -> FAILED: Could not get frame from camera")
                self.show_toast("Camera error! Could not get frame.", "error")
        
        modal = ConfirmationModal(
            title="Confirm Capture",
            message=message,
            on_confirm=on_confirm
        )
        modal.open()
    
    def show_toast(self, message, msg_type="info", duration=3):
        """Show a toast notification to the user"""
        toast = ToastMessage(message, msg_type, duration)
        toast.open()
    
    def check_network(self, dt):
        """Check network connectivity via API Sender"""
        if self.api_sender:
            is_online = self.api_sender.get_network_status()
            self._update_network_status(is_online)

    def _update_network_status(self, is_online):
        if is_online:
            self.network_status.text = 'ONLINE'
            self.network_status.color = hex_color(COLOR_STATUS_GREEN)
        else:
            self.network_status.text = 'OFFLINE'
            self.network_status.color = hex_color(COLOR_ALERT_RED)
    
    def recover_batches(self, dt):
        """Recover stuck batches"""
        try:
            stuck = self.database.get_stuck_batches(timeout_minutes=5)
            if stuck:
                self.add_log(f"Recovered {len(stuck)} stuck batches")
        except:
            pass
    
    def start_detection_system(self):
        """Initialize camera system"""
        try:
            self.camera = CameraManager(CAMERA_CONFIG)
            if self.camera.start():
                self.detection_active = True
                self.add_log("Camera started successfully")
            else:
                self.add_log("Camera failed to start")
        except Exception as e:
            self.add_log(f"Camera error: {str(e)[:50]}")
    
    def add_log(self, message):
        """Add message to log display"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        self.workflow_logs.append(log_entry)
        
        # Keep last 10 logs
        if len(self.workflow_logs) > 10:
            self.workflow_logs = self.workflow_logs[-10:]
        
        # Update system logs (optional - can be viewed in logs popup)
    
    def show_logs(self, instance):
        """Show detailed logs popup"""
        from kivy.uix.popup import Popup
        
        popup = Popup(title='System Logs', size_hint=(0.8, 0.6))
        
        scroll = ScrollView()
        content = BoxLayout(orientation='vertical', size_hint_y=None)
        content.bind(minimum_height=content.setter('height'))
        
        for log_entry in reversed(self.workflow_logs):
            label = Label(
                text=log_entry,
                font_size='10sp',
                color=hex_color(COLOR_TEXT_LIGHT),
                size_hint_y=None,
                height=25,
                halign='left',
                text_size=(400, None)
            )
            content.add_widget(label)
        
        scroll.add_widget(content)
        popup.content = scroll
        popup.open()
    
    def confirm_exit(self, instance):
        """Show confirmation dialog before exiting"""
        def do_exit():
            App.get_running_app().stop()
        
        modal = ConfirmationModal(
            title="Exit Application",
            message="Are you sure you want to exit?",
            on_confirm=do_exit
        )
        modal.open()
    
    def on_stop(self):
        """Cleanup on application stop"""
        if self.camera:
            self.camera.stop()
        if hasattr(self, 'api_sender'):
            self.api_sender.stop_retry_monitor()
            self.api_sender.close()

class SimpleKegApp(App):
    def build(self):
        self.title = 'Keg Counting System'
        Window.clearcolor = COLOR_BG_DARK
        Window.size = (1024, 600)  # Standard industrial display size
        return SimpleKegHMI()
    
    def on_stop(self):
        if hasattr(self, 'root') and self.root:
            self.root.on_stop()

if __name__ == '__main__':
    SimpleKegApp().run()