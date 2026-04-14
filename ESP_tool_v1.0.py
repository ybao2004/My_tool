#!/usr/bin/env python3
# ultimate_esp_inspector_v1.0.py
import sys
import threading
import re
import traceback
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import time
import multiprocessing
import io
import codecs
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
import os
import json

# ==========================================
# --- KHU VỰC DỄ DÀNG TÙY CHỈNH (CONFIG) ---
# ==========================================
# 1. Đường dẫn thư mục lưu file cấu hình. 
# Mặc định: os.path.expanduser("~") -> (Thường là C:\Users\<Tên_User_Của_Bạn>)
# Bạn có thể đổi thành ổ đĩa khác, ví dụ: CONFIG_DIR = "D:/" hoặc CONFIG_DIR = "./" (lưu ở thư mục code hiện tại)
CONFIG_DIR = os.path.expanduser("~") 

# 2. Tên file cấu hình
# ── Settings file: ~/setting_<scriptname>.json ─────────────────────
_SCRIPT_NAME = os.path.splitext(os.path.basename(
    sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
))[0]
CONFIG_FILENAME = os.path.join(os.path.expanduser("~"), f"setting_{_SCRIPT_NAME}.json")

# Đường dẫn đầy đủ của file cấu hình (Tự động nối thư mục và tên file)
CONFIG_FILE = os.path.join(CONFIG_DIR, CONFIG_FILENAME)

REPORT_FONT_SIZE = 10
DEFAULT_WINDOW_WIDTH = 1280 
DEFAULT_WINDOW_HEIGHT = 720
DEFAULT_GEOMETRY = f"{DEFAULT_WINDOW_WIDTH}x{DEFAULT_WINDOW_HEIGHT}+20+20"

Tu_dong_cuon = True
Them_dau_thoi_gian = False
So_dong_serial_toi_da = 20000

# Cấu hình tên hiển thị và lệnh gửi đi cho các nút F1 -> F12
CONTROL_CONFIG = {
    1: ("F1", "CMD_ON"),
    2: ("F2", "CMD_OFF"),
    3: ("F3", "CMD_RESET"),
    4: ("F4", "CMD_INFO"),
    5: ("F5", "CMD_MODE_1"),
    6: ("F6", "CMD_MODE_2"),
    7: ("F7", "CMD_TEST"),
    8: ("F8", "CMD_STOP"),
    9: ("F9", "CMD_CUSTOM_1"),
    10: ("F10", "CMD_CUSTOM_2"),
    11: ("F11", "CMD_CUSTOM_3"),
    12: ("F12", "CMD_CUSTOM_4")
}

# Cấu hình lệnh gửi đi cho các tác vụ Mạng (WiFi / Server)
NETWORK_COMMANDS = {
    "SCAN_WIFI": "SCAN_WIFI",                 
    "CONNECT_WIFI": "CONNECT_WIFI",           
    "DISCONNECT_WIFI": "DISCONNECT_WIFI",     
    "CONNECT_SERVER": "CONNECT_SERVER",       
    "DISCONNECT_SERVER": "DISCONNECT_SERVER"  
}

# --- BẢNG MÀU CHO GIAO DIỆN (PALETTES) ---
LIGHT_PALETTE = {
    "bg": "#f0f0f0", "fg": "#000000", "text_bg": "#ffffff", "text_fg": "#000000",
    "btn_bg": "#e1e1e1", "btn_hover": "#d4d4d4", "btn_pressed": "#c8c8c8",
    "disabled_bg": "#e9e9e9", "disabled_fg": "#8a8a8a", "border": "#cccccc",
    "accent": "#005fb8", "success": "#008000", "error": "#d50000", "warning": "#d55e00"
}

DARK_PALETTE = {
    "bg": "#2b2b2b", "fg": "#f0f0f0", "text_bg": "#1e1e1e", "text_fg": "#ffffff",
    "btn_bg": "#444444", "btn_hover": "#5a5a5a", "btn_pressed": "#333333",
    "disabled_bg": "#3a3a3a", "disabled_fg": "#a0a0a0", "border": "#4d4d4d",
    "accent": "#4dabf7", "success": "#00e676", "error": "#ff6b6b", "warning": "#ffaa00"
}
# ==========================================

try:
    from serial.tools import list_ports
    import serial
    import esptool
except ImportError:
    print("FATAL ERROR: Required packages 'pyserial' or 'esptool' not found.")
    sys.exit(1)

esptool_lock = threading.Lock()
COMMON_BAUDS = ["Tự động", "9600", "57600", "74880", "115200", "230400", "460800", "921600"]

# --- CSDL Chip và Flash ---
FLASH_MANUFACTURERS = { "0x20": "XMC", "0x68": "XMC", "0xC8": "GigaDevice", "0xEF": "Winbond", "0x1C": "EON", "0x0B": "Puya", "0xA1": "Fudan", "0xC2": "Macronix (MX)", "0xE0": "Fremont"}
CHIP_DATABASE = { 
    "ESP32": { "name": "ESP32", "architecture": "Xtensa® dual-core 32-bit LX6", "cores": 2, "cpu_freq_mhz": [160, 240], "sram_kb": 520, "rom_kb": 448, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": "Bluetooth v4.2 BR/EDR + BLE", "features": ["ADC", "DAC", "TWAI® (CAN)", "Cảm biến Hall"] }, 
    "ESP32-D2WD": { "name": "ESP32-D2WD", "architecture": "Xtensa® dual-core 32-bit LX6", "cores": 2, "cpu_freq_mhz": [160, 240], "sram_kb": 520, "rom_kb": 448, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": "Bluetooth v4.2 BR/EDR + BLE", "features": ["Flash 2MB tích hợp", "ADC", "DAC"] }, 
    "ESP32-S2": { "name": "ESP32-S2", "architecture": "Xtensa® single-core 32-bit LX7", "cores": 1, "cpu_freq_mhz": [160, 240], "sram_kb": 320, "rom_kb": 128, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": None, "features": ["USB OTG", "LCD/Camera Interface", "Cảm biến nhiệt độ"] }, 
    "ESP32-S3": { "name": "ESP32-S3", "architecture": "Xtensa® dual-core 32-bit LX7", "cores": 2, "cpu_freq_mhz": [160, 240], "sram_kb": 512, "rom_kb": 384, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": "Bluetooth 5 (LE)", "features": ["AI Acceleration", "USB OTG", "LCD/Camera Interface"] }, 
    "ESP32-C2": { "name": "ESP32-C2 (ESP8685)", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [120], "sram_kb": 272, "rom_kb": 576, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": "Bluetooth 5 (LE)", "features": ["Low Power Consumption"] }, 
    "ESP32-C3": { "name": "ESP32-C3", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [160], "sram_kb": 400, "rom_kb": 384, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": "Bluetooth 5 (LE)", "features": ["ADC", "TWAI® (CAN)", "Cảm biến nhiệt độ"] }, 
    "ESP32-C5": { "name": "ESP32-C5", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [240], "sram_kb": 400, "rom_kb": 384, "wifi": "Wi-Fi 6 (802.11ax) Dual Band", "bluetooth": "Bluetooth 5 (LE)", "features": ["Dual Band 2.4/5GHz Wi-Fi"] }, 
    "ESP32-C6": { "name": "ESP32-C6", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [160], "sram_kb": 512, "rom_kb": 320, "wifi": "Wi-Fi 6 (802.11ax)", "bluetooth": "Bluetooth 5.3 (LE)", "features": ["802.15.4 (Thread/Zigbee)", "Low-power LP Core"] }, 
    "ESP32-C61": { "name": "ESP32-C61", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [160], "sram_kb": 272, "rom_kb": 0, "wifi": "Wi-Fi 6 (802.11ax)", "bluetooth": "Bluetooth 5.3 (LE)", "features": ["Cost-Effective Wi-Fi 6"] }, 
    "ESP32-H2": { "name": "ESP32-H2", "architecture": "RISC-V 32-bit single-core", "cores": 1, "cpu_freq_mhz": [96], "sram_kb": 320, "rom_kb": 128, "wifi": None, "bluetooth": "Bluetooth 5.3 (LE)", "features": ["802.15.4 (Thread/Zigbee)", "Low Power Consumption"] }, 
    "ESP32-P4": { "name": "ESP32-P4", "architecture": "RISC-V dual-core 32-bit", "cores": 2, "cpu_freq_mhz": [400], "sram_kb": 768, "rom_kb": 0, "wifi": None, "bluetooth": None, "features": ["High Performance", "MIPI-DSI/CSI", "H.264 Encoder", "2D Graphics Acceleration"] }, 
    "ESP8266": { "name": "ESP8266EX", "architecture": "Xtensa® single-core 32-bit L106", "cores": 1, "cpu_freq_mhz": [80, 160], "sram_kb": 96, "rom_kb": 64, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": None, "features": ["ADC"] }, 
    "ESP8285": { "name": "ESP8285", "architecture": "Xtensa® single-core 32-bit L106", "cores": 1, "cpu_freq_mhz": [80, 160], "sram_kb": 96, "rom_kb": 64, "wifi": "Wi-Fi 4 (802.11 b/g/n)", "bluetooth": None, "features": ["Flash 1MB tích hợp", "ADC"] }
}
ESPRESSIF_OUIS = [ "CC:50:E3", "80:B5:4E", "8C:D0:B2", "94:A9:90", "E0:8C", "80:B5:E4", "7C:9E:BD", "A0:20:A6", "A4:7B:9D", "BC:DD:C2", "24:0A:C4", "24:B2:DE", "30:AE:A4", "60:01:94", "D8:A0:1D", "DC:4F:22" ]

def run_esptool_command(args):
    with esptool_lock:
        original_argv = sys.argv
        try:
            sys.argv = ['esptool.py'] + args
            output_capture = io.StringIO()
            with redirect_stdout(output_capture), redirect_stderr(output_capture): 
                esptool.main()
            res = output_capture.getvalue()
            output_capture.close()
            return res
        except SystemExit as e:
            res = output_capture.getvalue() + f"\n[Esptool exited with code: {e.code}]"
            output_capture.close()
            return res
        except Exception as e: 
            raise e
        finally: 
            sys.argv = original_argv

def list_com_ports_local():
    try: return [p.device for p in list_ports.comports()]
    except Exception: return []

def parse_esptool_output(output_dict):
    full_output = "\n".join(output_dict.values())
    info = {'raw': full_output}
    
    m = re.search(r"Detecting chip type...\s*([^\r\n]+)", full_output, re.IGNORECASE)
    if m: info['chip_name_key'] = m.group(1).strip()
    
    m = re.search(r"(?:Chip is|Chip type:)\s*([^\r\n]+)", full_output, re.IGNORECASE)
    if m: info['chip_line'] = m.group(1).strip()
    
    m = re.search(r"Features:\s*([^\r\n]+)", full_output, re.IGNORECASE)
    if m: info['features_line'] = m.group(1).strip()
    
    m = re.search(r"(?:Crystal is|Crystal frequency:)\s*([^\r\n]+)", full_output, re.IGNORECASE)
    if m: info['crystal'] = m.group(1).strip()
    
    if info.get('chip_line'):
        m = re.search(r"revision\s*v?([0-9.]+)", info['chip_line'], re.IGNORECASE)
        if m: info['revision'] = m.group(1)
        
    m = re.search(r"MAC\s*[:=]\s*([0-9A-Fa-f:.]{12,17})", full_output)
    if m: info['mac'] = "".join(re.findall("[0-9A-Fa-f]", m.group(1).upper()))
    
    m = re.search(r"Manufacturer:\s*([0-9a-fA-F_xX]+)", full_output)
    if m: info["flash_manufacturer"] = m.group(1)
    
    m = re.search(r"Device:\s*([0-9a-fA-F_xX]+)", full_output)
    if m: info["flash_device"] = m.group(1)
    
    m = re.search(r"Detected flash size:\s*([0-9.]+)MB", full_output, re.IGNORECASE)
    if m: info['flash_mb'] = float(m.group(1))
    
    m = re.search(r"Status value:\s*([^\r\n]+)", full_output, re.IGNORECASE)
    if m: info['status_value'] = m.group(1).strip()
    
    m = re.search(r"(?:PSRAM|Embedded PSRAM)\s+(\d+)MB", info.get('features_line', ''), re.IGNORECASE)
    if m: info['psram_mb'] = int(m.group(1))
    
    return info

def derive_mac_addresses(base_mac_hex):
    if not base_mac_hex or len(base_mac_hex) != 12: return {}
    try:
        b = bytes.fromhex(base_mac_hex)
        def fmt(offset):
            n = (int.from_bytes(b, 'big') + offset).to_bytes(6, 'big')
            return ":".join(f"{x:02X}" for x in n)
        return {"MAC Wi-Fi Station": fmt(0), "MAC Wi-Fi AP": fmt(1), "MAC Bluetooth": fmt(2)}
    except Exception: return {}

def get_chip_data(chip_name_key):
    if not chip_name_key: return None
    sorted_keys = sorted(CHIP_DATABASE.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in chip_name_key: return CHIP_DATABASE[key]
    return None

class InspectorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Tool điều kiển MCU ESP v1.0")
        self.root.geometry(DEFAULT_GEOMETRY)
        
        self.stop_scan = threading.Event()
        self.is_detecting = threading.Event()
        self.stop_serial_thread = threading.Event()
        
        self.serial_connection = None
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
        self.is_new_line = True
        self.feedback_buffer = "" 
        
        self.scanned_networks = []
        self.is_wifi_connected = False 
        self.is_server_connected = False 
        self.current_status_level = "info"
        
        # Biến cấu hình mặc định
        self.is_dark_mode = False
        self.saved_wifi_credentials = {}
        self.saved_geometry = DEFAULT_GEOMETRY
        self.saved_is_zoomed = False
        
        # Thêm các biến cấu hình có thể chỉnh sửa, load từ CONTROL_CONFIG
        self.default_ip = "192.168.4.1"
        self.default_port = "100"
        self.control_labels = {i: CONTROL_CONFIG.get(i, (f"F{i}", f"CMD_{i}"))[0] for i in range(1, 13)}
        self.control_commands = {i: CONTROL_CONFIG.get(i, (f"F{i}", f"CMD_{i}"))[1] for i in range(1, 13)}
        
        # Load cấu hình từ ổ cứng
        self.load_config()
        
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        self.img_unchecked = tk.PhotoImage(width=20, height=16)
        self.img_checked = tk.PhotoImage(width=20, height=16)
        self.img_disabled_unchecked = tk.PhotoImage(width=20, height=16)
        self.img_disabled_checked = tk.PhotoImage(width=20, height=16)

        try:
            self.style.element_create("custom.indicator", "image", self.img_unchecked, 
                                      ("disabled", "selected", self.img_disabled_checked), 
                                      ("disabled", self.img_disabled_unchecked), 
                                      ("pressed", "selected", self.img_checked), 
                                      ("active", "selected", self.img_checked), 
                                      ("selected", self.img_checked), 
                                      ("active", self.img_unchecked))
            
            self.style.layout("TCheckbutton", [('Checkbutton.padding', {'sticky': 'nswe', 'children': [
                ('custom.indicator', {'side': 'left', 'sticky': ''}), 
                ('Checkbutton.focus', {'side': 'left', 'sticky': 'w', 'children': [
                    ('Checkbutton.label', {'sticky': 'nswe'})
                ]})
            ]})])
        except tk.TclError: 
            pass 
        
        self.f_buttons = []
        self.create_widgets()
        self.apply_theme()
        
        # Đảm bảo phục hồi chính xác vị trí cửa sổ sau khi UI đã khởi tạo xong để không bị Windows ghi đè
        if self.saved_geometry:
            self.root.after(10, lambda: self.root.geometry(self.saved_geometry))
        # Khôi phục trạng thái toàn màn hình nếu lần trước đã phóng to
        if self.saved_is_zoomed:
            self.root.after(20, lambda: self.root.state('zoomed'))

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.saved_wifi_credentials = config.get("wifi_credentials", {})
                    self.is_dark_mode = config.get("is_dark_mode", False)
                    geom = config.get("geometry")
                    if geom:
                        self.saved_geometry = geom
                    self.saved_is_zoomed = config.get("is_zoomed", False)
                    
                    self.default_ip = config.get("default_ip", "192.168.4.1")
                    self.default_port = config.get("default_port", "100")
                    
                    loaded_labels = config.get("control_labels", {})
                    for k, v in loaded_labels.items():
                        if str(k).isdigit():
                            self.control_labels[int(k)] = v
                            
                    loaded_cmds = config.get("control_commands", {})
                    for k, v in loaded_cmds.items():
                        if str(k).isdigit():
                            self.control_commands[int(k)] = v
                            
            except Exception as e:
                print(f"Lỗi đọc file cấu hình: {e}")

    def save_config(self):
        geom = self.root.geometry()
        is_zoomed = False
        try:
            is_zoomed = self.root.state() == 'zoomed'
        except Exception:
            pass

        # Nếu đang mở toàn màn hình, ép tọa độ về +0+0 để khi mở lại không bị lệch viền (-8px của Windows)
        if is_zoomed:
            match = re.match(r"(\d+x\d+)", geom)
            if match:
                geom = f"{match.group(1)}+0+0"

        config = {
            "wifi_credentials": self.saved_wifi_credentials,
            "is_dark_mode": self.is_dark_mode,
            "geometry": geom,
            "is_zoomed": is_zoomed,
            "default_ip": self.default_ip,
            "default_port": self.default_port,
            "control_labels": self.control_labels,
            "control_commands": self.control_commands
        }
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            print(f"Lỗi lưu file cấu hình: {e}")

    def reset_config(self):
        if messagebox.askyesno("Xác nhận", "Bạn có chắc chắn muốn khôi phục cài đặt gốc?\n(Mật khẩu WiFi đã lưu, cấu hình và màu nền sẽ bị xóa)"):
            if os.path.exists(CONFIG_FILE):
                try:
                    os.remove(CONFIG_FILE)
                except: pass
            self.saved_wifi_credentials = {}
            self.is_dark_mode = False
            self.saved_geometry = DEFAULT_GEOMETRY
            self.saved_is_zoomed = False
            
            self.default_ip = "192.168.4.1"
            self.default_port = "100"
            self.control_labels = {i: CONTROL_CONFIG.get(i, (f"F{i}", f"CMD_{i}"))[0] for i in range(1, 13)}
            self.control_commands = {i: CONTROL_CONFIG.get(i, (f"F{i}", f"CMD_{i}"))[1] for i in range(1, 13)}
            
            self.root.state('normal')
            self.root.geometry(DEFAULT_GEOMETRY)
            self.apply_theme()
            
            self.wifi_pass_entry.delete(0, tk.END)
            self.wifi_cb.set('')
            self.wifi_cb['values'] = []
            
            self.ip_entry.delete(0, tk.END)
            self.ip_entry.insert(0, self.default_ip)
            self.port_entry.delete(0, tk.END)
            self.port_entry.insert(0, self.default_port)
            
            for i in range(1, 13):
                if i - 1 < len(self.f_buttons):
                    self.f_buttons[i-1].config(text=self.control_labels[i])
                    
            messagebox.showinfo("Thành công", "Đã khôi phục cài đặt mặc định.")
            return True
        return False

    def create_widgets(self):
        self.main_frame = ttk.Frame(self.root, padding=5)
        self.main_frame.grid(sticky="nsew", row=0, column=0)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(1, weight=1)
        
        self.controls_frame = ttk.LabelFrame(self.main_frame, text=" Cài đặt chung & Điều khiển Serial ", padding=(5, 5))
        self.controls_frame.grid(row=0, column=0, sticky="ew", padx=2, pady=2)
        
        ttk.Label(self.controls_frame, text="COM Port:").grid(row=0, column=0, sticky="w", pady=2, padx=(0, 5))
        self.port_cb = ttk.Combobox(self.controls_frame, values=[], width=12, state="readonly")
        self.port_cb.grid(row=0, column=1, sticky="w")
        
        self.var_auto_scan = tk.BooleanVar(value=True)
        ttk.Checkbutton(self.controls_frame, text="Tự động quét", variable=self.var_auto_scan).grid(row=0, column=2, padx=(5, 10))
        
        ttk.Label(self.controls_frame, text="Baudrate:").grid(row=0, column=3, sticky="w", padx=(0, 5))
        self.baud_cb = ttk.Combobox(self.controls_frame, values=COMMON_BAUDS, width=10)
        self.baud_cb.set("Tự động") 
        self.baud_cb.grid(row=0, column=4, sticky="w")
        
        separator = ttk.Separator(self.controls_frame, orient='vertical')
        separator.grid(row=0, column=5, sticky='ns', padx=10, pady=2)
        
        self.serial_toggle_btn = ttk.Button(self.controls_frame, text="Kết nối", command=self.toggle_serial_connection)
        self.serial_toggle_btn.grid(row=0, column=6, padx=(0,5))
        
        self.clear_monitor_btn = ttk.Button(self.controls_frame, text="Xóa màn hình", command=lambda: self.on_clear(self.monitor_txt))
        self.clear_monitor_btn.grid(row=0, column=7, padx=5)
        
        self.var_autoscroll = tk.BooleanVar(value=Tu_dong_cuon)
        ttk.Checkbutton(self.controls_frame, text="Tự động cuộn", variable=self.var_autoscroll).grid(row=0, column=8, padx=(10,0))
        
        self.var_timestamp = tk.BooleanVar(value=Them_dau_thoi_gian)
        ttk.Checkbutton(self.controls_frame, text="Thêm Dấu thời gian", variable=self.var_timestamp).grid(row=0, column=9, padx=(5, 15))
        
        self.settings_btn = ttk.Button(self.controls_frame, text="Cài đặt", command=self.open_settings_dialog)
        self.settings_btn.grid(row=0, column=10, sticky="e")
        self.controls_frame.columnconfigure(10, weight=1) 
        
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.grid(row=1, column=0, sticky="nsew", pady=5)
        self.inspector_tab = ttk.Frame(self.notebook)
        self.monitor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.inspector_tab, text=' Kiểm tra Phần cứng ')
        self.notebook.add(self.monitor_tab, text=' Serial Monitor & Điều khiển ')
        
        # Đặt tab "Serial Monitor" làm mặc định lúc khởi động
        self.notebook.select(self.monitor_tab)
        
        self.inspector_tab.columnconfigure(0, weight=1)
        self.inspector_tab.rowconfigure(1, weight=1)
        
        self.create_inspector_tab()
        self.create_monitor_tab()
        self._set_mcu_dependent_ui_state("disabled")
    
    def create_inspector_tab(self):
        buttons_frame = ttk.LabelFrame(self.inspector_tab, text=" Tác vụ ", padding=(10, 5))
        buttons_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        
        self.detect_btn = ttk.Button(buttons_frame, text="KIỂM TRA TOÀN DIỆN", command=self.on_detect, style="Accent.TButton")
        self.detect_btn.pack(side="left", padx=(0,5))
        
        ttk.Button(buttons_frame, text="Lưu Báo Cáo", command=self.on_save).pack(side="left", padx=5)
        
        self.clear_inspector_btn = ttk.Button(buttons_frame, text="Xóa", command=lambda: self.on_clear(self.inspector_txt))
        self.clear_inspector_btn.pack(side="left", padx=5)
        
        self.var_debug = tk.BooleanVar(value=False)
        ttk.Checkbutton(buttons_frame, text="Hiển thị Output Thô", variable=self.var_debug).pack(side="left", padx=(15,0))
        
        report_frame = ttk.Frame(self.inspector_tab)
        report_frame.grid(row=1, column=0, sticky="nsew")
        report_frame.columnconfigure(0, weight=1)
        report_frame.rowconfigure(0, weight=1)
        
        self.inspector_txt = tk.Text(report_frame, width=100, wrap="word", font=("Consolas", REPORT_FONT_SIZE), relief="flat", borderwidth=1, state="disabled")
        self.inspector_txt.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        vsb = ttk.Scrollbar(report_frame, orient="vertical", command=self.inspector_txt.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.inspector_txt.configure(yscrollcommand=vsb.set)
        self.inspector_txt.tag_configure('header', font=("Consolas", REPORT_FONT_SIZE, 'bold'))
        
    def create_monitor_tab(self):
        self.monitor_tab.rowconfigure(0, weight=1)
        self.monitor_tab.columnconfigure(0, weight=1)
        
        paned_window = ttk.PanedWindow(self.monitor_tab, orient=tk.HORIZONTAL)
        paned_window.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        
        serial_pane = ttk.Frame(paned_window)
        # Sửa thành tỷ lệ 5:1 (Nghĩa là Khung Serial lớn gấp 5 lần Khung Điều khiển)
        paned_window.add(serial_pane, weight=5) 
        serial_pane.columnconfigure(0, weight=1)
        serial_pane.rowconfigure(0, weight=1) 
        
        monitor_output_frame = ttk.Frame(serial_pane)
        monitor_output_frame.grid(row=0, column=0, sticky="nsew", pady=2)
        monitor_output_frame.columnconfigure(0, weight=1)
        monitor_output_frame.rowconfigure(0, weight=1)
        
        self.monitor_txt = tk.Text(monitor_output_frame, width=70, wrap="word", font=("Consolas", REPORT_FONT_SIZE), relief="flat", borderwidth=1, state="disabled")
        self.monitor_txt.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        vsb = ttk.Scrollbar(monitor_output_frame, orient="vertical", command=self.monitor_txt.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.monitor_txt.configure(yscrollcommand=vsb.set)
        
        self.monitor_txt.tag_configure('server_rx', foreground='#008000')

        net_ctrl_pane = ttk.Frame(paned_window)
        paned_window.add(net_ctrl_pane, weight=1) 
        net_ctrl_pane.columnconfigure(0, weight=1)
        
        wifi_frame = ttk.LabelFrame(net_ctrl_pane, text=" Cấu hình Mạng ", padding=(5, 5))
        wifi_frame.grid(row=0, column=0, sticky="new", padx=(5,0), pady=(0, 2))
        
        # [UI FIX 4] Ép cột 0 không được giãn (weight=0) để nhãn ôm sát chữ, rút ngắn khoảng cách với ô nhập
        wifi_frame.columnconfigure(0, weight=0) 
        wifi_frame.columnconfigure(1, weight=1)
        wifi_frame.columnconfigure(2, weight=0)
        
        # --- 1. WiFi ---
        ttk.Label(wifi_frame, text="WiFi:").grid(row=0, column=0, sticky="w", padx=(0,5), pady=2)
        self.wifi_cb = ttk.Combobox(wifi_frame, values=[])
        # Cho Combobox mở rộng qua cả cột 2 để bằng chiều dài cạnh phải của các ô bên dưới
        self.wifi_cb.grid(row=0, column=1, columnspan=2, sticky="ew", pady=2)
        
        self.wifi_cb.bind("<<ComboboxSelected>>", self.on_wifi_selected)
        self.wifi_cb.bind("<FocusOut>", self.on_wifi_selected)
        
        # --- 2. Mật khẩu ---
        ttk.Label(wifi_frame, text="Mật khẩu:").grid(row=1, column=0, sticky="w", padx=(0,5), pady=2)
        self.wifi_pass_entry = ttk.Entry(wifi_frame, show="*")
        self.wifi_pass_entry.grid(row=1, column=1, sticky="ew", pady=2)
        
        self.var_show_pwd = tk.BooleanVar(value=False)
        self.cb_show_pwd = ttk.Checkbutton(wifi_frame, text="Hiện", variable=self.var_show_pwd, command=self.toggle_pwd_visibility)
        self.cb_show_pwd.grid(row=1, column=2, padx=(5, 0), sticky="w")
        
        # --- 3. IP Server ---
        ttk.Label(wifi_frame, text="IP Server:").grid(row=2, column=0, sticky="w", padx=(0,5), pady=2)
        self.ip_entry = ttk.Entry(wifi_frame)
        self.ip_entry.insert(0, self.default_ip) 
        self.ip_entry.grid(row=2, column=1, sticky="ew", pady=2)
        
        self.var_auto_connect_server = tk.BooleanVar(value=True)
        self.cb_auto_server = ttk.Checkbutton(wifi_frame, text="auto_connect", variable=self.var_auto_connect_server)
        self.cb_auto_server.grid(row=2, column=2, padx=(5, 0), sticky="w")
        
        # --- 4. Cổng ---
        ttk.Label(wifi_frame, text="Cổng:").grid(row=3, column=0, sticky="w", padx=(0,5), pady=2)
        self.port_entry = ttk.Entry(wifi_frame)
        self.port_entry.insert(0, self.default_port) 
        # Cổng chiếm 2 cột để dài bằng cạnh phải ô Wifi
        self.port_entry.grid(row=3, column=1, columnspan=2, sticky="ew", pady=2)
        
        # --- 5. Nút bấm Mạng ---
        # Gộp tất cả 3 nút bấm vào cùng 1 hàng ngang
        buttons_wifi_frame = ttk.Frame(wifi_frame)
        buttons_wifi_frame.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(10, 5))
        
        self.btn_scan_wifi = ttk.Button(buttons_wifi_frame, text="Quét WiFi", command=self.on_wifi_scan, width=12)
        self.btn_scan_wifi.grid(row=0, column=0, padx=(0, 5), sticky="w")
        
        self.btn_connect_wifi = ttk.Button(buttons_wifi_frame, text="Kết nối WiFi", command=self.toggle_wifi_connection, width=12)
        self.btn_connect_wifi.grid(row=0, column=1, padx=(0, 5), sticky="w")
        
        self.btn_connect_server = ttk.Button(buttons_wifi_frame, text="Kết nối Server", command=self.toggle_server_connection, width=14)
        self.btn_connect_server.grid(row=0, column=2, padx=(0, 5), sticky="w")
        
        # --- 6. Trạng thái kết nối ---
        self.wifi_status_lbl = tk.Label(wifi_frame, text="Chưa kết nối")
        self.wifi_status_lbl.grid(row=5, column=0, columnspan=3, sticky="w", padx=(0,5), pady=(2,0))
        
        ctrl_frame = ttk.LabelFrame(net_ctrl_pane, text=" Bảng Điều khiển ", padding=(5, 5))
        ctrl_frame.grid(row=1, column=0, sticky="nsew", padx=(5,0), pady=(2, 0))
        ctrl_frame.columnconfigure(0, weight=1)
        ctrl_frame.columnconfigure(1, weight=1)
        ctrl_frame.columnconfigure(2, weight=1) 
        
        self.f_buttons = [] 
        for i in range(1, 13): 
            btn_text = self.control_labels.get(i, f"F{i}")
            btn = ttk.Button(ctrl_frame, text=btn_text, command=lambda x=i: self.on_control_click(x))
            btn.grid(row=(i - 1) // 3, column=(i - 1) % 3, sticky="ew", padx=2, pady=2, ipady=1)
            self.f_buttons.append(btn) 

        input_frame = ttk.LabelFrame(net_ctrl_pane, text=" Gửi dữ liệu Serial ", padding=(5,5))
        input_frame.grid(row=2, column=0, sticky="ew", padx=(5,0), pady=(10, 0)) 
        input_frame.columnconfigure(0, weight=1)
        
        # Thêm width=15 để gỡ bỏ chiều rộng 80 ký tự mặc định, trả lại tỷ lệ 1/5 cho cửa sổ
        self.serial_input = tk.Text(input_frame, height=5, width=15, font=("Consolas", REPORT_FONT_SIZE), wrap="word") 
        self.serial_input.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=0, pady=2)
        self.serial_input.bind("<Return>", self.on_serial_send)
        input_frame.rowconfigure(0, weight=1)
        
        self.var_crlf = tk.BooleanVar(value=True)
        self.cb_crlf = ttk.Checkbutton(input_frame, text="Thêm \\n\\r", variable=self.var_crlf)
        self.cb_crlf.grid(row=1, column=0, sticky="w", pady=(2,0))

        self.send_btn = ttk.Button(input_frame, text="Gửi", command=self.on_serial_send)
        self.send_btn.grid(row=1, column=1, sticky="e", pady=(2,0))

        self.req_status_lbl = tk.Label(net_ctrl_pane, text="Sẵn sàng.", font=("Segoe UI", 9, "italic"))
        self.req_status_lbl.grid(row=3, column=0, sticky="w", padx=(5,0), pady=(5,0))

    def open_settings_dialog(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Cài đặt hệ thống")
        
        # --- [USER UPDATE 7] Tính toán vị trí để mở ngay giữa màn hình chính ---
        dialog.update_idletasks() # Cập nhật trạng thái widget
        width = 750
        height = 550
        x = max(0, self.root.winfo_x() + (self.root.winfo_width() // 2) - (width // 2))
        y = max(0, self.root.winfo_y() + (self.root.winfo_height() // 2) - (height // 2))
        dialog.geometry(f"{width}x{height}+{x}+{y}")
        
        dialog.minsize(700, 500)
        dialog.transient(self.root)
        dialog.grab_set()

        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        dialog.configure(bg=p["bg"])

        # --- Frame Giao diện ---
        theme_frame = ttk.LabelFrame(dialog, text=" Giao diện ", padding=10)
        theme_frame.pack(fill="x", padx=10, pady=5)
        
        def toggle_theme_from_dialog():
            self.toggle_theme()
            dialog_p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
            dialog.configure(bg=dialog_p["bg"])
            dialog_theme_btn.config(text="Giao diện Sáng" if self.is_dark_mode else "Giao diện Tối")
            
        dialog_theme_btn = ttk.Button(theme_frame, text="Giao diện Sáng" if self.is_dark_mode else "Giao diện Tối", command=toggle_theme_from_dialog)
        dialog_theme_btn.pack(anchor="w")

        # --- Frame Mạng mặc định ---
        net_frame = ttk.LabelFrame(dialog, text=" Mạng mặc định ", padding=10)
        net_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(net_frame, text="IP Server:").grid(row=0, column=0, sticky="w", padx=(0,5), pady=2)
        ip_var = tk.StringVar(value=self.default_ip)
        ttk.Entry(net_frame, textvariable=ip_var).grid(row=0, column=1, sticky="ew", pady=2)
        
        ttk.Label(net_frame, text="Cổng:").grid(row=0, column=2, sticky="w", padx=(15,5), pady=2)
        port_var = tk.StringVar(value=self.default_port)
        ttk.Entry(net_frame, textvariable=port_var, width=8).grid(row=0, column=3, sticky="ew", pady=2)

        # --- Frame Cấu hình Nút chức năng ---
        btn_frame = ttk.LabelFrame(dialog, text=" Cấu hình Nút chức năng (F1-F12) ", padding=10)
        btn_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        btn_label_vars = {}
        btn_cmd_vars = {}
        
        for i in range(1, 13):
            row = (i - 1) % 6
            col = (i - 1) // 6 * 4
            
            ttk.Label(btn_frame, text=f"F{i}:").grid(row=row, column=col, sticky="e", padx=(15 if col > 0 else 0, 5), pady=5)
            
            l_var = tk.StringVar(value=self.control_labels.get(i, f"F{i}"))
            btn_label_vars[i] = l_var
            ttk.Entry(btn_frame, textvariable=l_var, width=12).grid(row=row, column=col+1, sticky="w", pady=5)
            
            ttk.Label(btn_frame, text="Lệnh:").grid(row=row, column=col+2, sticky="e", padx=(10, 5), pady=5)
            
            c_var = tk.StringVar(value=self.control_commands.get(i, f"CMD_{i}"))
            btn_cmd_vars[i] = c_var
            ttk.Entry(btn_frame, textvariable=c_var, width=18).grid(row=row, column=col+3, sticky="w", pady=5)

        # --- Frame Nút Lưu/Hủy ---
        action_frame = ttk.Frame(dialog)
        action_frame.pack(fill="x", padx=10, pady=10)
        
        # [USER UPDATE 7] Chuyển nút Khôi phục gốc vào đây
        def do_reset_from_dialog():
            if self.reset_config():
                dialog.destroy()
                
        ttk.Button(action_frame, text="Khôi phục gốc", command=do_reset_from_dialog).pack(side="left")
        
        def save_and_close():
            self.default_ip = ip_var.get().strip()
            self.default_port = port_var.get().strip()
            
            # Cập nhật ô nhập liệu Mạng trên giao diện chính
            self.ip_entry.delete(0, tk.END)
            self.ip_entry.insert(0, self.default_ip)
            self.port_entry.delete(0, tk.END)
            self.port_entry.insert(0, self.default_port)

            # Cập nhật nhãn tên và lệnh cho Bảng điều khiển
            for i in range(1, 13):
                self.control_labels[i] = btn_label_vars[i].get().strip()
                self.control_commands[i] = btn_cmd_vars[i].get().strip()
                self.f_buttons[i-1].config(text=self.control_labels[i])
            
            self.save_config()
            dialog.destroy()

        ttk.Button(action_frame, text="Hủy", command=dialog.destroy).pack(side="right", padx=(5,0))
        ttk.Button(action_frame, text="Lưu Cài đặt", command=save_and_close, style="Accent.TButton").pack(side="right")

    def on_wifi_selected(self, event=None):
        ssid = self.wifi_cb.get().strip()
        if ssid and ssid in self.saved_wifi_credentials:
            self.wifi_pass_entry.delete(0, tk.END)
            self.wifi_pass_entry.insert(0, self.saved_wifi_credentials[ssid])
            # Ép kiểm tra lại ẩn/hiện mật khẩu sau khi tự động điền
            self.toggle_pwd_visibility()

    def toggle_pwd_visibility(self):
        if self.var_show_pwd.get():
            self.wifi_pass_entry.config(show="")
        else:
            self.wifi_pass_entry.config(show="*")

    def _set_mcu_dependent_ui_state(self, ui_state):
        self.btn_scan_wifi.config(state=ui_state)
        self.btn_connect_wifi.config(state=ui_state)
        self.btn_connect_server.config(state=ui_state)
        self.wifi_cb.config(state=ui_state)
        self.wifi_pass_entry.config(state=ui_state)
        self.ip_entry.config(state=ui_state)
        self.port_entry.config(state=ui_state)
        self.cb_auto_server.config(state=ui_state)
        
        for btn in self.f_buttons: 
            btn.config(state=ui_state)
            
        self.serial_input.config(state=ui_state)
        self.send_btn.config(state=ui_state)
        self.cb_crlf.config(state=ui_state)
        self.cb_show_pwd.config(state=ui_state)

    def _draw_checkbox_image(self, img, border_color, bg_color, parent_bg, check_color=None):
        width = 20; height = 16; box_size = 14; offset_x = 1; offset_y = 1
        row_data = []
        for y in range(height):
            row = []
            for x in range(width):
                if offset_x <= x < offset_x + box_size and offset_y <= y < offset_y + box_size:
                    if x == offset_x or x == offset_x + box_size - 1 or y == offset_y or y == offset_y + box_size - 1: 
                        row.append(border_color)
                    else: 
                        row.append(bg_color)
                else: 
                    row.append(parent_bg)
            row_data.append("{" + " ".join(row) + "}")
        img.put(" ".join(row_data))
        if check_color:
            check_pts = [(4,8), (5,9), (6,10), (7,11), (8,10), (9,9), (10,8), (11,7), (12,6), 
                         (4,9), (5,10), (6,11), (7,12), (8,11), (9,10), (10,9), (11,8), (12,7)]
            for x, y in check_pts: 
                img.put(check_color, (x, y))

    def toggle_theme(self):
        self.is_dark_mode = not self.is_dark_mode
        self.apply_theme()

    def apply_theme(self):
        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        
        self._draw_checkbox_image(self.img_unchecked, p["border"], p["text_bg"], p["bg"])
        self._draw_checkbox_image(self.img_checked, p["accent"], p["accent"], p["bg"], "#ffffff") 
        self._draw_checkbox_image(self.img_disabled_unchecked, p["border"], p["disabled_bg"], p["bg"])
        self._draw_checkbox_image(self.img_disabled_checked, p["border"], p["disabled_bg"], p["bg"], p["disabled_fg"])

        self.root.configure(bg=p["bg"])
        
        self.inspector_txt.configure(bg=p["text_bg"], fg=p["text_fg"], insertbackground=p["fg"], selectbackground=p["accent"], selectforeground="#ffffff")
        self.monitor_txt.configure(bg=p["text_bg"], fg=p["text_bg"], insertbackground=p["fg"], selectbackground=p["accent"], selectforeground="#ffffff")
        self.monitor_txt.configure(fg=p["text_fg"])
        
        if hasattr(self, 'serial_input') and isinstance(self.serial_input, tk.Text):
            self.serial_input.configure(bg=p["text_bg"], fg=p["text_fg"], insertbackground=p["text_fg"], selectbackground=p["accent"], selectforeground="#ffffff")
            
        self.inspector_txt.tag_configure('green', foreground=p["success"])
        self.inspector_txt.tag_configure('blue', foreground=p["accent"])
        self.inspector_txt.tag_configure('red', foreground=p["error"])
        
        self.monitor_txt.tag_configure('server_rx', foreground="#00fa9a" if self.is_dark_mode else "#008000")
        
        self.wifi_status_lbl.configure(bg=p["bg"], fg=p["success"] if self.is_wifi_connected else p["error"])
        
        color_map = {
            "info": p["disabled_fg"], "success": p["success"],
            "error": p["error"], "warning": p["warning"]
        }
        self.req_status_lbl.configure(bg=p["bg"], fg=color_map.get(self.current_status_level, p["disabled_fg"]))

        self.style.theme_use('clam') 
        self.style.configure(".", background=p["bg"], foreground=p["fg"], fieldbackground=p["text_bg"])
        
        self.style.configure("TButton", background=p["btn_bg"], foreground=p["fg"], borderwidth=1, bordercolor=p["border"], lightcolor=p["btn_bg"], darkcolor=p["btn_bg"])
        self.style.map("TButton", 
                       background=[('disabled', p["disabled_bg"]), ('active', p["btn_hover"]), ('pressed', p["btn_pressed"])], 
                       foreground=[('disabled', p["disabled_fg"])], 
                       bordercolor=[('disabled', p["border"])], 
                       lightcolor=[('disabled', p["disabled_bg"]), ('active', p["btn_hover"]), ('pressed', p["btn_pressed"])], 
                       darkcolor=[('disabled', p["disabled_bg"]), ('active', p["btn_hover"]), ('pressed', p["btn_pressed"])])
        
        self.style.configure("Accent.TButton", foreground=p["accent"], font=('Segoe UI', 10, 'bold'))
        
        # Thêm thuộc tính insertcolor để sửa màu con trỏ chuột cho ttk.Entry
        self.style.configure("TEntry", fieldbackground=p["text_bg"], foreground=p["text_fg"], borderwidth=1, bordercolor=p["border"], lightcolor=p["text_bg"], darkcolor=p["text_bg"], insertcolor=p["text_fg"])
        self.style.map("TEntry", 
                       fieldbackground=[('disabled', p["disabled_bg"])], 
                       foreground=[('disabled', p["disabled_fg"])], 
                       lightcolor=[('disabled', p["disabled_bg"])], 
                       darkcolor=[('disabled', p["disabled_bg"])])
        
        # Thêm thuộc tính insertcolor để sửa màu con trỏ chuột cho ttk.Combobox
        self.style.configure("TCombobox", fieldbackground=p["text_bg"], foreground=p["text_fg"], borderwidth=1, bordercolor=p["border"], lightcolor=p["text_bg"], darkcolor=p["text_bg"], arrowcolor=p["fg"], insertcolor=p["text_fg"])
        self.style.map("TCombobox", 
                       fieldbackground=[('disabled', p["disabled_bg"])], 
                       foreground=[('disabled', p["disabled_fg"])], 
                       lightcolor=[('disabled', p["disabled_bg"])], 
                       darkcolor=[('disabled', p["disabled_bg"])])
                       
        # [UI FIX 5] Can thiệp đồng bộ màu cho menu danh sách (Listbox) xổ xuống của Combobox
        self.root.option_add('*TCombobox*Listbox.background', p["text_bg"])
        self.root.option_add('*TCombobox*Listbox.foreground', p["text_fg"])
        self.root.option_add('*TCombobox*Listbox.selectBackground', p["accent"])
        self.root.option_add('*TCombobox*Listbox.selectForeground', '#ffffff')
        
        self.style.configure("TCheckbutton", background=p["bg"], foreground=p["fg"], focuscolor=p["bg"])
        self.style.map("TCheckbutton", background=[('active', p["bg"])], foreground=[('disabled', p["disabled_fg"])])
        
        self.style.configure("TLabel", background=p["bg"], foreground=p["fg"])
        self.style.configure("TFrame", background=p["bg"])
        
        self.style.configure("TLabelframe", background=p["bg"], foreground=p["fg"], borderwidth=1, bordercolor=p["border"], lightcolor=p["bg"], darkcolor=p["bg"])
        self.style.configure("TLabelframe.Label", background=p["bg"], foreground=p["fg"])
        
        self.style.configure("TNotebook", background=p["bg"], borderwidth=0)
        self.style.configure("TNotebook.Tab", background=p["disabled_bg"], foreground=p["fg"], borderwidth=1, bordercolor=p["border"], lightcolor=p["disabled_bg"])
        self.style.map("TNotebook.Tab", background=[("selected", p["bg"])], lightcolor=[("selected", p["bg"])])
        
        # Đảm bảo mật khẩu không bị hiện khi đổi Theme hoặc lúc khởi động
        if hasattr(self, 'wifi_pass_entry') and self.wifi_pass_entry.winfo_exists():
            self.toggle_pwd_visibility()

    def start_background_tasks(self):
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        threading.Thread(target=self._auto_scan_ports, daemon=True).start()
        self.set_status("Sẵn sàng.", "info")

    def _auto_scan_ports(self):
        while not self.stop_scan.is_set():
            if self.var_auto_scan.get() and not self.is_detecting.is_set():
                try:
                    current_selection = self.port_cb.get()
                    new_ports = list_com_ports_local()
                    self.root.after(0, self.update_port_list, current_selection, new_ports)
                except Exception: 
                    pass
            time.sleep(2)
            
    def update_port_list(self, current_selection, new_ports):
        if self.root.winfo_exists():
            if tuple(new_ports) != self.port_cb['values']:
                self.port_cb['values'] = new_ports
                if current_selection in new_ports: 
                    self.port_cb.set(current_selection)
                elif new_ports: 
                    self.port_cb.set(new_ports[0])
                else: 
                    self.port_cb.set("")
                
    def on_closing(self):
        self.save_config() # Gọi lưu tự động khi đóng ứng dụng
        self.on_serial_disconnect()
        self.stop_scan.set()
        self.root.destroy()
        
    def log(self, s, tag=None): 
        self.inspector_txt.config(state="normal")
        self.inspector_txt.insert("end", s + "\n", tag)
        self.inspector_txt.see("end")
        self.inspector_txt.config(state="disabled")
        
    def on_clear(self, text_widget, clear_status=True):
        text_widget.config(state="normal")
        text_widget.delete("1.0", "end")
        text_widget.config(state="disabled")
        if clear_status: 
            self.set_status("Sẵn sàng.", "info")
        
    def on_save(self):
        content = self.inspector_txt.get("1.0", "end").strip()
        if not content: 
            messagebox.showinfo("Lưu", "Không có nội dung để lưu.")
        else:
            chip_name_match = re.search(r"Loại chip:\s*(\S+)", content)
            chip_name = chip_name_match.group(1) if chip_name_match else "ESP_Device"
            default_filename = f"Report_{chip_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
            fpath = filedialog.asksaveasfilename(initialfile=default_filename, defaultextension=".txt", filetypes=[("Text File","*.txt")])
            if fpath:
                try:
                    with open(fpath, "w", encoding="utf-8") as f: 
                        f.write(content)
                    messagebox.showinfo("Lưu", f"Báo cáo đã được lưu vào {fpath}")
                except Exception as e: 
                    messagebox.showerror("Lỗi Lưu", str(e))
                
    def set_status(self, s, level="info"): 
        self.current_status_level = level
        if hasattr(self, 'req_status_lbl') and self.req_status_lbl.winfo_exists():
            p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
            color_map = {
                "info": p["disabled_fg"], "success": p["success"],
                "error": p["error"], "warning": p["warning"]
            }
            color = color_map.get(level, p["disabled_fg"])
            self.root.after(0, lambda: self.req_status_lbl.config(text=s, fg=color))

    def on_detect(self):
        port = self.port_cb.get().strip()
        if not port: 
            messagebox.showwarning("Thiếu Cổng", "Vui lòng chọn cổng COM.")
            return
        
        if self.serial_connection and self.serial_connection.is_open:
            self.on_serial_disconnect()
            self.root.after(500, lambda: self._start_detect_thread(port))
        else:
            self._start_detect_thread(port)
            
    def _start_detect_thread(self, port):
        self.on_clear(self.inspector_txt, clear_status=False)
        self.set_status(f"Đang kiểm tra trên {port}...", "info")
        self.detect_btn.config(state="disabled")
        self.serial_toggle_btn.config(state="disabled") 
        self.is_detecting.set()
        threading.Thread(target=self._detect_thread, args=(port, self.baud_cb.get()), daemon=True).start()

    def _detect_thread(self, port, baud):
        has_error = False
        try:
            if baud == 'Tự động':
                detected_baud = self.detect_baud_rate(port)
                if not detected_baud: 
                    raise Exception("Không thể tự động dò tìm Baudrate. Vui lòng chọn thủ công.")
                baud = detected_baud
                self.root.after(0, lambda: self.baud_cb.set(baud))
            outputs = {}
            for name in ['chip_id', 'flash_id', 'read_mac']:
                self.set_status(f"Đang chạy {name}...", "info")
                args = ['--port', port, '--baud', baud, name]
                outputs[name] = run_esptool_command(args)
            info = parse_esptool_output(outputs)
            self.root.after(0, self._display_formatted_report, info)
        except Exception as e:
            has_error = True
            error_str = str(e)
            if isinstance(e, serial.SerialException) or "could not open port" in error_str.lower():
                self.root.after(0, lambda: messagebox.showerror("Lỗi Truy Cập Cổng COM", f"Không thể mở cổng '{port}'.\nCổng đang bị chiếm. Vui lòng thử lại."))
            else:
                self.root.after(0, self.log, "ĐÃ XẢY RA LỖI KHÁC:\n" + error_str)
        finally:
            self.root.after(0, lambda: self.set_status("Lỗi kiểm tra." if has_error else "Hoàn thành.", "error" if has_error else "success"))
            self.root.after(0, lambda: self.detect_btn.config(state="normal"))
            self.root.after(0, lambda: self.serial_toggle_btn.config(state="normal"))
            self.is_detecting.clear()

    def _display_formatted_report(self, info):
        self.on_clear(self.inspector_txt, clear_status=False)
        if self.var_debug.get():
            self.log("--- OUTPUT THÔ TỪ ESPTOOL " + "-"*48, 'header')
            self.log(info.get('raw', 'Không có output thô.').strip())
            self.log("-" * 72 + "\n")
            
        chip_data = get_chip_data(info.get('chip_name_key'))
        man_id = info.get('flash_manufacturer', 'N/A')
        man_name = FLASH_MANUFACTURERS.get(man_id.lower(), "Không rõ")
        base_mac_hex = info.get('mac')
        base_mac_formatted = ":".join(base_mac_hex[i:i+2] for i in range(0, 12, 2)) if base_mac_hex else None
        
        report_data = [
            ("Thông tin Động (đọc từ chip)", None, 'header'),
            ("  Tên chip đầy đủ", info.get('chip_line', 'N/A'), None),
            ("  Các tính năng (Features)", info.get('features_line', 'N/A'), None),
            ("  Phiên bản Silicon", info.get('revision', 'N/A'), None),
            ("  Tần số thạch anh", info.get('crystal', 'N/A'), None),
            ("Thông tin Chip Flash & PSRAM", None, 'header'),
            ("  Nhà sản xuất Flash", f"{man_name} ({man_id})", None),
            ("  ID Flash", info.get('flash_device', 'N/A'), None),
            ("  Kích thước Flash", f"{info.get('flash_mb', 0):.2f} MB" if 'flash_mb' in info else "N/A", None),
            ("  Kích thước PSRAM", f"{info['psram_mb']} MB" if 'psram_mb' in info else "Không hỗ trợ", None),
            ("Thông số Kỹ thuật (tra cứu từ CSDL)", None, 'header')
        ]
        
        if chip_data:
            report_data.extend([
                ("  Loại chip", chip_data['name'], None), 
                ("  Kiến trúc CPU", chip_data['architecture'], None),
                ("  Số nhân CPU", chip_data['cores'], None), 
                ("  Tần số CPU hỗ trợ", f"{', '.join(map(str, chip_data['cpu_freq_mhz']))} MHz", None),
                ("  Kích thước SRAM", f"{chip_data['sram_kb']} KB", None), 
                ("  Kích thước ROM", f"{chip_data['rom_kb']} KB", None)
            ])
        else: 
            report_data.append(("  Loại chip", f"{info.get('chip_name_key', 'Unknown')} (Không có trong CSDL)", 'red'))
        
        report_data.append(("Mạng & Kết nối", None, 'header'))
        report_data.extend([
            ("  Chuẩn Wi-Fi", (chip_data.get('wifi') if chip_data else None) or "Không hỗ trợ", None), 
            ("  Chuẩn Bluetooth", (chip_data.get('bluetooth') if chip_data else None) or "Không hỗ trợ", None)
        ])
        
        if base_mac_hex:
            macs = derive_mac_addresses(base_mac_hex)
            if chip_data and chip_data.get('wifi'): 
                report_data.extend([
                    ("    + MAC Wi-Fi Station", macs.get("MAC Wi-Fi Station"), None), 
                    ("    + MAC Wi-Fi AP", macs.get("MAC Wi-Fi AP"), None)
                ])
            if chip_data and chip_data.get('bluetooth'): 
                report_data.append(("    + MAC Bluetooth", macs.get("MAC Bluetooth"), None))
        else: 
            report_data.append(("  Địa MAC", "Không đọc được", 'red'))
        
        if chip_data and chip_data.get('features'):
            report_data.extend([("Tính năng Nổi bật", None, 'header'), ("", ", ".join(chip_data['features']), None)])
        
        report_data.append(("Phân tích Nguồn gốc", None, 'header'))
        if base_mac_formatted:
            oui = ":".join(base_mac_formatted.split(":")[:3])
            is_genuine_oui = any(base_mac_formatted.startswith(o) for o in ESPRESSIF_OUIS)
            if is_genuine_oui:
                report_data.append(("  Chip Xử Lý (SoC)", f"SoC Chính hãng (OUI: {oui} khớp với Espressif)", 'green'))
                report_data.append(("  Board/Module", "Module chính thức từ Espressif (DevKit)" if "espressif" in man_name.lower() else f"Module của bên thứ ba (dùng Flash từ {man_name})", 'green' if "espressif" in man_name.lower() else 'blue'))
            else: 
                report_data.append(("  Chip Xử Lý (SoC)", f"Không xác định (OUI: {oui} không khớp, có nguy cơ là hàng clone)", 'red'))
        else: 
            report_data.append(("  Chip Xử Lý (SoC)", "Không thể xác thực (thiếu MAC)", 'red'))
        
        key_width = max((len(k) for k, v, t in report_data if k and not k.startswith("---") and k.strip() != ""), default=25) + 2
        
        self.inspector_txt.config(state="normal")
        for key, value, tag in report_data:
            if value is None: 
                self.inspector_txt.insert("end", f"\n{key}\n", tag)
            elif key == "": 
                self.inspector_txt.insert("end", f"  {value}\n", tag)
            else: 
                self.inspector_txt.insert("end", f"{key+':':<{key_width}} {value}\n", tag)
        self.inspector_txt.see("end")
        self.inspector_txt.config(state="disabled")
                
    def detect_baud_rate(self, port):
        test_bauds = ["115200", "74880", "230400", "460800", "921600", "9600"]
        self.set_status(f"Đang dò tìm Baudrate...", "info")
        for baud in test_bauds:
            self.set_status(f"Đang thử @ {baud} bps...", "info")
            try:
                args = ['--port', port, '--baud', str(baud), 'chip_id']
                output = run_esptool_command(args)
                if "A fatal error occurred" not in output and ("Chip is" in output or "Chip type:" in output):
                    self.set_status(f"Đã tìm thấy Baudrate: {baud}", "success")
                    return baud
            except Exception: 
                continue
        return None

    def toggle_serial_connection(self):
        if self.serial_connection and self.serial_connection.is_open: 
            self.on_serial_disconnect()
        else: 
            self.on_serial_connect()

    def on_serial_connect(self):
        port = self.port_cb.get().strip()
        baud = self.baud_cb.get().strip()
        if not port: 
            messagebox.showwarning("Thiếu Cổng", "Vui lòng chọn cổng COM.")
            return
        
        self.serial_toggle_btn.config(state="disabled")
        threading.Thread(target=self._connect_serial_thread, args=(port, baud), daemon=True).start()

    def _connect_serial_thread(self, port, baud):
        try:
            if baud == "Tự động":
                self.root.after(0, lambda: self.set_status(f"Đang tự động dò Baudrate cho {port}...", "info"))
                detected_baud = self.detect_baud_rate(port)
                if not detected_baud:
                    self.root.after(0, lambda: messagebox.showerror("Lỗi", "Không thể tự dò Baudrate. Vui lòng chọn thủ công."))
                    self.root.after(0, lambda: self.set_status("Lỗi: Dò Baudrate thất bại.", "error"))
                    self.root.after(0, lambda: self.serial_toggle_btn.config(state="normal"))
                    return
                baud = detected_baud
                self.root.after(0, lambda b=baud: self.baud_cb.set(b))

            self.root.after(0, lambda: self.set_status(f"Đang kết nối {port} @ {baud} bps...", "info"))
            self.serial_connection = serial.Serial(port, int(baud), timeout=0.1)
            
            p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
            self.root.after(0, lambda: self.set_status(f"Kết nối thành công! Đang đọc trên {port} @ {baud} bps", "success"))
            self.root.after(0, lambda: self.detect_btn.config(state="disabled"))
            self.root.after(0, lambda: self.serial_toggle_btn.config(text="Ngắt kết nối", state="normal"))
            self.root.after(0, lambda: self.monitor_txt.config(state="normal"))
            self.root.after(0, lambda: self._set_mcu_dependent_ui_state("normal"))
            
            self.stop_serial_thread.clear()
            self.serial_thread = threading.Thread(target=self._serial_read_thread, daemon=True)
            self.serial_thread.start()

        except serial.SerialException as e:
            self.root.after(0, lambda: messagebox.showerror("Lỗi Kết nối", f"Không thể mở cổng {port}.\nLỗi: {e}"))
            self.serial_connection = None
            self.root.after(0, lambda: self.set_status("Lỗi: kết nối Serial thất bại.", "error"))
            self.root.after(0, lambda: self.serial_toggle_btn.config(state="normal"))
            
    def on_serial_disconnect(self):
        self.is_wifi_connected = False
        self.is_server_connected = False
        self.btn_connect_wifi.config(text="Kết nối WiFi")
        self.btn_connect_server.config(text="Kết nối Server")
        
        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        self.wifi_status_lbl.config(text="Chưa kết nối", fg=p["error"])
        
        if self.serial_connection: 
            self.stop_serial_thread.set()
            try:
                self.serial_connection.close()
            except Exception:
                pass
            self.serial_connection = None
            
        self.set_status("Đã ngắt kết nối.", "info")
        self.detect_btn.config(state="normal")
        self.serial_toggle_btn.config(text="Kết nối")
        self._set_mcu_dependent_ui_state("disabled")
        
    def _serial_read_thread(self):
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
        self.is_new_line = True
        self.feedback_buffer = ""
        
        while not self.stop_serial_thread.is_set():
            if self.serial_connection and self.serial_connection.is_open:
                try:
                    data = self.serial_connection.read(1024)
                    if data:
                        text = self.decoder.decode(data)
                        if text:
                            self.root.after(0, self.append_to_monitor, text)
                            self.feedback_buffer += text
                            while '\n' in self.feedback_buffer:
                                line, self.feedback_buffer = self.feedback_buffer.split('\n', 1)
                                self.root.after(0, self.process_esp_feedback, line.strip())
                except serial.SerialException:
                    if not self.stop_serial_thread.is_set():
                        self.root.after(0, self.on_serial_disconnect)
                        self.root.after(0, lambda: messagebox.showerror("Lỗi", "Mất kết nối với thiết bị đột ngột."))
                    break
            else: 
                break
            time.sleep(0.01)

    def process_esp_feedback(self, line):
        if not line.startswith("RES:"): return
        
        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        
        if line == "RES:SYSTEM_READY":
            self.is_wifi_connected = False
            self.is_server_connected = False
            self.btn_connect_wifi.config(text="Kết nối WiFi")
            self.btn_connect_server.config(text="Kết nối Server")
            self.wifi_status_lbl.config(text="Chưa kết nối", fg=p["error"])
            self.set_status("Cảnh báo: ESP32 vừa khởi động lại. Vui lòng nối mạng.", "warning")
            return
            
        if line.startswith("RES:WIFI|"):
            parts = line.split("|", 4)
            if len(parts) >= 2:
                ssid = parts[1]
                if ssid not in self.scanned_networks:
                    self.scanned_networks.append(ssid)
                    self.wifi_cb.config(values=self.scanned_networks)
            return
            
        if line == "RES:SCAN_START":
            self.scanned_networks = []
            current_typed = self.wifi_cb.get()
            self.wifi_cb.config(values=[])
            self.wifi_cb.set(current_typed)
            self.set_status("Đang cập nhật danh sách WiFi...", "info")
            
        elif line == "RES:WIFI_CONNECTED":
            self.is_wifi_connected = True
            self.btn_connect_wifi.config(text="Ngắt WiFi")
            self.wifi_status_lbl.config(text="Đã kết nối WiFi", fg=p["success"])
            self.set_status("Thành công: ESP32 đã kết nối WiFi!", "success")
            
            # Lưu lại cặp WiFi và Mật khẩu khi kết nối thành công
            ssid = self.wifi_cb.get().strip()
            pwd = self.wifi_pass_entry.get().strip()
            if ssid:
                self.saved_wifi_credentials[ssid] = pwd
                self.save_config()
            
            if self.var_auto_connect_server.get():
                self.root.after(1000, self.on_server_connect) 
                
        elif line == "RES:WIFI_FAIL":
            self.is_wifi_connected = False
            self.btn_connect_wifi.config(text="Kết nối WiFi")
            self.wifi_status_lbl.config(text="Sai Pass hoặc mất sóng", fg=p["error"])
            self.set_status("Lỗi: ESP32 không thể vào WiFi.", "error")
            
        elif line == "RES:WIFI_DISCONNECTED":
            self.is_wifi_connected = False
            self.btn_connect_wifi.config(text="Kết nối WiFi")
            self.wifi_status_lbl.config(text="Chưa kết nối", fg=p["error"])
            self.set_status("Thành công: ESP32 đã ngắt WiFi.", "success")
            
        elif line == "RES:SERVER_CONNECTED":
            self.is_server_connected = True
            self.btn_connect_server.config(text="Ngắt Server")
            self.wifi_status_lbl.config(text="Đã kết nối WiFi & Server", fg=p["success"])
            self.set_status("Thành công: ESP32 đã nối TCP Server!", "success")
            
        elif line == "RES:SERVER_FAIL:NO_WIFI":
            self.is_server_connected = False
            self.btn_connect_server.config(text="Kết nối Server")
            self.wifi_status_lbl.config(text="Lỗi: Cần nối WiFi trước", fg=p["error"])
            self.set_status("Lỗi: Không thể kết nối Server khi chưa có WiFi.", "error")
            
        elif line == "RES:SERVER_FAIL":
            self.is_server_connected = False
            self.btn_connect_server.config(text="Kết nối Server")
            self.wifi_status_lbl.config(text="Lỗi nối Server", fg=p["warning"])
            self.set_status("Lỗi: ESP32 không thể nối Server (Timeout).", "error")
            
        elif line == "RES:SERVER_DISCONNECTED":
            self.is_server_connected = False
            self.btn_connect_server.config(text="Kết nối Server")
            self.wifi_status_lbl.config(text="Đã kết nối WiFi", fg=p["success"])
            self.set_status("Thành công: ESP32 đã ngắt TCP Server.", "success")
            
        elif line.startswith("RES:CMD_SENT_TO_SERVER:"):
            cmd_sent = line.split(":", 2)[2]
            self.set_status(f"Thành công: Đã đẩy lệnh '{cmd_sent}' lên Server TCP.", "success")
            
        elif line == "RES:SERVER_NOT_CONNECTED":
            self.set_status("Cảnh báo: Lệnh chỉ chạy ở ESP32 (Chưa kết nối TCP Server).", "warning")
            
    def append_to_monitor(self, text):
        self.monitor_txt.config(state="normal")
        
        is_server_rx = text.startswith("[SERVER_RX]")
        tag_to_use = 'server_rx' if is_server_rx else None
        
        if self.var_timestamp.get():
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if self.is_new_line and line:
                    timestamp = datetime.now().strftime("[%H:%M:%S.%f")[:-3] + "] "
                    self.monitor_txt.insert("end", timestamp, tag_to_use)
                self.monitor_txt.insert("end", line, tag_to_use)
                if i < len(lines) - 1:
                    self.monitor_txt.insert("end", "\n", tag_to_use)
                    self.is_new_line = True
                else: 
                    self.is_new_line = text.endswith('\n')
        else:
            self.monitor_txt.insert("end", text, tag_to_use)
            self.is_new_line = text.endswith('\n')
            
        current_lines = int(self.monitor_txt.index('end-1c').split('.')[0])
        if current_lines > So_dong_serial_toi_da: 
            self.monitor_txt.delete("1.0", "500.0")

        if self.var_autoscroll.get(): 
            self.monitor_txt.see("end")
        self.monitor_txt.config(state="disabled")
        
    def on_serial_send(self, event=None):
        data = self.serial_input.get("1.0", "end-1c")
        
        if not data.strip(): 
            if event: return "break" 
            return 
        
        self._send_serial_command(data, f"Đang gửi Serial: {data.strip()}...", "info")
        self.serial_input.delete("1.0", "end")
        if event: return "break" 

    def _send_serial_command(self, data_to_send, status_description="", status_level="info"):
        if self.serial_connection and self.serial_connection.is_open:
            if self.var_crlf.get() and not data_to_send.endswith("\n"): 
                data_to_send += "\r\n"
            try:
                self.serial_connection.write(data_to_send.encode('utf-8'))
                self.set_status(status_description, status_level)
            except serial.SerialException as e: 
                messagebox.showerror("Lỗi Gửi", f"Không thể gửi dữ liệu.\nLỗi: {e}")
        else:
            self.set_status("Lỗi: Chưa kết nối Serial!", "error")

    def on_wifi_scan(self):
        cmd = NETWORK_COMMANDS.get("SCAN_WIFI", "SCAN_WIFI")
        self._send_serial_command(cmd, "Đã gửi lệnh Quét WiFi...", "info")
        
    def toggle_wifi_connection(self):
        if not self.is_wifi_connected: 
            self.on_wifi_connect()
        else: 
            self.on_wifi_disconnect()

    def toggle_server_connection(self):
        if not self.is_server_connected: 
            self.on_server_connect()
        else: 
            self.on_server_disconnect()

    def on_wifi_connect(self):
        ssid = self.wifi_cb.get().strip()
        pwd = self.wifi_pass_entry.get().strip()
        
        set_cmd = f"SET_WIFI|{ssid}|{pwd}"
        self._send_serial_command(set_cmd, "Đang gửi cấu hình mạng...", "info")
        
        connect_cmd = NETWORK_COMMANDS.get("CONNECT_WIFI", "CONNECT_WIFI")
        self.root.after(100, lambda: self._send_serial_command(connect_cmd, "Đang chờ ESP32 nối WiFi...", "info"))
        
        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        self.wifi_status_lbl.config(text="Đang chờ kết nối...", fg=p["warning"]) 
        
    def on_wifi_disconnect(self):
        cmd = NETWORK_COMMANDS.get("DISCONNECT_WIFI", "DISCONNECT_WIFI")
        self._send_serial_command(cmd, "Đang chờ ESP32 ngắt WiFi...", "info")

    def on_server_connect(self):
        if not self.is_wifi_connected:
            messagebox.showwarning("Cảnh báo", "Vui lòng kết nối WiFi trước khi kết nối Server!")
            return
        
        ip = self.ip_entry.get().strip()
        port = self.port_entry.get().strip()
        
        set_cmd = f"SET_SERVER|{ip}|{port}"
        self._send_serial_command(set_cmd, "Đang gửi cấu hình Server...", "info")
        
        connect_cmd = NETWORK_COMMANDS.get("CONNECT_SERVER", "CONNECT_SERVER")
        self.root.after(100, lambda: self._send_serial_command(connect_cmd, f"Đang chờ ESP32 nối Server ({ip}:{port})...", "info"))
        
        p = DARK_PALETTE if self.is_dark_mode else LIGHT_PALETTE
        self.wifi_status_lbl.config(text="Đang chờ nối Server...", fg=p["warning"])

    def on_server_disconnect(self):
        cmd = NETWORK_COMMANDS.get("DISCONNECT_SERVER", "DISCONNECT_SERVER")
        self._send_serial_command(cmd, "Đang chờ ESP32 ngắt Server...", "info")

    def on_control_click(self, f_number):
        command_to_send = self.control_commands.get(f_number, f"F{f_number}_PRESSED")
        self._send_serial_command(command_to_send, f"Đang gửi lệnh {command_to_send} (F{f_number})...", "info")
            

def main():
    root = tk.Tk()
    app = InspectorApp(root)
    root.after(100, app.start_background_tasks)
    root.mainloop()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    
    if os.name == 'nt':
        import ctypes
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            try:
                ctypes.windll.user32.SetProcessDPIAware()
            except Exception:
                pass
                
    main()
