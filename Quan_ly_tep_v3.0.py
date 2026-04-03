import os, sys, shutil, unicodedata, re, json
from datetime import datetime

# Yêu cầu cài đặt: pip install PyQt6
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QPushButton, QProgressBar, QTextEdit,
                             QTreeView, QAbstractItemView, QMessageBox, QFileDialog,
                             QDialog, QCheckBox, QScrollArea, QComboBox, QMenu)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QFont, QColor, QPalette, QTextCursor

APP_TITLE   = "Quản lý tệp"
APP_VERSION = "3.0"
APP_AUTHOR  = "ybao"
SKIP_DST_IF_SUBDIR: bool = True

# --- DỮ LIỆU CỐT LÕI ---
KEYWORD_GROUPS: dict[str, list[str]] = {
    "Trắc nghiệm": ["tracnghiem", "tracnghim", "tracnghem", "tracngiem"],
    "Kiểm tra":    ["kiemtra", "kiem tra", "kiểm tra", "kiemtra15p", "kiemtra1tiet"],
    "Đề cương":    ["decuong", "de cuong", "đề cương"],
    "Đáp án":      ["dapan", "dap an", "đáp án"],
}
DEFAULT_KW_GROUPS: list[str] = ["Trắc nghiệm"]
DEFAULT_KW_NORMALIZE: bool   = True

EXT_GROUPS: dict[str, list[str]] = {
    "Đề trắc nghiệm": [".doc", ".docx"],
    "Word":     [".doc", ".docx", ".docm", ".dot", ".dotx", ".rtf", ".odt"],
    "PDF":      [".pdf"],
    "Text":     [".txt", ".md", ".rst", ".log"],
    "Excel":    [".xls", ".xlsx", ".xlsm", ".xlsb", ".xlt", ".xltx", ".xltm", ".ods"],
    "PPT":      [".ppt", ".pptx", ".pptm", ".pot", ".potx", ".pps", ".ppsx", ".odp"],
    "Adobe":    [".pdf", ".ai", ".psd", ".psb", ".eps", ".indd", ".aep", ".xd", ".fla"],
    "Image":    [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".svg", ".ico", ".heic", ".raw"],
    "Video":    [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".mpg", ".ts"],
    "Audio":    [".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a", ".opus"],
    "Archive":  [".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso", ".cab"],
    "Code":     [".py", ".js", ".ts", ".html", ".htm", ".css", ".json", ".xml", ".yaml", ".yml",
                 ".java", ".cpp", ".c", ".h", ".cs", ".php", ".rb", ".go", ".rs", ".sql", ".sh", ".bat", ".ps1"],
    "File tạm": [".tmp", ".temp", ".bak", ".old", ".orig", ".cache", ".swp", ".lock", ".crdownload", ".part"],
    "Exec":     [".exe", ".msi", ".apk", ".dmg", ".pkg", ".deb", ".rpm"],
}
DEFAULT_EXT_GROUPS: list[str] = ["Đề trắc nghiệm"]

DEFAULT_THEME_MODE: str   = "Light"
DEFAULT_SRC           = "{HOME}/Documents"
DEFAULT_DST           = "{HOME}/Documents/Bài trắc nghiệm"
DEFAULT_AUTO_SELECT: bool = True

_SCRIPT_NAME  = os.path.splitext(os.path.basename(
    sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)))[0]
SETTINGS_FILE = os.path.join(os.path.expanduser("~"), f"setting_{_SCRIPT_NAME}.json")

# ─────────────────────────────────────────────────────────────────────
# HÀM TIỆN ÍCH
# ─────────────────────────────────────────────────────────────────────
def _encode_path(p: str) -> str:
    home = os.path.expanduser("~").replace("\\", "/")
    p2   = p.replace("\\", "/")
    return ("{HOME}" + p2[len(home):]) if (p2.startswith(home + "/") or p2 == home) else p

def _decode_path(p: str) -> str:
    return p.replace("{HOME}", os.path.expanduser("~")).replace("/", os.sep)

def normalize(s: str) -> str:
    s = s.lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    return re.sub(r"[^a-z0-9]", "", s)

def raw_lower(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower()).strip()

def save_settings(data: dict) -> bool:
    """Lưu cài đặt ra JSON. Trả True nếu thành công."""
    try:
        d = dict(data)
        for k in ("src", "dst"):
            if k in d: d[k] = _encode_path(d[k])
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

# =====================================================================
# WORKERS — XỬ LÝ NỀN AN TOÀN
# =====================================================================

class ScanWorker(QThread):
    """Quét thư mục theo regex và phần mở rộng."""
    finished_scan = pyqtSignal(list, int, bool)
    log_msg       = pyqtSignal(str, str)

    def __init__(self, src, dst, regex, exts, is_normalize):
        super().__init__()
        self.src = src; self.dst = dst
        self.regex = regex; self.exts = exts
        self.is_normalize = is_normalize; self.is_cancelled = False

    def run(self):
        found = []; dirs_set = set()
        if not os.path.exists(self.src):
            self.log_msg.emit(f"Thư mục không tồn tại: {self.src}", "error")
            self.finished_scan.emit([], 0, False); return

        # [B8-FIX] except: pass → log cảnh báo rõ ràng.
        skip_dst = None
        if SKIP_DST_IF_SUBDIR and self.dst:
            try:
                sr = os.path.realpath(self.src); dr = os.path.realpath(self.dst)
                if dr.startswith(sr + os.sep) or dr == sr: skip_dst = dr
            except Exception as e:
                self.log_msg.emit(f"Cảnh báo: không thể kiểm tra thư mục đích ({e})", "warn")

        self.log_msg.emit(f"Đang quét: {self.src}", "info")
        for dirpath, dirs, files in os.walk(self.src):
            if self.is_cancelled: break
            if skip_dst:
                try:
                    rd = os.path.realpath(dirpath)
                    if rd == skip_dst or rd.startswith(skip_dst + os.sep):
                        dirs.clear(); continue
                except Exception as e:
                    self.log_msg.emit(f"Cảnh báo: bỏ qua thư mục (lỗi realpath: {e})", "warn")
            for fname in files:
                if self.is_cancelled: break
                ext = os.path.splitext(fname)[1].lower()
                if self.exts and "ALL" not in self.exts and ext not in self.exts: continue
                if self.regex:
                    fp = normalize(fname) if self.is_normalize else raw_lower(fname)
                    if not self.regex.search(fp): continue
                found.append({"path": os.path.join(dirpath, fname), "name": fname, "dir": dirpath})
                dirs_set.add(dirpath)
        self.finished_scan.emit(found, len(dirs_set), self.is_cancelled)


class FileActionWorker(QThread):
    """Sao chép hoặc di chuyển file."""
    progress_update = pyqtSignal(int)
    log_msg         = pyqtSignal(str, str)
    finished_action = pyqtSignal(int, int)
    # [B2-FIX] Signal 3 tham số: dict, status, dest_path
    # dest_path = đường dẫn file đích (cho cả copy và move), "" nếu không áp dụng.
    item_processed  = pyqtSignal(dict, str, str)

    def __init__(self, action, dst, files):
        super().__init__()
        self.action = action; self.dst = dst
        self.files = files; self.is_cancelled = False

    def run(self):
        ok = 0; total = len(self.files)
        for idx, f in enumerate(self.files):
            if self.is_cancelled: break
            if not os.path.exists(f["path"]):
                self.log_msg.emit(f"Lỗi: {f['name']} (Không tồn tại)", "error")
                self.item_processed.emit(f, "error_not_found", ""); continue
            try:
                tgt = os.path.join(self.dst, f["name"])
                if os.path.exists(tgt):
                    if os.path.realpath(f["path"]) == os.path.realpath(tgt):
                        self.log_msg.emit(f"Bỏ qua: {f['name']} (Đã nằm sẵn trong đích)", "warn")
                        self.item_processed.emit(f, "skip_duplicate", ""); continue
                    base, ext = os.path.splitext(f["name"])
                    tgt = os.path.join(self.dst, f"{base}_{datetime.now().strftime('%H%M%S_%f')}{ext}")
                if self.action == "move":
                    shutil.move(f["path"], tgt)
                else:
                    shutil.copy2(f["path"], tgt)
                ok += 1
                # [B2-FIX] Truyền tgt qua signal — không ghi vào dict từ thread nền.
                self.item_processed.emit(f, self.action, tgt)
            except Exception as ex:
                self.log_msg.emit(f"Lỗi: {f['name']} ({ex})", "error")
                self.item_processed.emit(f, "error_process", "")
            self.progress_update.emit(int(((idx + 1) / total) * 100))
        self.finished_action.emit(ok, total)


class UndoCopyWorker(QThread):
    """Hoàn tác sao chép: xóa các file đã sao chép vào thư mục đích."""
    progress_update = pyqtSignal(int)
    log_msg         = pyqtSignal(str, str)
    finished_undo   = pyqtSignal(int, int)
    # status: "success" | "not_found" | "error"
    item_processed  = pyqtSignal(dict, str)

    def __init__(self, records: list):
        super().__init__()
        self.records = records; self.is_cancelled = False

    def run(self):
        ok = 0; total = len(self.records)
        for idx, rec in enumerate(self.records):
            if self.is_cancelled: break
            copied_to = rec.get("copied_to", "")
            if not copied_to or not os.path.isfile(copied_to):
                self.log_msg.emit(f"Bỏ qua: {rec['name']} (không tìm thấy file đã sao chép)", "warn")
                self.item_processed.emit(rec, "not_found")
            else:
                try:
                    os.remove(copied_to)
                    ok += 1
                    self.item_processed.emit(rec, "success")
                except Exception as ex:
                    self.log_msg.emit(f"Lỗi xóa: {rec['name']} ({ex})", "error")
                    self.item_processed.emit(rec, "error")
            self.progress_update.emit(int(((idx + 1) / total) * 100))
        self.finished_undo.emit(ok, total)


class UndoMoveWorker(QThread):
    """Hoàn tác di chuyển: đưa file về vị trí gốc."""
    progress_update  = pyqtSignal(int)
    log_msg          = pyqtSignal(str, str)
    finished_undo    = pyqtSignal(int, int)
    # status: "success" | "not_found" | "error"
    item_processed   = pyqtSignal(dict, str)

    def __init__(self, records: list):
        super().__init__()
        self.records = records; self.is_cancelled = False

    def run(self):
        ok = 0; total = len(self.records)
        for idx, rec in enumerate(self.records):
            if self.is_cancelled: break
            src_file = rec.get("moved", "")
            dst_dir  = os.path.dirname(rec.get("original", ""))
            if not src_file or not os.path.isfile(src_file):
                self.log_msg.emit(f"Lỗi: {rec['name']} (Không tìm thấy file để khôi phục)", "error")
                self.item_processed.emit(rec, "not_found"); continue
            try:
                if not os.path.exists(dst_dir): os.makedirs(dst_dir)
                tgt = rec["original"]
                if os.path.exists(tgt):
                    base, ext = os.path.splitext(rec["name"])
                    tgt = os.path.join(dst_dir, f"{base}_restored_{datetime.now().strftime('%H%M%S_%f')}{ext}")
                shutil.move(src_file, tgt)
                ok += 1
                self.item_processed.emit(rec, "success")
            except Exception as ex:
                self.log_msg.emit(f"Lỗi khôi phục: {rec['name']} ({ex})", "error")
                self.item_processed.emit(rec, "error")
            self.progress_update.emit(int(((idx + 1) / total) * 100))
        self.finished_undo.emit(ok, total)


# =====================================================================
# GIAO DIỆN CHÍNH
# =====================================================================
class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.setMinimumSize(850, 550)

        # ── Trạng thái hệ thống ──────────────────────────────────────
        self._ui_ready       = False
        self._is_closing     = False
        self._is_processing  = False
        # "scan" | "copy" | "move" | "undo_copy" | "undo_move" | None
        self._processing_type: str | None = None

        # ── Dữ liệu ──────────────────────────────────────────────────
        self.found_files   = []
        self._copy_history: list[dict] = []   # {"original","copied_to","name"}
        self._move_history: list[dict] = []   # {"original","moved","name"}
        self._all_selected = False
        self._item_map:  dict = {}            # original_path → QStandardItem

        # ── Thread references ─────────────────────────────────────────
        self.scan_thread   = None
        self.action_thread = None
        self.undo_thread   = None

        # ── Khởi tạo ─────────────────────────────────────────────────
        self._load_settings()
        self.autosave_timer = QTimer()
        self.autosave_timer.setSingleShot(True)
        self.autosave_timer.timeout.connect(self._autosave_silent)

        self._build_ui()
        self._apply_styles()
        self._update_ui_states()

        self.setGeometry(self._saved_x, self._saved_y, self._saved_w, self._saved_h)
        if self._saved_state == "maximized":
            self.showMaximized()

        # [B9-FIX] Hiển thị cảnh báo settings SAU KHI UI sẵn sàng.
        if getattr(self, "_settings_load_warning", ""):
            self.log(self._settings_load_warning, "warn")
            del self._settings_load_warning

        self.log(f"Sẵn sàng  ·  Từ khóa: {self._kw_summary()}  ·  Hậu tố: {self._ext_summary()}", "info")
        self._ui_ready = True

    # ─────────────────────────────────────────────────────────────────
    # CÀI ĐẶT
    # ─────────────────────────────────────────────────────────────────
    def _load_settings(self):
        # [B9-FIX] Phân biệt từng loại exception, không bắt tất cả bằng "except: s = {}".
        self._settings_load_warning = ""
        s = {}
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                s = json.load(f)
        except FileNotFoundError:
            pass  # Lần đầu chạy — bình thường
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._settings_load_warning = f"⚠ File cài đặt bị hỏng, đặt lại mặc định. ({e})"
        except Exception as e:
            self._settings_load_warning = f"⚠ Không đọc được cài đặt: {e}"

        self._src_default       = _decode_path(s.get("src", DEFAULT_SRC))
        self._dst_default       = _decode_path(s.get("dst", DEFAULT_DST))
        self._auto_select       = s.get("auto_select", DEFAULT_AUTO_SELECT)
        self._active_ext_groups = s.get("ext_groups", list(DEFAULT_EXT_GROUPS))
        self._active_kw_groups  = s.get("kw_groups",  list(DEFAULT_KW_GROUPS))
        self._kw_normalize      = s.get("kw_normalize", DEFAULT_KW_NORMALIZE)
        self._kw_all_groups     = {**KEYWORD_GROUPS, **s.get("kw_custom_groups", {})}
        self._ui_theme_mode     = s.get("theme_mode", DEFAULT_THEME_MODE)
        self._saved_x     = s.get("win_x", 50)
        self._saved_y     = s.get("win_y", 50)
        self._saved_w     = s.get("win_w", 850)
        self._saved_h     = s.get("win_h", 550)
        self._saved_state = s.get("win_state", "maximized")

    # ─────────────────────────────────────────────────────────────────
    # XÂY DỰNG GIAO DIỆN
    # ─────────────────────────────────────────────────────────────────
    def _build_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(15, 10, 15, 10)
        main_layout.setSpacing(12)

        # ── ROW 1: Nguồn / Đích / Cài đặt ───────────────────────────
        row1 = QHBoxLayout(); row1.setSpacing(10)

        row1.addWidget(QLabel("Nguồn:"))
        self.ent_src = QLineEdit(self._src_default)
        row1.addWidget(self.ent_src, 1)
        self.btn_br_src = QPushButton("Thay đổi")
        self.btn_br_src.setProperty("class", "btn_secondary")
        row1.addWidget(self.btn_br_src)
        row1.addSpacing(10)

        row1.addWidget(QLabel("Đích:"))
        self.ent_dst = QLineEdit(self._dst_default)
        row1.addWidget(self.ent_dst, 1)
        self.btn_br_dst = QPushButton("Thay đổi")
        self.btn_br_dst.setProperty("class", "btn_secondary")
        row1.addWidget(self.btn_br_dst)
        row1.addSpacing(15)

        self.btn_settings = QPushButton("Cài đặt")
        self.btn_settings.setProperty("class", "btn_secondary")
        m = QMenu(self.btn_settings)
        m.addAction("Từ khóa").triggered.connect(self._open_kw_settings)
        m.addAction("Hậu tố").triggered.connect(self._open_ext_settings)
        m.addAction("Giao diện").triggered.connect(self._open_ui_settings)
        m.addSeparator()
        m.addAction("Thông tin").triggered.connect(self._open_info_dialog)
        m.addSeparator()
        m.addAction("Reset mặc định").triggered.connect(self._reset_defaults)
        self.btn_settings.setMenu(m)
        row1.addWidget(self.btn_settings)
        main_layout.addLayout(row1)

        # ── ROW 2: Hành động & Thống kê ─────────────────────────────
        row2 = QHBoxLayout(); row2.setSpacing(8)

        self.btn_scan = QPushButton("Quét File")
        self.btn_scan.setProperty("class", "btn_primary_blue")
        row2.addWidget(self.btn_scan)

        # Nút Sao Chép — toggle: "Sao Chép" ↔ "↩ Hoàn Tác Sao Chép"
        self.btn_copy = QPushButton("Sao Chép")
        self.btn_copy.setProperty("class", "btn_primary_cyan")
        row2.addWidget(self.btn_copy)

        # Nút Di Chuyển — toggle: "Di Chuyển" ↔ "↩ Hoàn Tác Di Chuyển"
        self.btn_move = QPushButton("Di Chuyển")
        self.btn_move.setProperty("class", "btn_primary_green")
        row2.addWidget(self.btn_move)

        # Nút Dừng — thay thế nút Khôi Phục cũ
        self.btn_stop = QPushButton("Dừng")
        self.btn_stop.setProperty("class", "btn_danger_solid")
        row2.addWidget(self.btn_stop)

        row2.addSpacing(10)

        self.progress = QProgressBar()
        self.progress.setFixedWidth(175)
        self.progress.setTextVisible(True)
        self.progress.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress.hide()
        row2.addWidget(self.progress)

        row2.addStretch()

        lbl_t = QLabel("Tệp:")
        lbl_t.setStyleSheet("font-weight: bold; color: gray;")
        row2.addWidget(lbl_t)
        self.lbl_total = QLabel("0")
        self.lbl_total.setStyleSheet("color: #3B82F6; font-weight: bold;")
        row2.addWidget(self.lbl_total)
        row2.addSpacing(10)

        lbl_s = QLabel("Chọn:")
        lbl_s.setStyleSheet("font-weight: bold; color: gray;")
        row2.addWidget(lbl_s)
        self.lbl_sel = QLabel("0")
        self.lbl_sel.setStyleSheet("color: #10B981; font-weight: bold;")
        row2.addWidget(self.lbl_sel)
        row2.addSpacing(15)

        self.btn_toggle_all = QPushButton("Chọn Tất Cả")
        self.btn_toggle_all.setProperty("class", "btn_secondary")
        row2.addWidget(self.btn_toggle_all)

        self.btn_clear = QPushButton("Xóa Danh Sách")
        self.btn_clear.setProperty("class", "btn_danger_text")
        row2.addWidget(self.btn_clear)
        row2.addSpacing(15)

        row2.addWidget(QLabel("Tự động chọn:"))
        self.btn_auto = QPushButton()
        self.btn_auto.setFixedWidth(55)
        self._update_auto_btn_ui()
        row2.addWidget(self.btn_auto)
        main_layout.addLayout(row2)

        # ── ROW 3: Cây dữ liệu ──────────────────────────────────────
        self.tree = QTreeView()
        self.tree.setHeaderHidden(True)
        self.tree.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tree.setAnimated(True)
        self.model = QStandardItemModel()
        self.tree.setModel(self.model)
        main_layout.addWidget(self.tree, 1)

        # ── ROW 4: Nhật ký ──────────────────────────────────────────
        log_hlay = QHBoxLayout()
        log_lbl  = QLabel("Log hoạt động:")
        log_lbl.setStyleSheet("color: gray; font-weight: bold;")
        log_hlay.addWidget(log_lbl); log_hlay.addStretch()
        main_layout.addLayout(log_hlay)

        self.log_box = QTextEdit()
        self.log_box.setFixedHeight(75)
        self.log_box.setReadOnly(True)
        self.log_box.setObjectName("log_box")
        self.log_box.document().setMaximumBlockCount(1000)
        main_layout.addWidget(self.log_box)

        # ── Kết nối tín hiệu ────────────────────────────────────────
        self.btn_scan.clicked.connect(self.run_scan)
        self.btn_copy.clicked.connect(self.run_copy)
        self.btn_move.clicked.connect(self.run_move)
        self.btn_stop.clicked.connect(self._do_stop)
        self.btn_br_src.clicked.connect(lambda: self._browse(self.ent_src))
        self.btn_br_dst.clicked.connect(lambda: self._browse(self.ent_dst))
        self.btn_clear.clicked.connect(self._clear_results)
        self.btn_toggle_all.clicked.connect(self._toggle_all)
        self.btn_auto.clicked.connect(self._toggle_auto)
        self.ent_src.textChanged.connect(self._schedule_save)
        self.ent_dst.textChanged.connect(self._schedule_save)
        self.model.itemChanged.connect(self._on_item_changed)

    # ─────────────────────────────────────────────────────────────────
    # THEME & STYLE
    # ─────────────────────────────────────────────────────────────────
    def _is_dark_mode(self):
        return self.palette().color(QPalette.ColorRole.Window).lightness() < 128

    def _apply_styles(self):
        is_dark = self._ui_theme_mode == "Dark"
        QApplication.setStyle("Fusion")
        p = QPalette()
        bc = QColor("#71717A" if is_dark else "#94A3B8")
        if is_dark:
            p.setColor(QPalette.ColorRole.Window,          QColor("#1E1E1E"))
            p.setColor(QPalette.ColorRole.WindowText,      QColor("#E0E0E0"))
            p.setColor(QPalette.ColorRole.Base,            QColor("#2D2D30"))
            p.setColor(QPalette.ColorRole.AlternateBase,   QColor("#1E1E1E"))
            p.setColor(QPalette.ColorRole.Text,            QColor("#E0E0E0"))
            p.setColor(QPalette.ColorRole.Button,          QColor("#3F3F46"))
            p.setColor(QPalette.ColorRole.ButtonText,      QColor("#E0E0E0"))
            p.setColor(QPalette.ColorRole.Highlight,       QColor("#3B82F6"))
            p.setColor(QPalette.ColorRole.HighlightedText, QColor("#FFFFFF"))
        else:
            p.setColor(QPalette.ColorRole.Window,          QColor("#F3F4F6"))
            p.setColor(QPalette.ColorRole.WindowText,      QColor("#1E293B"))
            p.setColor(QPalette.ColorRole.Base,            QColor("#FFFFFF"))
            p.setColor(QPalette.ColorRole.AlternateBase,   QColor("#F3F4F6"))
            p.setColor(QPalette.ColorRole.Text,            QColor("#1E293B"))
            p.setColor(QPalette.ColorRole.Button,          QColor("#E2E8F0"))
            p.setColor(QPalette.ColorRole.ButtonText,      QColor("#1E293B"))
            p.setColor(QPalette.ColorRole.Highlight,       QColor("#3B82F6"))
            p.setColor(QPalette.ColorRole.HighlightedText, QColor("#FFFFFF"))
        p.setColor(QPalette.ColorRole.Dark,   bc)
        p.setColor(QPalette.ColorRole.Shadow, bc)
        QApplication.setPalette(p)

        # [B7-FIX] Font size cố định — tính năng scaling đã bị xóa khỏi UI.
        fs_base = 13; fs_log = 12
        qb = "#52525B" if is_dark else "#CBD5E1"
        qm = "#2D2D30" if is_dark else "#FFFFFF"
        qh = "#3F3F46" if is_dark else "#E2E8F0"

        qss = f"""
            QWidget {{ font-family: 'Segoe UI', Arial; font-size: {fs_base}px; }}
            QLineEdit {{ padding: 6px; border: 1px solid {qb}; border-radius: 4px; }}
            QPushButton {{ padding: 6px 12px; border-radius: 4px; border: none; }}
            QPushButton:disabled {{ background-color: {"#3F3F46" if is_dark else "#CBD5E1"};
                                    color: {"#A0A0A0" if is_dark else "gray"}; }}
            QPushButton[class="btn_secondary"] {{
                background-color: {"#3F3F46" if is_dark else "#E2E8F0"}; font-weight: bold; }}
            QPushButton[class="btn_secondary"]:hover {{
                background-color: {"#52525B" if is_dark else "#CBD5E1"}; }}
            QPushButton[class="btn_secondary"]::menu-indicator {{ image: none; }}
            QPushButton[class="btn_primary_blue"]  {{
                background-color: #3B82F6; color: white; font-weight: bold; }}
            QPushButton[class="btn_primary_cyan"]  {{
                background-color: #0EA5E9; color: white; font-weight: bold; }}
            QPushButton[class="btn_primary_green"] {{
                background-color: #10B981; color: white; font-weight: bold; }}
            QPushButton[class="btn_undo_cyan"]     {{
                background-color: #F59E0B; color: white; font-weight: bold; }}
            QPushButton[class="btn_undo_green"]    {{
                background-color: #F59E0B; color: white; font-weight: bold; }}
            QPushButton[class="btn_danger_solid"]  {{
                background-color: #DC2626; color: white; font-weight: bold; }}
            QPushButton[class="btn_danger_solid"]:hover {{
                background-color: #B91C1C; }}
            QPushButton[class="btn_danger_solid"]:disabled {{
                background-color: {"#3F3F46" if is_dark else "#CBD5E1"};
                color: {"#A0A0A0" if is_dark else "gray"}; }}
            QPushButton[class="btn_danger_text"] {{
                color: #DC2626; font-weight: bold; background-color: transparent; }}
            QPushButton[class="btn_danger_text"]:hover {{
                background-color: {"#3F3F46" if is_dark else "#FEE2E2"}; }}
            QPushButton[class="btn_danger_text"]:disabled {{
                color: {"#A0A0A0" if is_dark else "gray"}; background-color: transparent; }}
            QTreeView {{
                border: 1px solid {qb}; border-radius: 4px; padding: 5px; outline: none; }}
            QTreeView::item {{ padding: 4px; }}
            QProgressBar {{
                border: 1px solid {qb}; border-radius: 4px; text-align: center;
                color: {"white" if is_dark else "black"}; font-weight: bold; }}
            QProgressBar::chunk {{ background-color: #3B82F6; border-radius: 3px; }}
            QMenu {{
                background-color: {qm}; border: 1px solid {qb}; border-radius: 4px; padding: 4px; }}
            QMenu::item {{ padding: 6px 24px; border-radius: 4px; }}
            QMenu::item:selected {{ background-color: {qh}; }}
            #log_box {{
                border: 1px solid {qb}; border-radius: 4px;
                font-family: Consolas; font-size: {fs_log}px; }}
        """
        self.setStyleSheet(qss)
        QApplication.instance().setFont(QFont("Segoe UI", fs_base))
        self._update_auto_btn_ui()

    def _refresh_scan_btn_style(self):
        """[B12-FIX] Làm tươi style CHỈ nút Quét — không gọi _apply_styles() toàn bộ."""
        self.btn_scan.setProperty("class", "btn_primary_blue")
        self.btn_scan.setStyleSheet("")
        self.btn_scan.style().unpolish(self.btn_scan)
        self.btn_scan.style().polish(self.btn_scan)

    def _update_auto_btn_ui(self):
        if self._auto_select:
            self.btn_auto.setStyleSheet("background-color: #3B82F6; color: white; font-weight: bold;")
            self.btn_auto.setText("BẬT")
        else:
            bg = "#3F3F46" if self._is_dark_mode() else "#E2E8F0"
            fg = "#E0E0E0" if self._is_dark_mode() else "#1E293B"
            self.btn_auto.setStyleSheet(f"background-color: {bg}; color: {fg}; font-weight: bold;")
            self.btn_auto.setText("TẮT")

    def _toggle_auto(self):
        self._auto_select = not self._auto_select
        self._update_auto_btn_ui(); self._schedule_save()

    def _browse(self, le):
        p = QFileDialog.getExistingDirectory(self, "Chọn thư mục")
        if p: le.setText(p); self._schedule_save()

    def log(self, msg, tag="info"):
        if self._is_closing: return
        is_dark = self._is_dark_mode()
        colors  = {"info": "#A0A0A0" if is_dark else "gray",
                   "success": "#22C55E" if is_dark else "#10B981",
                   "warn":    "#F59E0B" if is_dark else "#D97706",
                   "error":   "#EF4444" if is_dark else "#DC2626"}
        c    = colors.get(tag, "black")
        ts   = datetime.now().strftime("%H:%M:%S")
        html = f'<span style="color:#6B7280">[{ts}]</span> <span style="color:{c}">{msg}</span>'
        self.log_box.append(html)
        self.log_box.moveCursor(QTextCursor.MoveOperation.End)

    # ─────────────────────────────────────────────────────────────────
    # TRUNG TÂM KIỂM SOÁT TRẠNG THÁI UI
    # ─────────────────────────────────────────────────────────────────
    def _update_ui_states(self):
        has_copy_hist = len(self._copy_history) > 0
        has_move_hist = len(self._move_history) > 0
        try:
            sel_count = int(self.lbl_sel.text())
        except ValueError:
            sel_count = 0
        has_files = len(self.found_files) > 0

        if self._is_processing:
            # Khóa tất cả trừ btn_stop và btn_scan (scan có nút tự quản)
            for w in [self.ent_src, self.ent_dst, self.btn_br_src, self.btn_br_dst,
                      self.btn_settings, self.btn_auto, self.btn_copy, self.btn_move,
                      self.btn_toggle_all, self.btn_clear]:
                w.setEnabled(False)
            # btn_stop: chỉ kích hoạt khi đang copy/move/undo (không phải scan)
            is_action = self._processing_type in ("copy", "move", "undo_copy", "undo_move")
            self.btn_stop.setEnabled(is_action)
            return

        # ── Không đang xử lý ─────────────────────────────────────────
        for w in [self.ent_src, self.ent_dst, self.btn_br_src, self.btn_br_dst,
                  self.btn_settings, self.btn_auto, self.btn_scan]:
            w.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.btn_clear.setEnabled(has_files)
        self.btn_toggle_all.setEnabled(has_files)

        # ── Nút Sao Chép ─────────────────────────────────────────────
        if has_copy_hist:
            self.btn_copy.setText(f"↩ Hoàn Tác Sao Chép ({len(self._copy_history)})")
            self.btn_copy.setProperty("class", "btn_undo_cyan")
            self.btn_copy.setEnabled(True)
        else:
            self.btn_copy.setText("Sao Chép")
            self.btn_copy.setProperty("class", "btn_primary_cyan")
            self.btn_copy.setEnabled(sel_count > 0)
        self.btn_copy.style().unpolish(self.btn_copy)
        self.btn_copy.style().polish(self.btn_copy)

        # ── Nút Di Chuyển ────────────────────────────────────────────
        if has_move_hist:
            self.btn_move.setText(f"↩ Hoàn Tác Di Chuyển ({len(self._move_history)})")
            self.btn_move.setProperty("class", "btn_undo_green")
            self.btn_move.setEnabled(True)
        else:
            self.btn_move.setText("Di Chuyển")
            self.btn_move.setProperty("class", "btn_primary_green")
            self.btn_move.setEnabled(sel_count > 0)
        self.btn_move.style().unpolish(self.btn_move)
        self.btn_move.style().polish(self.btn_move)

    # ─────────────────────────────────────────────────────────────────
    # CÂY THƯ MỤC — QUẢN LÝ TRẠNG THÁI CHECKBOX
    # ─────────────────────────────────────────────────────────────────
    def _recalc_folder_state(self, folder_item):
        checked = enabled = 0
        for r in range(folder_item.rowCount()):
            ch = folder_item.child(r)
            if ch.isEnabled():
                enabled += 1
                if ch.checkState() == Qt.CheckState.Checked: checked += 1
        self.model.blockSignals(True)
        if enabled == 0:
            folder_item.setEnabled(False)
            folder_item.setCheckState(Qt.CheckState.Unchecked)
        else:
            folder_item.setEnabled(True)
            if checked == 0:       folder_item.setCheckState(Qt.CheckState.Unchecked)
            elif checked == enabled: folder_item.setCheckState(Qt.CheckState.Checked)
            else:                  folder_item.setCheckState(Qt.CheckState.PartiallyChecked)
        self.model.blockSignals(False)

    def _on_item_changed(self, item):
        if not item.isCheckable(): return
        if self._is_processing:
            self.model.blockSignals(True)
            old = Qt.CheckState.Unchecked if item.checkState() == Qt.CheckState.Checked else Qt.CheckState.Checked
            item.setCheckState(old)
            self.model.blockSignals(False)
            self.log("⚠ Hệ thống đang xử lý, không thể thay đổi lựa chọn.", "warn")
            return
        # [B1-FIX] try/finally đảm bảo signal luôn được reconnect kể cả khi exception.
        self.model.itemChanged.disconnect(self._on_item_changed)
        try:
            state = item.checkState()
            if item.hasChildren():
                for r in range(item.rowCount()):
                    ch = item.child(r)
                    if ch.isEnabled(): ch.setCheckState(state)
            else:
                parent = item.parent()
                if parent: self._recalc_folder_state(parent)
        finally:
            self.model.itemChanged.connect(self._on_item_changed)
        self._update_sel_count()

    def _update_sel_count(self):
        count = 0
        for r in range(self.model.rowCount()):
            fi = self.model.item(r)
            for c in range(fi.rowCount()):
                ch = fi.child(c)
                if ch.checkState() == Qt.CheckState.Checked and ch.isEnabled():
                    count += 1
        self.lbl_sel.setText(str(count))
        self._update_ui_states()

    def _toggle_all(self):
        self._all_selected = not self._all_selected
        state = Qt.CheckState.Checked if self._all_selected else Qt.CheckState.Unchecked
        # [B1-FIX] try/finally đảm bảo reconnect kể cả khi exception trong vòng lặp.
        self.model.itemChanged.disconnect(self._on_item_changed)
        try:
            for r in range(self.model.rowCount()):
                fi = self.model.item(r)
                if fi.isEnabled(): fi.setCheckState(state)
                for c in range(fi.rowCount()):
                    ch = fi.child(c)
                    if ch.isEnabled(): ch.setCheckState(state)
                self._recalc_folder_state(fi)
        finally:
            self.model.itemChanged.connect(self._on_item_changed)
        self.btn_toggle_all.setText("Bỏ Chọn Hết" if self._all_selected else "Chọn Tất Cả")
        self._update_sel_count()

    def _clear_results(self):
        self.model.clear()
        self.found_files = []; self._item_map = {}
        self.lbl_total.setText("0"); self.lbl_sel.setText("0")
        self._all_selected = False
        self.btn_toggle_all.setText("Chọn Tất Cả")
        self._update_ui_states()

    # ─────────────────────────────────────────────────────────────────
    # QUÉT FILE
    # ─────────────────────────────────────────────────────────────────
    def _build_search_regex(self):
        if not self._active_kw_groups: return None
        parts = []
        for g in self._active_kw_groups:
            for kw in self._kw_all_groups.get(g, []):
                kp = normalize(kw) if self._kw_normalize else raw_lower(kw)
                if kp: parts.append(re.escape(kp))
        if not parts: return None
        parts.sort(key=len, reverse=True)
        return re.compile("|".join(parts))

    def run_scan(self):
        if self.scan_thread and self.scan_thread.isRunning():
            self.scan_thread.is_cancelled = True
            self.btn_scan.setText("Đang dừng...")
            self.btn_scan.setEnabled(False)
            return
        if not self._active_kw_groups and not self._active_ext_groups:
            QMessageBox.warning(self, "Thiếu cài đặt", "Chưa chọn từ khóa VÀ chưa chọn nhóm hậu tố.")
            return

        self._is_processing = True; self._processing_type = "scan"
        self._update_ui_states()
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("Dừng Quét")
        self.btn_scan.setStyleSheet(
            "background-color: #DC2626; color: white; font-weight: bold; padding: 6px 12px;")

        self.progress.setMinimum(0); self.progress.setMaximum(0)
        self.progress.setFormat("  Đang quét dữ liệu...  "); self.progress.show()
        self._clear_results()

        if self.scan_thread: self.scan_thread.deleteLater()
        self.scan_thread = ScanWorker(
            self.ent_src.text().strip(), self.ent_dst.text().strip(),
            self._build_search_regex(),
            ["ALL"] if "ALL" in self._active_ext_groups
                    else [ext for g in self._active_ext_groups for ext in EXT_GROUPS.get(g, [])],
            self._kw_normalize)
        self.scan_thread.log_msg.connect(self.log)
        self.scan_thread.finished_scan.connect(self._on_scan_finished)
        self.scan_thread.start()

    def _on_scan_finished(self, found, total_dirs, cancelled):
        if self._is_closing: return
        self._is_processing = False; self._processing_type = None
        self.btn_scan.setText("Quét File")
        self._refresh_scan_btn_style()  # [B12-FIX]
        self.progress.hide()

        if cancelled: self.log("Đã dừng quét.", "warn")
        else: self.log(f"Hoàn thành. Tìm thấy {len(found)} file trong {total_dirs} thư mục.", "success")

        self.lbl_total.setText(str(len(found)))
        self.found_files = found
        groups = {}
        for f in found: groups.setdefault(f["dir"], []).append(f)

        for folder, files in groups.items():
            fi = QStandardItem(f"📁 {folder}  ({len(files)} file)")
            fi.setCheckable(True)
            font = fi.font(); font.setBold(True); fi.setFont(font)
            for f in files:
                ci = QStandardItem(f["name"])
                ci.setCheckable(True)
                ci.setData(f, Qt.ItemDataRole.UserRole)
                fi.appendRow(ci)
                self._item_map[f["path"]] = ci
            self.model.appendRow(fi)

        self.tree.expandAll()
        if len(found) > 0 and self._auto_select and not cancelled:
            self._all_selected = False; self._toggle_all()
        else:
            self._update_sel_count()

    # ─────────────────────────────────────────────────────────────────
    # SAO CHÉP & DI CHUYỂN — DISPATCH THEO TRẠNG THÁI
    # ─────────────────────────────────────────────────────────────────
    def run_copy(self):
        """Sao chép nếu chưa có lịch sử; Hoàn tác sao chép nếu đã có."""
        if self._copy_history:
            self._do_undo_copy()
        else:
            self._do_action("copy")

    def run_move(self):
        """Di chuyển nếu chưa có lịch sử; Hoàn tác di chuyển nếu đã có."""
        if self._move_history:
            self._do_undo_move()
        else:
            self._do_action("move")

    def _do_action(self, action: str):
        """Thực thi sao chép hoặc di chuyển."""
        items = self._get_selected_items()
        if not items: return

        dst = self.ent_dst.text().strip()
        if not dst:
            QMessageBox.warning(self, "Lỗi", "Chưa nhập thư mục đích."); return

        # [B3-FIX] os.makedirs bọc try/except — PermissionError/OSError gây crash trước đây.
        try:
            os.makedirs(dst, exist_ok=True)
        except OSError as e:
            QMessageBox.critical(self, "Lỗi tạo thư mục",
                                 f"Không thể tạo thư mục đích:\n{e}"); return

        self.log(f"Đang tiến hành {'Sao chép' if action == 'copy' else 'Di chuyển'}...", "info")
        self._is_processing = True; self._processing_type = action
        self._update_ui_states()

        self.progress.setMinimum(0); self.progress.setMaximum(100); self.progress.setValue(0)
        self.progress.setFormat(f"  Đang {'Sao chép' if action == 'copy' else 'Di chuyển'} %p%  ")
        self.progress.show()

        files_data = [it.data(Qt.ItemDataRole.UserRole) for it in items]

        # [B13-FIX] Disconnect tường minh signal cũ trước khi tạo thread mới.
        if self.action_thread:
            try:
                self.action_thread.item_processed.disconnect()
                self.action_thread.finished_action.disconnect()
                self.action_thread.log_msg.disconnect()
                self.action_thread.progress_update.disconnect()
            except Exception:
                pass
            self.action_thread.deleteLater()

        self.action_thread = FileActionWorker(action, dst, files_data)
        self.action_thread.log_msg.connect(self.log)
        self.action_thread.progress_update.connect(self.progress.setValue)

        # [B2-FIX] Handler nhận dest_path từ signal — không đọc từ dict thread nền.
        def _on_item(f_data, status, dest_path):
            if self._is_closing: return
            file_item = self._item_map.get(f_data["path"])
            if not file_item: return
            self.model.blockSignals(True)
            file_item.setCheckState(Qt.CheckState.Unchecked)
            is_dark = self._is_dark_mode()
            if status == "move":
                file_item.setText(f"{f_data['name']}   [✔ Đã di chuyển]")
                file_item.setForeground(QColor("#22C55E" if is_dark else "#15803D"))
                file_item.setEnabled(False)
                self._move_history.append({
                    "original": f_data["path"], "moved": dest_path, "name": f_data["name"]})
            elif status == "copy":
                file_item.setText(f"{f_data['name']}   [✔ Đã sao chép]")
                file_item.setForeground(QColor("#3B82F6" if is_dark else "#1D4ED8"))
                self._copy_history.append({
                    "original": f_data["path"], "copied_to": dest_path, "name": f_data["name"]})
            elif status == "skip_duplicate":
                file_item.setText(f"{f_data['name']}   [Bỏ qua - Trùng lặp]")
                file_item.setForeground(QColor("#F59E0B" if is_dark else "#D97706"))
                file_item.setEnabled(False)
            elif "error" in status:
                file_item.setText(f"{f_data['name']}   [Lỗi]")
                file_item.setForeground(QColor("#EF4444" if is_dark else "#B91C1C"))
                if status == "error_not_found": file_item.setEnabled(False)
            self.model.blockSignals(False)

        self.action_thread.item_processed.connect(_on_item)

        def _on_done(ok, total):
            if self._is_closing: return
            cancelled = self.action_thread.is_cancelled if self.action_thread else False
            verb = "Sao chép" if action == "copy" else "Di chuyển"
            if cancelled:
                self.log(f"Đã dừng {verb.lower()}. Xử lý được {ok}/{total} file.", "warn")
            else:
                self.log(f"Hoàn thành {verb.lower()} {ok}/{total} file.", "success")
            for r in range(self.model.rowCount()):
                self._recalc_folder_state(self.model.item(r))
            self._is_processing = False; self._processing_type = None
            self.btn_stop.setText("Dừng")   # Reset text nếu đã được đổi thành "Đang dừng..."
            self.progress.hide(); self._update_sel_count()

        self.action_thread.finished_action.connect(_on_done)
        self.action_thread.start()

    def _get_selected_items(self):
        items = []
        for r in range(self.model.rowCount()):
            fi = self.model.item(r)
            for c in range(fi.rowCount()):
                ch = fi.child(c)
                if ch.checkState() == Qt.CheckState.Checked and ch.isEnabled():
                    items.append(ch)
        return items

    # ─────────────────────────────────────────────────────────────────
    # DỪNG TIẾN TRÌNH
    # ─────────────────────────────────────────────────────────────────
    def _do_stop(self):
        """Dừng tiến trình sao chép, di chuyển hoặc hoàn tác đang chạy."""
        cancelled_any = False
        if self.action_thread and self.action_thread.isRunning():
            self.action_thread.is_cancelled = True
            cancelled_any = True
        if self.undo_thread and self.undo_thread.isRunning():
            self.undo_thread.is_cancelled = True
            cancelled_any = True
        if cancelled_any:
            self.btn_stop.setEnabled(False)
            self.btn_stop.setText("Đang dừng...")
            self.log("Đã gửi lệnh dừng, chờ tiến trình hiện tại hoàn tất...", "warn")
        else:
            self.log("Không có tiến trình nào đang chạy.", "warn")

    # ─────────────────────────────────────────────────────────────────
    # HOÀN TÁC SAO CHÉP — Xóa file đã copy khỏi đích
    # ─────────────────────────────────────────────────────────────────
    def _do_undo_copy(self):
        if not self._copy_history: return
        n = len(self._copy_history)
        reply = QMessageBox.question(
            self, "Xác nhận hoàn tác sao chép",
            f"Sẽ XÓA VĨNH VIỄN {n} file đã sao chép vào thư mục đích.\n"
            f"Thao tác này KHÔNG THỂ khôi phục. Tiếp tục?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes: return

        records = list(self._copy_history)   # snapshot — không sửa list gốc trong thread
        self.log(f"Đang hoàn tác sao chép {n} file...", "info")
        self._is_processing = True; self._processing_type = "undo_copy"
        self._update_ui_states()

        self.progress.setMinimum(0); self.progress.setMaximum(100); self.progress.setValue(0)
        self.progress.setFormat("  Đang hoàn tác sao chép %p%  "); self.progress.show()

        if self.undo_thread:
            try:
                self.undo_thread.item_processed.disconnect()
                self.undo_thread.finished_undo.disconnect()
                self.undo_thread.log_msg.disconnect()
                self.undo_thread.progress_update.disconnect()
            except Exception:
                pass
            self.undo_thread.deleteLater()

        self.undo_thread = UndoCopyWorker(records)
        self.undo_thread.log_msg.connect(self.log)
        self.undo_thread.progress_update.connect(self.progress.setValue)

        failed: list[dict] = []

        def _on_item(rec, status):
            if self._is_closing: return
            if status == "error":
                failed.append(rec)   # Giữ lại trong history để retry
            else:
                # "success" hoặc "not_found" — đều xem là đã undo thành công
                # Khôi phục hiển thị trong cây về trạng thái ban đầu (chỉ tên)
                file_item = self._item_map.get(rec.get("original", ""))
                if file_item:
                    self.model.blockSignals(True)
                    file_item.setText(rec["name"])
                    is_dark = self._is_dark_mode()
                    file_item.setForeground(QColor("#E0E0E0" if is_dark else "#1E293B"))
                    self.model.blockSignals(False)

        def _on_done(ok, total):
            if self._is_closing: return
            cancelled = self.undo_thread.is_cancelled if self.undo_thread else False
            if cancelled:
                self.log(f"Đã dừng hoàn tác sao chép. Xóa được {ok}/{total} file.", "warn")
            else:
                self.log(f"Hoàn tác sao chép: đã xóa {ok}/{total} file.", "success")
            # Chỉ giữ lại những record bị lỗi (thất bại) trong history
            self._copy_history = failed
            self._is_processing = False; self._processing_type = None
            self.progress.hide()
            self.btn_stop.setText("Dừng")
            self._update_sel_count()

        self.undo_thread.item_processed.connect(_on_item)
        self.undo_thread.finished_undo.connect(_on_done)
        self.undo_thread.start()

    # ─────────────────────────────────────────────────────────────────
    # HOÀN TÁC DI CHUYỂN — Trả file về vị trí cũ
    # ─────────────────────────────────────────────────────────────────
    def _do_undo_move(self):
        if not self._move_history: return
        n = len(self._move_history)
        reply = QMessageBox.question(
            self, "Xác nhận hoàn tác di chuyển",
            f"Sẽ di chuyển {n} file trở về vị trí gốc. Tiếp tục?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes: return

        records = list(self._move_history)
        self.log(f"Đang hoàn tác di chuyển {n} file...", "info")
        self._is_processing = True; self._processing_type = "undo_move"
        self._update_ui_states()

        self.progress.setMinimum(0); self.progress.setMaximum(100); self.progress.setValue(0)
        self.progress.setFormat("  Đang hoàn tác di chuyển %p%  "); self.progress.show()

        if self.undo_thread:
            try:
                self.undo_thread.item_processed.disconnect()
                self.undo_thread.finished_undo.disconnect()
                self.undo_thread.log_msg.disconnect()
                self.undo_thread.progress_update.disconnect()
            except Exception:
                pass
            self.undo_thread.deleteLater()

        self.undo_thread = UndoMoveWorker(records)
        self.undo_thread.log_msg.connect(self.log)
        self.undo_thread.progress_update.connect(self.progress.setValue)

        failed: list[dict] = []

        def _on_item(rec, status):
            if self._is_closing: return
            if status == "error":
                failed.append(rec)
            else:
                # success hoặc not_found — phục hồi tree item
                file_item = self._item_map.get(rec.get("original", ""))
                if file_item:
                    self.model.blockSignals(True)
                    file_item.setText(rec["name"])
                    is_dark = self._is_dark_mode()
                    file_item.setForeground(QColor("#E0E0E0" if is_dark else "#1E293B"))
                    file_item.setEnabled(True)
                    file_item.setCheckState(Qt.CheckState.Unchecked)
                    self.model.blockSignals(False)

        def _on_done(ok, total):
            if self._is_closing: return
            cancelled = self.undo_thread.is_cancelled if self.undo_thread else False
            if cancelled:
                self.log(f"Đã dừng hoàn tác di chuyển. Khôi phục được {ok}/{total} file.", "warn")
            else:
                self.log(f"Hoàn tác di chuyển: khôi phục {ok}/{total} file về vị trí gốc.", "success")
            self._move_history = failed
            for r in range(self.model.rowCount()):
                self._recalc_folder_state(self.model.item(r))
            self._is_processing = False; self._processing_type = None
            self.progress.hide()
            self.btn_stop.setText("Dừng")
            self._update_sel_count()

        self.undo_thread.item_processed.connect(_on_item)
        self.undo_thread.finished_undo.connect(_on_done)
        self.undo_thread.start()

    # ─────────────────────────────────────────────────────────────────
    # DIALOGS CÀI ĐẶT
    # ─────────────────────────────────────────────────────────────────
    def _open_info_dialog(self):
        dlg = QDialog(self)
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dlg.setWindowTitle("Thông tin phần mềm")
        dlg.setFixedSize(320, 160)
        lay = QVBoxLayout(dlg)
        lay.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl = QLabel(APP_TITLE); lbl.setStyleSheet("font-size: 20px; font-weight: bold; color: #3B82F6;")
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); lay.addWidget(lbl)
        lbl2 = QLabel(f"Phiên bản: {APP_VERSION}"); lbl2.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lay.addWidget(lbl2)
        lbl3 = QLabel(f"Tác giả: {APP_AUTHOR}")
        lbl3.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl3.setStyleSheet("font-weight: bold;"); lay.addWidget(lbl3)
        '''
        lbl4 = QLabel("Công cụ hỗ trợ quét, lọc và quản lý\ntệp tin thông minh tốc độ cao.")
        lbl4.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl4.setStyleSheet("color: gray; margin-top: 5px;"); lay.addWidget(lbl4)
        '''
        dlg.exec()

    def _open_kw_settings(self):
        dlg = QDialog(self)
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dlg.setWindowTitle("Cài đặt từ khóa"); dlg.setMinimumSize(500, 400)
        lay = QVBoxLayout(dlg)
        lbl = QLabel("Cài đặt từ khóa tìm kiếm")
        lbl.setStyleSheet("font-weight: bold; font-size: 14px;"); lay.addWidget(lbl)

        nl = QHBoxLayout(); nl.addWidget(QLabel("Chế độ làm gọn tìm kiếm (tăng khả năng chính xác):"))
        btn_n = QPushButton(); self._temp_kw_normalize = self._kw_normalize
        def upd_n():
            if self._temp_kw_normalize:
                btn_n.setText('BẬT ("tracnghiem")'); btn_n.setStyleSheet("background-color:#3B82F6;color:white;")
            else:
                btn_n.setText('TẮT ("trắc nghiệm")'); btn_n.setStyleSheet("background-color:gray;color:white;")
        upd_n()
        def tog_n(): self._temp_kw_normalize = not self._temp_kw_normalize; upd_n()
        btn_n.clicked.connect(tog_n); nl.addWidget(btn_n); nl.addStretch(); lay.addLayout(nl)

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        cont = QWidget(); vbox = QVBoxLayout(cont)
        gvars = {}; gents = {}; grows = {}

        def build_row(gname, kws):
            rw = QWidget(); rl = QVBoxLayout(rw); rl.setContentsMargins(0,5,0,5)
            tl = QHBoxLayout()
            cb = QCheckBox(gname); cb.setChecked(gname in self._active_kw_groups)
            cb.setStyleSheet("font-weight: bold;"); gvars[gname] = cb; tl.addWidget(cb)
            is_cust = gname not in KEYWORD_GROUPS
            lt = QLabel("Tùy chỉnh" if is_cust else "Mặc định"); lt.setStyleSheet("color:gray;")
            tl.addWidget(lt); tl.addStretch()
            if is_cust:
                bd = QPushButton("Xóa"); bd.setStyleSheet("background-color:#DC2626;color:white;")
                def del_g(gn=gname):
                    if QMessageBox.question(dlg,"Xác nhận",f"Xóa nhóm '{gn}'?") == QMessageBox.StandardButton.Yes:
                        grows[gn].hide()
                        gvars.pop(gn,None); gents.pop(gn,None); grows.pop(gn,None)
                        self._kw_all_groups.pop(gn,None)
                        if gn in self._active_kw_groups: self._active_kw_groups.remove(gn)
                bd.clicked.connect(del_g); tl.addWidget(bd)
            rl.addLayout(tl)
            ent = QLineEdit(", ".join(kws)); rl.addWidget(ent)
            gents[gname] = ent; vbox.addWidget(rw); grows[gname] = rw

        for gn, kws in self._kw_all_groups.items(): build_row(gn, kws)
        vbox.addStretch(); scroll.setWidget(cont); lay.addWidget(scroll)

        al = QHBoxLayout(); al.addWidget(QLabel("Tên nhóm mới:"))
        en = QLineEdit(); al.addWidget(en)
        ba = QPushButton("Thêm")
        def do_add():
            nm = en.text().strip()
            if nm and nm not in gvars:
                self._kw_all_groups[nm] = []; build_row(nm, []); en.clear()
        ba.clicked.connect(do_add); al.addWidget(ba); lay.addLayout(al)

        bl = QHBoxLayout()
        bs = QPushButton("Áp dụng"); bs.setStyleSheet("font-weight:bold;background-color:#3B82F6;color:white;")
        def apply():
            for gn, ent in gents.items():
                self._kw_all_groups[gn] = [k.strip() for k in ent.text().split(",") if k.strip()]
            self._active_kw_groups = [g for g, cb in gvars.items() if cb.isChecked()]
            self._kw_normalize = self._temp_kw_normalize
            self._schedule_save(); dlg.accept()
        bs.clicked.connect(apply); bl.addWidget(bs); bl.addStretch()
        bl.addWidget(QPushButton("Đóng", clicked=dlg.reject)); lay.addLayout(bl)
        dlg.exec()

    def _open_ext_settings(self):
        dlg = QDialog(self)
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dlg.setWindowTitle("Nhóm hậu tố"); dlg.setMinimumSize(400, 400)
        lay = QVBoxLayout(dlg)
        lbl = QLabel("Chọn nhóm hậu tố file")
        lbl.setStyleSheet("font-weight: bold; font-size: 14px;"); lay.addWidget(lbl)

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        cont = QWidget(); vbox = QVBoxLayout(cont)
        cb_all = QCheckBox("Tất cả các loại file (ALL)")
        cb_all.setStyleSheet("font-weight: bold; color: #3B82F6;")
        cb_all.setChecked("ALL" in self._active_ext_groups); vbox.addWidget(cb_all)

        vm = {}
        def on_all():
            if cb_all.isChecked():
                for cb in vm.values(): cb.setChecked(False)
        def on_grp():
            if any(cb.isChecked() for cb in vm.values()): cb_all.setChecked(False)
        cb_all.stateChanged.connect(on_all)

        for gn, exts in EXT_GROUPS.items():
            hl = QHBoxLayout(); cb = QCheckBox(gn); cb.setFixedWidth(120)
            cb.setChecked(gn in self._active_ext_groups and "ALL" not in self._active_ext_groups)
            cb.stateChanged.connect(on_grp); vm[gn] = cb; hl.addWidget(cb)
            le = QLabel(", ".join(exts[:6]) + ("..." if len(exts) > 6 else ""))
            le.setStyleSheet("color: gray;")
            le.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            hl.addWidget(le, 1); vbox.addLayout(hl)

        vbox.addStretch(); scroll.setWidget(cont); lay.addWidget(scroll)
        bl = QHBoxLayout()
        bs = QPushButton("Áp dụng"); bs.setStyleSheet("font-weight:bold;background-color:#3B82F6;color:white;")
        def apply():
            self._active_ext_groups = ["ALL"] if cb_all.isChecked() else [g for g, cb in vm.items() if cb.isChecked()]
            self._schedule_save(); dlg.accept()
        bs.clicked.connect(apply); bl.addWidget(bs); bl.addStretch()
        bl.addWidget(QPushButton("Đóng", clicked=dlg.reject)); lay.addLayout(bl)
        dlg.exec()

    def _open_ui_settings(self):
        dlg = QDialog(self)
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dlg.setWindowTitle("Giao diện"); dlg.setMinimumSize(100, 100)
        lay = QVBoxLayout(dlg)
        lay.addWidget(QLabel("CHẾ ĐỘ MÀU SẮC (THEME)"))
        cb = QComboBox(); cb.addItems(["System", "Light", "Dark"])
        cb.setCurrentText(self._ui_theme_mode); lay.addWidget(cb)
        lay.addStretch()
        bl = QHBoxLayout()
        bs = QPushButton("Áp dụng")
        def apply():
            self._ui_theme_mode = cb.currentText()
            self._apply_styles(); self._update_auto_btn_ui()
            self._schedule_save(); dlg.accept()
        bs.clicked.connect(apply); bl.addWidget(bs)
        bl.addWidget(QPushButton("Đóng", clicked=dlg.reject)); lay.addLayout(bl)
        dlg.exec()

    def _reset_defaults(self):
        reply = QMessageBox.question(
            self, "Xác nhận",
            "Đặt lại tất cả về mặc định?\nCửa sổ sẽ trở về chế độ toàn màn hình.")
        if reply != QMessageBox.StandardButton.Yes: return
        self._ui_theme_mode     = DEFAULT_THEME_MODE
        self._active_kw_groups  = list(DEFAULT_KW_GROUPS)
        self._active_ext_groups = list(DEFAULT_EXT_GROUPS)
        self._kw_normalize      = DEFAULT_KW_NORMALIZE
        self._kw_all_groups     = dict(KEYWORD_GROUPS)
        self._src_default       = _decode_path(DEFAULT_SRC)
        self._dst_default       = _decode_path(DEFAULT_DST)
        self._auto_select       = DEFAULT_AUTO_SELECT
        self.ent_src.setText(self._src_default); self.ent_dst.setText(self._dst_default)
        self._update_auto_btn_ui(); self._apply_styles()
        self._saved_x = 50; self._saved_y = 50; self._saved_w = 850; self._saved_h = 550
        self.setGeometry(50, 50, 850, 550); self.showMaximized()
        self._schedule_save(); self.log("Đã cài đặt lại tất cả về mặc định.", "info")

    # ─────────────────────────────────────────────────────────────────
    # LƯU TRỮ & VÒNG ĐỜI
    # ─────────────────────────────────────────────────────────────────
    def _schedule_save(self):
        self.autosave_timer.start(800)

    def _autosave_silent(self):
        self._save_settings()

    def _build_settings_dict(self) -> dict:
        if not self.isMaximized():
            x, y, w, h = self.x(), self.y(), self.width(), self.height()
        else:
            x, y, w, h = self._saved_x, self._saved_y, self._saved_w, self._saved_h
        return {
            "src":              self.ent_src.text(),
            "dst":              self.ent_dst.text(),
            "auto_select":      self._auto_select,
            "ext_groups":       self._active_ext_groups,
            "kw_groups":        self._active_kw_groups,
            "kw_normalize":     self._kw_normalize,
            "kw_custom_groups": {k: v for k, v in self._kw_all_groups.items()
                                 if k not in KEYWORD_GROUPS},
            "win_x": x, "win_y": y, "win_w": w, "win_h": h,
            "win_state": "maximized" if self.isMaximized() else "normal",
            "theme_mode": self._ui_theme_mode,
        }

    def _save_settings(self):
        # [B11-FIX] _is_closing còn True khi đóng → dùng cờ để guard.
        if self._is_closing or not self._ui_ready: return
        # [B10-FIX] Kiểm tra return value — trước đây bỏ qua → mất settings im lặng.
        if not save_settings(self._build_settings_dict()):
            self.log("⚠ Không thể lưu cài đặt (kiểm tra quyền ghi file).", "warn")

    def resizeEvent(self, event):
        if self._ui_ready and not self.isMaximized():
            self._saved_x = self.x(); self._saved_y = self.y()
            self._saved_w = self.width(); self._saved_h = self.height()
        self._schedule_save(); super().resizeEvent(event)

    def moveEvent(self, event):
        if self._ui_ready and not self.isMaximized():
            self._saved_x = self.x(); self._saved_y = self.y()
            self._saved_w = self.width(); self._saved_h = self.height()
        self._schedule_save(); super().moveEvent(event)

    def closeEvent(self, event):
        # [B11-FIX] Giữ _is_closing = True xuyên suốt — không đặt lại False.
        self._is_closing = True
        self.autosave_timer.stop()
        for worker in [self.scan_thread, self.action_thread, self.undo_thread]:
            if worker and worker.isRunning():
                worker.blockSignals(True)
                worker.is_cancelled = True
                worker.wait()
        # Gọi save_settings() trực tiếp (bypass _save_settings vì _is_closing=True).
        save_settings(self._build_settings_dict())
        event.accept()

    def _kw_summary(self):
        mode = "gọn" if self._kw_normalize else "thô"
        g    = ", ".join(self._active_kw_groups) if self._active_kw_groups else "(không)"
        return f"{g} [{mode}]"

    def _ext_summary(self):
        if "ALL" in self._active_ext_groups: return "ALL"
        return ", ".join(self._active_ext_groups) if self._active_ext_groups else "(không)"


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    window = App()
    window.show()
    sys.exit(app.exec())