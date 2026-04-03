import os
import re
import asyncio
import aiohttp
import aiofiles
import async_timeout
import pandas as pd
from tqdm.asyncio import tqdm
from pdf2image import convert_from_path
from PIL import Image
import tkinter as tk
from tkinter import filedialog
from urllib.error import HTTPError, URLError

# ===================== CẤU HÌNH =====================
BASE_URL = "https://wiki.thuvientinlanh.org/ThanhCa/TCTLVNMN/sheet"
START_ID = 1
END_ID = 903
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS-21OhiOIRVmud9Dgc1vZw02d65XHjoQKDFPxSxAmFpGopkzKnUbPMvxNb_I3QSw/pub?output=csv"
PAPER_SIZES = {"1": 3508, "2": 2480, "3": 1748}
root_folder = None  # Lưu thư mục gốc

# ===================== HỘP THOẠI =====================
def select_folder(title="Chọn thư mục"):
    root = tk.Tk()
    root.withdraw()
    folder = filedialog.askdirectory(title=title)
    root.destroy()
    return folder

def select_excel_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Chọn file Excel thay thế",
        filetypes=[("Excel files", "*.xlsx *.xls")]
    )
    root.destroy()
    return file_path

# ===================== TẢI 1 FILE =====================
async def download_file(session, url, filepath):
    try:
        async with async_timeout.timeout(30):
            async with session.get(url) as resp:
                if resp.status == 200:
                    async with aiofiles.open(filepath, "wb") as f:
                        await f.write(await resp.read())
                    return "ok"
                else:
                    return "not-found"
    except Exception:
        return "failed"

# ===================== TẢI TOÀN BỘ =====================
async def download_pdfs(target_folder):
    os.makedirs(target_folder, exist_ok=True)
    results = {"ok": 0, "not-found": 0, "failed": 0}
    not_found_links, failed_links = [], []

    async with aiohttp.ClientSession() as session:
        with tqdm(total=END_ID - START_ID + 1, desc="Đang tải", unit="file") as pbar:
            for i in range(START_ID, END_ID + 1):
                url = f"{BASE_URL}/{i}.pdf"
                # Cập nhật mô tả, luôn hiển thị trên 1 dòng cùng thanh tiến trình
                pbar.set_description(f"Đang tải: {url}")
                filepath = os.path.join(target_folder, f"{i}.pdf")
                result = await download_file(session, url, filepath)
                results[result] += 1
                if result == "not-found":
                    not_found_links.append(url)
                elif result == "failed":
                    failed_links.append(url)
                pbar.update(1)

    print("\n=== Hoàn tất tải ===")
    print(f"  Thành công: {results['ok']}")
    print(f"  Không tìm thấy: {results['not-found']}")
    print(f"  Thất bại: {results['failed']}")
    if not_found_links:
        print("\n--- Các file không tìm thấy ---")
        for link in not_found_links:
            print(link)
    if failed_links:
        print("\n--- Các file tải thất bại ---")
        for link in failed_links:
            print(link)

# ===================== ĐỔI TÊN FILE =====================
def rename_pdfs(base_folder, mode):
    print("\nĐang tải dữ liệu Google Sheets...")
    try:
        df = pd.read_csv(SHEET_URL)
    except (HTTPError, URLError, Exception) as e:
        print(f"Không thể tải dữ liệu online ({e}).")
        print("→ Mở hộp thoại để chọn file Excel thủ công.")
        excel_path = select_excel_file()
        if not excel_path:
            print("Không chọn file. Thoát chức năng đổi tên.")
            return
        df = pd.read_excel(excel_path)

    if len(df.columns) < 2:
        print("Lỗi: File dữ liệu không hợp lệ.")
        return

    # Ánh xạ dạng "TC 001" → "TÊN BÀI HÁT"
    mapping = {}
    for row in df.values:
        raw = str(row[0]).strip().upper()
        digits = re.findall(r"\d+", raw)
        if digits:
            key = f"TC {int(digits[0]):03d}"
            mapping[key] = str(row[1]).strip().upper()

    invalid_chars = r'[\\/:*?"<>|]'
    total_renamed = 0

    # Nếu là thư mục gốc
    if os.path.basename(base_folder).upper() == "THÁNH CA TIN LÀNH":
        print("Phát hiện thư mục gốc → đổi tên trong tất cả thư mục con...")
        target_folders = []
        for root, dirs, _ in os.walk(base_folder):
            for d in dirs:
                subdir = os.path.join(root, d)
                target_folders.append(subdir)
    else:
        target_folders = [base_folder]

    for folder in target_folders:
        renamed = 0
        for filename in os.listdir(folder):
            if not filename.lower().endswith((".pdf", ".jpg", ".png")):
                continue

            name_no_ext, ext = os.path.splitext(filename)
            upper_name = name_no_ext.upper()
            nums = re.findall(r"\d+", upper_name)
            if not nums:
                continue

            key = f"TC {int(nums[0]):03d}"
            if key in mapping:
                # Giữ hậu tố -TRANG n nếu có
                match_suffix = re.search(r"(-\s*TRANG\s*\d+)$", upper_name)
                suffix = match_suffix.group(1).title() if match_suffix else ""

                title = re.sub(invalid_chars, "_", mapping[key])
                new_name = f"{key} - {title}{suffix}{ext}" if mode != "1" else f"{key}{suffix}{ext}"

                old_path = os.path.join(folder, filename)
                new_path = os.path.join(folder, new_name)

                # Nếu trùng tên, thêm (1), (2)...
                counter = 1
                while os.path.exists(new_path):
                    name_only, ext2 = os.path.splitext(new_name)
                    new_name = f"{name_only} ({counter}){ext2}"
                    new_path = os.path.join(folder, new_name)
                    counter += 1

                os.rename(old_path, new_path)
                renamed += 1

        if renamed > 0:
            print(f"Đã đổi tên {renamed} file trong: {folder}")
            print(f"  → Vị trí: {folder}")
        total_renamed += renamed

    if total_renamed == 0:
        print("Không có file nào được đổi tên.")
    else:
        print(f"\nTổng cộng đã đổi tên {total_renamed} file.")
        print(f"Đã lưu tại: {base_folder}")

# ===================== XUẤT HÌNH =====================
def export_images(pdf_folder, image_folder):
    os.makedirs(image_folder, exist_ok=True)
    print("\nChọn kích thước chiều cao hình:")
    print("1: A3, 2: A4 (mặc định), 3: A5, 0: Pixel tùy chỉnh")
    choice = input("Lựa chọn: ").strip() or "2"

    if choice == "0":
        h_input = input("Nhập chiều cao hình (px, mặc định 2000): ").strip()
        target_height = int(h_input) if h_input.isdigit() else 2000
    else:
        target_height = PAPER_SIZES.get(choice, PAPER_SIZES["2"])

    fmt_choice = input("Chọn định dạng (1: JPG (mặc định), 2: PNG): ").strip() or "1"
    img_format = "JPEG" if fmt_choice == "1" else "PNG"
    ext = "jpg" if fmt_choice == "1" else "png"

    print("\nChọn chất lượng ảnh:")
    print("1: Trung bình")
    print("2: Cao (mặc định)")
    print("3: Tối đa")
    q_choice = input("Lựa chọn: ").strip() or "2"

    if q_choice == "1":
        quality = 80
    elif q_choice == "3":
        quality = 100
    else:
        quality = 90  # trung bình

    print("\nĐang xuất hình (đen trắng)...")

    pdf_files = [f for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
    with tqdm(total=len(pdf_files), desc="Đang xử lý", unit="file") as pbar:
        for filename in pdf_files:
            pdf_path = os.path.join(pdf_folder, filename)
            name_base = os.path.splitext(filename)[0]
            pbar.set_description(f"Đang xử lý: {filename[:6]}")

            try:
                pages = convert_from_path(pdf_path, dpi=300)
                for idx, page in enumerate(pages, start=1):
                    page = page.convert("L")
                    w, h = page.size
                    scale = target_height / h
                    new_size = (int(w * scale), target_height)
                    page = page.resize(new_size, Image.LANCZOS)
                    suffix = f" - trang {idx}" if len(pages) > 1 else ""
                    out_name = f"{name_base}{suffix}.{ext}"

                    save_params = {"format": img_format}
                    if img_format == "JPEG":
                        save_params["quality"] = quality
                        save_params["optimize"] = True

                    page.save(os.path.join(image_folder, out_name), **save_params)

            except Exception as e:
                print(f"\nLỗi khi xử lý {filename}: {e}")

            pbar.update(1)

    print(f"\nĐã xuất toàn bộ hình. Lưu tại: {image_folder}")

# ===================== MENU CHÍNH =====================
async def main():
    global root_folder

    while True:
        print("\n=== MENU CHÍNH ===")
        print("1: Tải PDF Thánh Ca")
        print("2: Đổi tên hàng loạt")
        print("3: Tạo hình từ PDF")
        print("0: Thoát")

        choice = input("Chọn chức năng: ").strip()

        if choice == "1":
            base_folder = select_folder("Chọn nơi lưu thư mục 'Thánh Ca Tin Lành'")
            if not base_folder:
                continue
            root_folder = os.path.join(base_folder, "Thánh Ca Tin Lành")
            pdf_folder = os.path.join(root_folder, "PDF")
            os.makedirs(pdf_folder, exist_ok=True)
            await download_pdfs(pdf_folder)
            print(f"Đã tải xong, lưu tại: {pdf_folder}")

        elif choice == "2":
            if root_folder and os.path.exists(root_folder):
                target_folder = root_folder
            else:
                target_folder = select_folder("Chọn thư mục chứa PDF hoặc thư mục gốc 'Thánh Ca Tin Lành'")
            if not target_folder:
                continue

            print("\nChọn kiểu đổi tên:")
            print("1: Chỉ số bài hát (TC 001, TC 002...)")
            print("2: Số và tiêu đề (TC 001 - TÊN BÀI HÁT...) (mặc định)")
            mode = input("Chọn: ").strip() or "2"

            rename_pdfs(target_folder, mode)

        elif choice == "3":
            if not root_folder or not os.path.exists(root_folder):
                pdf_folder = select_folder("Chọn thư mục PDF cần xuất hình")
                image_folder = os.path.join(os.path.dirname(pdf_folder), "HÌNH")
            else:
                pdf_folder = os.path.join(root_folder, "PDF")
                image_folder = os.path.join(root_folder, "HÌNH")

            if not pdf_folder:
                continue
            export_images(pdf_folder, image_folder)

        elif choice == "0":
            print("Thoát chương trình.")
            break
        else:
            print("Lựa chọn không hợp lệ.")

# ===================== CHẠY CHƯƠNG TRÌNH =====================
if __name__ == "__main__":
    asyncio.run(main())
