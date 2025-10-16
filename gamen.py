import os
import re
import warnings
from pathlib import Path
from typing import Dict, Optional, Tuple, Union, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from openpyxl import load_workbook

# Ẩn cảnh báo Data Validation của openpyxl
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="openpyxl",
    message=".*Data Validation extension is not supported.*",
)

# =========================
# ====== CẤU HÌNH =========
# =========================
FE_FOLDER   = r"D:\Documents\027-社内レビュー\モック開発\海外\ベトナムチーム"
BE_FOLDER   = r"D:\Documents\027-社内レビュー\WEBAPI開発\海外\ベトナム"
TC_FOLDER   = r"D:\Documents\90.作業情報\50.作業報告\KMD（ケアマネ先行開発）\成果物\単体テスト仕様"
# EXEC_FOLDER = r"D:\Documents\90.作業情報\50.作業報告\KMD（ケアマネ先行開発）\成果物\単体テスト実施"

SUMMARY_FILE = r"C:\Users\KDVN-ANHNC\Documents\tool\SoukeiTool\画面_summary.xlsx"
OUTPUT_FILE = "Screen_LOC_Summary.xlsx"
ERROR_LOG_FILE = "collect_and_fill_error_log.txt"

# Hiệu năng
MAX_WORKERS = max(4, (os.cpu_count() or 4))
PROGRESS_EVERY = 25
VERBOSE = False

def vprint(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)

# =========================
# ====== HÀM PHỤ ==========
# =========================
ERROR_STRINGS = {"#REF!", "#DIV/0!", "#NAME?", "#NULL!", "#NUM!", "#N/A", "#VALUE!"}
GUI_RE = re.compile(r"(GUI\d{5})")

def normalize_name(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip().replace("\u3000", " ")
    return re.sub(r"\s+", " ", s)

def is_error_value(val) -> bool:
    return isinstance(val, str) and val.strip().upper() in ERROR_STRINGS

def coerce_number(v) -> Optional[float]:
    if v is None: return None
    if isinstance(v, str):
        t = v.strip()
        if is_error_value(t): return None
        m = re.search(r"[-+]?\d[\d,\.]*", t)
        if not m: return None
        try: return float(m.group(0).replace(",", ""))
        except: return None
    try: return float(v)
    except: return None

def list_excel_files(folder: str):
    p = Path(folder)
    for f in p.rglob("*"):
        if f.is_file() and f.suffix.lower() in {".xlsx", ".xlsm"} and not f.name.startswith("~$"):
            yield str(f)

def extract_gui_from_filename(path: str) -> Optional[str]:
    m = GUI_RE.search(Path(path).name)
    return m.group(1) if m else None

def try_pick_sheet_name(path: str, preferred: str | None):
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
        sheets = wb.sheetnames
        name = preferred if preferred in sheets else sheets[0]
        wb.close()
        return name
    except Exception:
        return preferred or "Sheet1"

def get_cell_value_fast(path: str, sheet_name: str, addr: str):
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
        val = ws[addr].value
        wb.close()
        if is_error_value(val) or val is not None:
            return val
        wb2 = load_workbook(path, data_only=False, read_only=True)
        ws2 = wb2[sheet_name] if sheet_name in wb2.sheetnames else wb2.active
        val2 = ws2[addr].value
        wb2.close()
        if isinstance(val2, str) and ("#REF!" in val2 or is_error_value(val2)):
            return "#REF!"
        return val2
    except Exception:
        return None

def read_name_and_loc(path: str):
    sheet = try_pick_sheet_name(path, "レビュー依頼書兼報告書")
    name_v = get_cell_value_fast(path, sheet, "G5")
    au6 = get_cell_value_fast(path, sheet, "AU6")
    av6 = None if au6 is not None else get_cell_value_fast(path, sheet, "AV6")
    name = normalize_name(name_v)
    if is_error_value(au6): loc = "#REF!"
    elif au6 is not None: loc = coerce_number(au6) or str(au6)
    elif is_error_value(av6): loc = "#REF!"
    elif av6 is not None: loc = coerce_number(av6) or str(av6)
    else: loc = None
    return name, loc

def find_total_cases_fast(path: str, sheet_name: str):
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
        for row in ws.iter_rows(values_only=True):
            if any(isinstance(v, str) and "総ケース数" in v for v in row or []):
                nums = [coerce_number(v) for v in row if coerce_number(v) is not None]
                wb.close()
                return nums[0] if len(set(nums)) == 1 else None
        wb.close()
    except Exception:
        pass
    return None

def read_TC_from_spec(path: str):
    rev_sheet = try_pick_sheet_name(path, "改訂履歴")
    sum_sheet = try_pick_sheet_name(path, "評価項目サマリ")
    screen_raw = get_cell_value_fast(path, rev_sheet, "AG1")
    screen_name = normalize_name(screen_raw) or extract_gui_from_filename(path)
    tc = find_total_cases_fast(path, sum_sheet)
    if tc is None:
        f5 = get_cell_value_fast(path, sum_sheet, "F5")
        tc = coerce_number(f5) or (str(f5) if f5 else None)
    return screen_name, tc

def find_ng_sum_fast(path: str, sheet_name: str, header_rows=7):
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
        for r in range(1, header_rows + 1):
            for c in range(1, ws.max_column + 1):
                if isinstance(ws.cell(r, c).value, str) and "NGケース数" in ws.cell(r, c).value:
                    total = sum(filter(None, [coerce_number(ws.cell(r+i, c).value) for i in range(1, 6)]))
                    wb.close()
                    return total
        wb.close()
    except Exception:
        pass
    return None

def read_exec_ngsum(path: str):
    rev_sheet = try_pick_sheet_name(path, "改訂履歴")
    sum_sheet = try_pick_sheet_name(path, "評価項目サマリ")
    screen_raw = get_cell_value_fast(path, rev_sheet, "AG1")
    screen_name = normalize_name(screen_raw) or extract_gui_from_filename(path)
    return screen_name, find_ng_sum_fast(path, sum_sheet)

def load_target_screens(summary_path: str) -> List[str]:
    try:
        import pandas as pd
        df = pd.read_excel(summary_path)
        col = next((c for c in df.columns if c in ["Tên màn hình","Screen","画面","画面ID"]), df.columns[0])
        return [normalize_name(x) for x in df[col].dropna().tolist()]
    except Exception as e:
        print(f"[WARN] Không đọc được SUMMARY_FILE: {e}")
        return []


# =========================
# ====== WORKERS ==========
# =========================
def _worker_fe(path: str):
    try:
        name, loc = read_name_and_loc(path)
        if name is None and loc is None:
            return path, (name, loc), "FE: Không đọc được G5/AU6/AV6"
        return path, (name, loc), None
    except Exception as e:
        return path, (None, None), f"FE EXC: {type(e).__name__}: {e}"

def _worker_be(path: str):
    try:
        name, loc = read_name_and_loc(path)
        if name is None and loc is None:
            return path, (name, loc), "BE: Không đọc được G5/AU6/AV6"
        return path, (name, loc), None
    except Exception as e:
        return path, (None, None), f"BE EXC: {type(e).__name__}: {e}"

def _worker_tc(path: str):
    try:
        screen, tc = read_TC_from_spec(path)
        if screen is None and tc is None:
            return path, (screen, tc), "TC: Không đọc được AG1/総ケース数/F5"
        return path, (screen, tc), None
    except Exception as e:
        return path, (None, None), f"TC EXC: {type(e).__name__}: {e}"

def _worker_exec(path: str):
    try:
        screen, ngsum = read_exec_ngsum(path)
        if screen is None and ngsum is None:
            return path, (screen, ngsum), "EXEC: Không đọc được AG1/NGケース数"
        return path, (screen, ngsum), None
    except Exception as e:
        return path, (None, None), f"EXEC EXC: {type(e).__name__}: {e}"
# =========================
# ===== LUỒNG CHÍNH =======
# =========================
import time

def run_parallel(stage_name: str, files: List[str], worker, max_workers: int = MAX_WORKERS) -> List[tuple]:
    """
    Chạy song song các worker trên danh sách files với progress bar.
    Trả về list các tuple do worker trả (path, (val1, val2), err).
    """
    results = []
    total = len(files)
    if total == 0:
        print(f"⚠️  {stage_name}: Không có file để xử lý\n")
        return results

    print(f"=== {stage_name} ({total} files) ===")
    start = time.time()
    done = 0
    last_percent = -1

    bar_len = 24
    def print_progress():
        nonlocal last_percent
        percent = int(done * 100 / total)
        if percent != last_percent or done == total:
            filled = int(bar_len * percent / 100)
            bar = "#" * filled + "-" * (bar_len - filled)
            print(f"\r    [{bar}] {percent}% ({done}/{total})", end="", flush=True)
            last_percent = percent

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(worker, f): f for f in files}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                # đảm bảo không làm vỡ tiến trình khi 1 task lỗi
                results.append((futures[fut], (None, None), f"EXC: {type(e).__name__}: {e}"))
            finally:
                done += 1
                if (done % PROGRESS_EVERY == 0) or done == total:
                    print_progress()

    print_progress()
    elapsed = time.time() - start
    print(f"\n✅ {stage_name} done ({total} files) in {elapsed:.1f}s\n")
    return results

def main():
    import pandas as pd

    result: Dict[str, Dict[str, Union[float, str, None]]] = {}
    errors: List[str] = []

    # 1) Đọc danh sách màn hình mục tiêu
    target_screens = load_target_screens(SUMMARY_FILE)
    target_set = set(target_screens)
    print(f"🎯 Mục tiêu: {len(target_screens)} màn hình")

    if not target_screens:
        print("❌ Không có danh sách 'Tên màn hình' trong SUMMARY_FILE — dừng.")
        return

    # Lọc file theo GUIxxxxx trong target_set
    def filter_target_files(folder):
        all_files = list(list_excel_files(folder))
        filtered = [f for f in all_files if (extract_gui_from_filename(f) or "") in target_set]
        return filtered, len(all_files)

    fe_files, fe_all = filter_target_files(FE_FOLDER)
    be_files, be_all = filter_target_files(BE_FOLDER)
    tc_files, tc_all = filter_target_files(TC_FOLDER)
    # exec_files, exec_all = filter_target_files(EXEC_FOLDER)

    print(f"📂 FE: {len(fe_files)}/{fe_all} | BE: {len(be_files)}/{be_all} | TC: {len(tc_files)}/{tc_all} |")

    # Khởi tạo result cho tất cả màn mục tiêu
    for scr in target_screens:
        result[scr] = {"LOCFE": None, "LOCBE": None, "TestCase": None, "NGケース数_5行合計": None}

    # 2) FE
    for p, (n, loc), err in run_parallel("Bước 1: FE", fe_files, _worker_fe):
        if n in target_set:
            # giữ bản ghi đầu tiên (tránh trùng), không ghi đè nếu đã có
            if result[n].get("LOCFE") is None:
                result[n]["LOCFE"] = loc
        if err:
            errors.append(f"[FE] {p}: {err}")

    # 3) BE
    for p, (n, loc), err in run_parallel("Bước 2: BE", be_files, _worker_be):
        if n in target_set:
            if result[n].get("LOCBE") is None:
                result[n]["LOCBE"] = loc
        if err:
            errors.append(f"[BE] {p}: {err}")

    # 4) TC
    for p, (n, tc), err in run_parallel("Bước 3: 単体テスト仕様 (TC)", tc_files, _worker_tc):
        if n in target_set:
            result[n]["TestCase"] = tc
        if err:
            errors.append(f"[TC] {p}: {err}")

    # # 5) EXEC
    # for p, (n, ngsum), err in run_parallel("Bước 4: 単体テスト実施 (EXEC)", exec_files, _worker_exec):
    #     if n in target_set:
    #         result[n]["NGケース数_5行合計"] = ngsum
    #     if err:
    #         errors.append(f"[EXEC] {p}: {err}")

    # 6) Xuất Excel
    print("📊 Đang ghi file kết quả ...")
    df = pd.DataFrame([
        {"Tên màn hình": n,
         "LOCFE": result.get(n, {}).get("LOCFE"),
         "LOCBE": result.get(n, {}).get("LOCBE"),
         "TestCase": result.get(n, {}).get("TestCase"),
        #  "NGケース数_5行合計": result.get(n, {}).get("NGケース数_5行合計")
         }
        for n in sorted(target_screens)
    ])
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Đã tạo: {OUTPUT_FILE}")

    # 7) Ghi log lỗi
    if errors:
        with open(ERROR_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(errors))
        print(f"⚠️  Có {len(errors)} lỗi, xem {ERROR_LOG_FILE}")
    else:
        print("✅ Không có lỗi nào được ghi nhận.")

if __name__ == "__main__":
    main()
