# 2. 파이썬 코드 작업: src 폴더 안에 extract.py 파일 생성
# Cursor IDE 좌측 탐색기에서 src 폴더를 우클릭하고 'New File'을 눌러 'extract.py'를 만드게.
# (이유: 향후 어떤 파일이든 이 코드 하나만 통과하면 안전하게 불러올 수 있도록 '추출 전용 기계'를 만드는 과정입니다.)
# 아래 코드를 extract.py에 그대로 복사해서 붙여넣게.

import pandas as pd
import os
from logger import logger

# 예약 리스트 기본 파일명 (실제 파일은 .xlsx)
DEFAULT_RESERVATION_FILENAME = "Reservation List_20260301.xlsx"
RELATIVE_RAW_DIR = "data/raw"
HOTEL_RAW_SUBDIR = "data/raw/hotel"
HOTEL_RES_LIST_FILENAME = "res_list_2024.csv"
HOTEL_RES_LIST_XLSX = "Reservation List_20260301.xlsx"  # 호텔 폴더에서 xlsx 직접 사용 가능

# merged_hotel_for_review 저장 시 남길 컬럼 (이외 삭제)
MERGED_REVIEW_COLUMNS = [
    "Rsvn No", "Arr Date", "Dep Date", "Nts", "Rm Ty", "Rms",
    "Total Amount", "Ra Ty", "Ma Ty", "Nat", "Visit", "ADR", "account", "Account", "ACCOUNT",
]

# 개인정보 컬럼 (호텔 CSV 로드 시 즉시 삭제)
PII_COLUMNS_HOTEL = ["고객명", "전화번호", "이메일", "Guest Name", "Phone", "E-Mail", "Mobile", "연락처", "이름"]

# 호텔 핵심 데이터 6컬럼만 유지 (그 외 id, name, email, phone, city, company 등 모두 삭제)
HOTEL_CANONICAL_COLS = ["예약번호", "입실일", "퇴실일", "객실타입", "예약경로(Source)", "객실료(Rate)"]
HOTEL_COL_MAPPING = {
    "예약번호": ["Rsvn No", "예약번호", "Reservation No", "Booking ID", "Res No", "transaction_id"],
    "입실일": ["Arr Date", "입실일", "체크인", "체크인일", "Check-in", "Check-in Date", "check_in"],
    "퇴실일": ["Dep Date", "퇴실일", "체크아웃", "Check-out", "Check-out Date", "check_out"],
    "객실타입": ["Rm Ty", "객실타입", "Room Type", "객실", "Room", "room_type"],
    "예약경로(Source)": ["Nat", "Region", "예약경로(Source)", "예약경로", "Source", "source", "OTA"],
    "객실료(Rate)": ["Room Rate", "Total Amount", "객실료(Rate)", "실객실료", "객실료", "room_rate", "금액"],
}


def _find_col(candidates: list, columns) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


def _slim_to_canonical_hotel(df: pd.DataFrame) -> pd.DataFrame:
    """오직 핵심 6컬럼만 남기고 나머지 영구 삭제."""
    result = pd.DataFrame()
    for canon, candidates in HOTEL_COL_MAPPING.items():
        src = _find_col(candidates, df.columns)
        if src is not None:
            result[canon] = df[src].values
        else:
            result[canon] = None
    return result


def _get_raw_path(filename: str) -> str:
    """
    Ryun/data/raw 경로를 반환합니다.
    (extract.py가 enterprise_data_pipeline 안에 있으므로, 상위 폴더(Ryun) 기준으로 data/raw 사용)
    data/raw 폴더가 없으면 생성합니다.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_root = os.path.dirname(script_dir)  # Ryun
    raw_dir = os.path.join(data_root, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    return os.path.join(raw_dir, filename)


def extract_data(file_path: str) -> pd.DataFrame:
    """
    지정된 경로에서 CSV 또는 Excel(.xlsx) 데이터를 읽어 DataFrame으로 반환합니다.
    """
    logger.info(f"데이터 추출을 시작합니다: {file_path}")

    if not os.path.exists(file_path):
        logger.error(f"오류: '{file_path}' 경로에 데이터가 존재하지 않습니다.")
        raise FileNotFoundError(f"오류: '{file_path}' 경로에 데이터가 존재하지 않습니다.")

    low = file_path.lower()
    if low.endswith(".xlsx") or low.endswith(".xls"):
        # 엑셀: 1행 제목, 2행 필터 설명, 3행이 실제 컬럼명인 경우가 많음
        df = pd.read_excel(file_path, engine="openpyxl", header=2)
    else:
        df = pd.read_csv(file_path, encoding="utf-8-sig")

    logger.info(f"추출 완료! 총 {len(df)}개의 행(Row)을 가져왔습니다.")
    return df


def _get_hotel_dir() -> str:
    """Ryun/data/raw/hotel 경로. 폴더 없으면 생성."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_root = os.path.dirname(script_dir)
    hotel_dir = os.path.join(data_root, "data", "raw", "hotel")
    os.makedirs(hotel_dir, exist_ok=True)
    return hotel_dir


def _get_hotel_raw_path(filename: str) -> str:
    hotel_dir = _get_hotel_dir()
    return os.path.join(hotel_dir, filename)


def _read_one_hotel_xlsx(path: str) -> pd.DataFrame:
    """단일 xlsx를 WINGS 형식(header=2)으로 읽음."""
    return pd.read_excel(path, engine="openpyxl", header=2)


def _drop_embedded_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """중간에 끼어든 헤더와 동일한 행 제거 (첫 행은 컬럼명이므로 데이터 행만 검사)."""
    if df.empty:
        return df
    # 첫 행이 컬럼명과 완전히 같은 데이터 행이 있으면 제거
    cols = list(df.columns)
    mask = df.astype(str).apply(lambda row: list(row) != [str(c) for c in cols], axis=1)
    n_before = len(df)
    df = df[mask].reset_index(drop=True)
    if len(df) < n_before:
        logger.info(f"중복 헤더 행 제거: {n_before - len(df)}건")
    return df


def _merge_all_hotel_xlsx(hotel_dir: str) -> pd.DataFrame:
    """
    Merge all xlsx in data/raw/hotel/ into one DataFrame.
    - Merge basis: first file's column structure (header). Rows are concatenated; no key column.
    - Result is saved to data/processed/merged_hotel_for_review_YYYYMMDD_HHMMSS.xlsx (see extract_hotel_res_list).
    - Downsides of "no key" merge: (1) Duplicate rows if files overlap → specify MERGE_DEDUP_COLUMNS to deduplicate.
      (2) Different column names in other files → specify column mapping so we normalize before merge.
    """
    xlsx_files = sorted([f for f in os.listdir(hotel_dir) if f.lower().endswith(".xlsx")])
    if not xlsx_files:
        return None

    logger.info(f"Merge target: {len(xlsx_files)} xlsx file(s)")

    first_cols = None
    frames = []

    for name in xlsx_files:
        path = os.path.join(hotel_dir, name)
        try:
            df = _read_one_hotel_xlsx(path)
        except Exception as e:
            logger.warning(f"  Skip {name}: {e}")
            continue

        if df.empty:
            logger.info(f"  {name}: empty sheet, skip")
            continue

        cols = list(df.columns)
        if first_cols is None:
            first_cols = cols
        else:
            if cols != first_cols:
                missing = [c for c in first_cols if c not in df.columns]
                if missing:
                    logger.warning(f"  {name}: missing columns vs first file: {missing} (filled with NaN)")
                df = df.reindex(columns=first_cols)
            else:
                df = df[first_cols]

        df = _drop_embedded_header_rows(df)
        df["_source_file"] = name
        frames.append(df)
        logger.info(f"  {name}: {len(df)} rows")

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop(columns=["_source_file"], errors="ignore")
    logger.info(f"Merge complete: {len(merged)} rows total")
    logger.info(f"Merge basis: First file's column structure. Columns: {list(first_cols)}. Rows concatenated (no key).")
    logger.info(f"Merged Excel will be saved as merged_hotel_for_review_*.xlsx in data/processed/")
    return merged


def extract_hotel_res_list(filename: str | None = None) -> pd.DataFrame:
    """
    data/raw/hotel/ 아래에서 호텔 예약 데이터를 읽어 DataFrame으로 반환합니다.
    - filename이 없으면: 폴더 내 모든 xlsx를 읽어 병합(Merge First). 컬럼 구조 동일 여부 체크, 중복 헤더 행 제거.
    - filename을 주면: 해당 파일만 읽습니다.
    개인정보 컬럼은 즉시 drop, 핵심 6컬럼만 유지합니다.
    """
    hotel_dir = _get_hotel_dir()

    if filename:
        # Single file
        file_path = os.path.join(hotel_dir, filename)
        if not os.path.exists(file_path):
            logger.error(f"Hotel data file not found: {file_path}")
            raise FileNotFoundError(file_path)
        logger.info(f"Hotel list (single file): {file_path}")
        low = file_path.lower()
        if low.endswith(".xlsx") or low.endswith(".xls"):
            df = pd.read_excel(file_path, engine="openpyxl", header=2)
        else:
            df = pd.read_csv(file_path, encoding="utf-8-sig")
        logger.info(f"Extract complete: {len(df)} rows loaded.")
        pii_found = [c for c in PII_COLUMNS_HOTEL if c in df.columns]
        if pii_found:
            df = df.drop(columns=pii_found)
            logger.info(f"PII columns dropped: {pii_found}")
        df = _slim_to_canonical_hotel(df)
        logger.info(f"Slimmed to 6 columns: {list(df.columns)}")
        return df
    else:
        # Merge: all xlsx in folder → 1 DataFrame
        df = _merge_all_hotel_xlsx(hotel_dir)
        if df is None:
            for name in [HOTEL_RES_LIST_XLSX, HOTEL_RES_LIST_FILENAME]:
                path = os.path.join(hotel_dir, name)
                if os.path.exists(path):
                    logger.info(f"No xlsx found, using single file: {path}")
                    if path.lower().endswith(".xlsx") or path.lower().endswith(".xls"):
                        df = pd.read_excel(path, engine="openpyxl", header=2)
                    else:
                        df = pd.read_csv(path, encoding="utf-8-sig")
                    break
            if df is None:
                logger.error("No hotel data file in data/raw/hotel/")
                raise FileNotFoundError("No hotel data file in data/raw/hotel/")

        logger.info(f"Extract complete: {len(df)} rows loaded.")

        # Save merge-only file (all columns) for review
        _save_merged_for_review(df, "merged_hotel_for_review")

        # For pipeline: PII drop + slim to 6 columns
        pii_found = [c for c in PII_COLUMNS_HOTEL if c in df.columns]
        if pii_found:
            df = df.drop(columns=pii_found)
            logger.info(f"PII columns dropped (for pipeline): {pii_found}")
        df = _slim_to_canonical_hotel(df)
        logger.info(f"Pipeline: slimmed to 6 columns: {list(df.columns)}")
        return df


def _get_processed_dir() -> str:
    """Ryun/data/processed 경로. 폴더 없으면 생성."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_root = os.path.dirname(script_dir)
    out_dir = os.path.join(data_root, "data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def _save_merged_for_review(df: pd.DataFrame, prefix: str = "merged_hotel_for_review") -> None:
    """Save merge result to Excel. Only MERGED_REVIEW_COLUMNS (noise removed)."""
    from datetime import datetime
    keep = [c for c in MERGED_REVIEW_COLUMNS if c in df.columns]
    out_df = df[keep].copy() if keep else df
    out_dir = _get_processed_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"{prefix}_{ts}.xlsx")
    out_df.to_excel(path, index=False, engine="openpyxl")
    logger.info(f"Saved for review ({len(keep)} columns): {path}")


def extract_reservation_list(filename: str = DEFAULT_RESERVATION_FILENAME) -> pd.DataFrame:
    """
    Ryun/data/raw/ 아래의 예약 리스트 파일을 읽어 DataFrame으로 반환합니다.
    예: Reservation List_20260301.xlsx (또는 .csv)
    """
    file_path = _get_raw_path(filename)
    logger.info(f"예약 리스트 추출: {file_path}")
    return extract_data(file_path)


if __name__ == "__main__":
    try:
        df = extract_reservation_list()
        print("컬럼:", list(df.columns))
        print(df.head())
    except FileNotFoundError:
        logger.error("Ryun/data/raw 아래에 Reservation List_20260301.xlsx(또는 .csv)가 없습니다.")