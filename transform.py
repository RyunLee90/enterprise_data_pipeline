# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\transform.py

import pandas as pd
from logger import setup_logger

logger = setup_logger()

# ----- 주가 데이터용 (기존) -----
KEEP_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]

# ----- WINGS 예약 → DB 규격 컬럼 매핑 (가능한 원본 컬럼명 후보) -----
WINGS_TO_DB = {
    "transaction_id": ["Rsvn No", "예약번호", "Reservation No", "Booking ID", "Res No", "Confirmation No", "transaction_id"],
    "check_in": ["Arr Date", "체크인", "체크인일", "Check-in", "Check-in Date", "Check In", "Arrival", "도착일", "check_in"],
    "check_out": ["Dep Date", "체크아웃", "체크아웃일", "Check-out", "Check-out Date", "Departure", "출발일", "check_out"],
    "room_type": ["Rm Ty", "객실타입", "Room Type", "객실", "Room", "RoomType", "room_type"],
    "source_market": ["Nat", "Region", "시장", "유통경로", "Source", "Market", "Channel", "출처", "Source Market", "판매채널", "source_market"],
    "rate": ["Room Rate", "Total Amount", "요금", "Rate", "Amount", "금액", "총액", "Room Revenue", "Total", "객실요금", "rate", "amount"],
    "status": ["Sts", "상태", "Status", "예약상태", "Reservation Status", "status"],
}

# 개인정보 컬럼 (즉시 삭제)
PII_COLUMNS = ["고객명", "전화번호", "Guest Name", "Phone", "연락처", "이름", "전화", "Mobile", "E-Mail", "Caller", "Caller Tel"]

# 취소 상태 표기
CANCELLED_STATUS = "Cancelled(취소)"

# ----- 호텔 res_list 필터/컬럼 후보 -----
HOTEL_MARKET_CODE_COLS = ["마켓코드", "Market Code", "마켓", "Market", "market_code", "Nat", "Region"]
HOTEL_ROOM_RATE_COLS = ["실객실료", "Room Rate", "객실료", "실객실요금", "room_rate", "Total Amount", "Room Rate"]
HOTEL_EXCLUDE_MARKETS = ["GRP", "MICE"]
HOTEL_SOURCE_COLS = ["예약경로(Source)", "예약경로", "Source", "source", "OTA", "예약채널", "Nat", "Region"]
HOTEL_CHECKIN_COLS = ["입실일", "체크인", "Check-in", "Arr Date", "check_in", "Arrival"]
HOTEL_ROOM_TYPE_COLS = ["객실타입", "Room Type", "Rm Ty", "room_type", "객실"]
HOTEL_REVENUE_COLS = ["매출액", "Revenue", "Total Amount", "실객실료", "Room Rate", "금액"]


def _find_column(candidates: list, columns: pd.Index) -> str | None:
    """후보 중 DataFrame에 존재하는 첫 번째 컬럼명을 반환합니다."""
    for c in candidates:
        if c in columns:
            return c
    return None


def _map_wings_to_db(df: pd.DataFrame) -> pd.DataFrame:
    """WINGS 컬럼명을 DB 규격(transaction_id, check_in, room_type, source_market 등)으로 변환합니다."""
    result = pd.DataFrame()
    cols_used = []

    for db_col, candidates in WINGS_TO_DB.items():
        src = _find_column(candidates, df.columns)
        if src is not None:
            result[db_col] = df[src].values
            cols_used.append(src)

    # 매핑되지 않은 나머지 컬럼 중 PII가 아닌 것은 유지할지 선택 가능 (여기서는 매핑된 것만 사용)
    return result


def transform_reservation_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    WINGS 예약 리스트 데이터를 정제하고 DB 규격으로 변환합니다.
    - 객실 요금(Rate 또는 Amount)이 0원인 행 제외
    - 예약 상태가 'Cancelled(취소)'인 행 제외
    - 고객명, 전화번호 등 개인정보 컬럼 삭제
    - WINGS 컬럼명 → transaction_id, check_in, room_type, source_market 등으로 매핑
    """
    logger.info("예약 데이터 변환(Transform)을 시작합니다...")
    processed = df.copy()

    # 1. 개인정보 컬럼 즉시 삭제
    pii_found = [c for c in PII_COLUMNS if c in processed.columns]
    if pii_found:
        processed = processed.drop(columns=pii_found)
        logger.info(f"개인정보 컬럼 삭제: {pii_found}")

    # 2. 요금 컬럼 찾기 (Rate 또는 Amount)
    rate_col = _find_column(WINGS_TO_DB["rate"], processed.columns)
    if rate_col is not None:
        processed[rate_col] = pd.to_numeric(processed[rate_col], errors="coerce")
        before = len(processed)
        processed = processed[processed[rate_col].fillna(0) > 0]
        logger.info(f"객실 요금 0원 행 제외: {before - len(processed)}건 제거")
    else:
        logger.warning("요금(Rate/Amount) 컬럼을 찾을 수 없어 0원 제외 로직을 건너뜁니다.")

    # 3. 예약 상태가 'Cancelled(취소)'인 데이터 제외 (삭제)
    status_col = _find_column(WINGS_TO_DB["status"], processed.columns)
    if status_col is not None:
        before = len(processed)
        processed = processed[processed[status_col].astype(str).str.strip() != CANCELLED_STATUS]
        logger.info(f"취소(Cancelled) 예약 제외: {before - len(processed)}건 제거")
    else:
        logger.warning("상태(Status) 컬럼을 찾을 수 없어 취소 제외 로직을 건너뜁니다.")

    # 4. WINGS 컬럼명 → DB 규격 매핑
    processed = _map_wings_to_db(processed)
    if processed.empty:
        logger.warning("매핑 후 데이터가 비었습니다. 원본 CSV 컬럼명을 확인하세요.")

    logger.info(f"예약 변환 완료! 최종 컬럼: {list(processed.columns)}, 총 {len(processed)}건")
    return processed


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    추출된 주가 데이터를 Pandas DataFrame 형태로 정리합니다.
    Date, Open, High, Low, Close, Volume, Ticker 컬럼을 유지합니다.
    (주가용 — 예약 데이터는 transform_reservation_data 사용)
    """
    logger.info("데이터 변환(Transform)을 시작합니다...")

    processed_df = df.copy()

    # Date를 datetime으로 통일
    if "Date" in processed_df.columns:
        processed_df["Date"] = pd.to_datetime(processed_df["Date"])
        if hasattr(processed_df["Date"].dt, "tz_localize") and processed_df["Date"].dt.tz is not None:
            processed_df["Date"] = processed_df["Date"].dt.tz_localize(None)

    # 필요한 컬럼만 유지 (없는 컬럼은 제외)
    existing = [c for c in KEEP_COLUMNS if c in processed_df.columns]
    processed_df = processed_df[existing]

    # 수치 컬럼 정리
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in processed_df.columns:
            processed_df[col] = pd.to_numeric(processed_df[col], errors="coerce")
    processed_df = processed_df.dropna(subset=["Close", "Volume"], how="all")

    logger.info(f"변환 완료! 최종 컬럼: {list(processed_df.columns)}")
    logger.info(f"총 {len(processed_df)}건 변환 성공")

    return processed_df


def transform_hotel_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    호텔 슬림 데이터(핵심 6컬럼) 정제.
    - 예약경로(Source)가 'GRP', 'MICE'인 행 제거
    - 객실료(Rate)가 0원 이하인 행 제거
    - 퇴실일 < 입실일인 데이터 오류 행 제거
    - 입실일 기준 '요일', '월' 컬럼 생성
    """
    logger.info("호텔 예약 데이터 변환(Transform)을 시작합니다...")
    processed = df.copy()

    # 컬럼명: 슬림화 후 한글 핵심명 사용
    col_rvno = "예약번호"
    col_ci = "입실일"
    col_co = "퇴실일"
    col_room = "객실타입"
    col_src = "예약경로(Source)"
    col_rate = "객실료(Rate)"

    for c in [col_rvno, col_ci, col_co, col_room, col_src, col_rate]:
        if c not in processed.columns:
            logger.warning(f"핵심 컬럼 없음: {c}. extract 단계에서 6컬럼만 나왔는지 확인하세요.")
            return processed

    # 1. 예약경로(Source)가 GRP, MICE인 행 제거
    processed[col_src] = processed[col_src].astype(str).str.strip().str.upper()
    before = len(processed)
    processed = processed[~processed[col_src].isin(HOTEL_EXCLUDE_MARKETS)]
    logger.info(f"마켓코드 GRP/MICE 제거: {before - len(processed)}건")

    # 2. 객실료가 0원 이하인 행 제거
    processed[col_rate] = pd.to_numeric(processed[col_rate], errors="coerce")
    before = len(processed)
    processed = processed[processed[col_rate].fillna(0) > 0]
    logger.info(f"객실료 0원 이하 행 제거: {before - len(processed)}건")

    # 3. 퇴실일 < 입실일인 데이터 오류 행 제거 (둘 다 유효한 날짜일 때만 비교)
    processed["_dt_ci"] = pd.to_datetime(processed[col_ci], errors="coerce")
    processed["_dt_co"] = pd.to_datetime(processed[col_co], errors="coerce")
    both_valid = processed["_dt_ci"].notna() & processed["_dt_co"].notna()
    invalid_order = both_valid & (processed["_dt_co"] < processed["_dt_ci"])
    before = len(processed)
    processed = processed[~invalid_order].drop(columns=["_dt_ci", "_dt_co"])
    logger.info(f"퇴실일<입실일 오류 행 제거: {invalid_order.sum()}건")

    # 4. 입실일 기준 '요일', '월' 컬럼 생성
    processed["_dt"] = pd.to_datetime(processed[col_ci], errors="coerce")
    processed["요일"] = processed["_dt"].dt.dayofweek  # 0=월 .. 6=일
    dow_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    processed["요일"] = processed["요일"].map(dow_map)
    processed["월"] = processed["_dt"].dt.month
    processed = processed.drop(columns=["_dt"])

    logger.info(f"호텔 변환 완료! 컬럼: {list(processed.columns)}, 총 {len(processed)}건")
    return processed


if __name__ == "__main__":
    # 예약 리스트 변환 테스트
    try:
        from extract import extract_reservation_list
        raw = extract_reservation_list()
        cleaned = transform_reservation_data(raw)
        print(cleaned.head().to_string())
    except FileNotFoundError:
        from extract_api import extract_from_api
        raw_data = extract_from_api()
        if raw_data is not None:
            cleaned = transform_data(raw_data)
            print(cleaned.head().to_string())
