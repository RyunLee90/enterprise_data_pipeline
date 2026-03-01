# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\transform.py

import pandas as pd
from logger import setup_logger

logger = setup_logger()

# 유지할 컬럼 (주가 DataFrame용)
KEEP_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    추출된 주가 데이터를 Pandas DataFrame 형태로 정리합니다.
    Date, Open, High, Low, Close, Volume, Ticker 컬럼을 유지합니다.
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

    # 수치 컬럼 정리 (결측 제거는 선택 사항)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in processed_df.columns:
            processed_df[col] = pd.to_numeric(processed_df[col], errors="coerce")
    processed_df = processed_df.dropna(subset=["Close", "Volume"], how="all")

    logger.info(f"변환 완료! 최종 컬럼: {list(processed_df.columns)}")
    logger.info(f"총 {len(processed_df)}건 변환 성공")

    return processed_df


if __name__ == "__main__":
    from extract_api import extract_from_api

    raw_data = extract_from_api()
    if raw_data is not None:
        cleaned = transform_data(raw_data)
        print(cleaned.head().to_string())
