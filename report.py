# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\report.py

import pandas as pd
import sqlite3
import os
from logger import setup_logger

logger = setup_logger()

DEFAULT_DB = "enterprise_dw.db"
DEFAULT_TABLE = "stock_prices"


def generate_report(db_name: str = DEFAULT_DB, table_name: str = DEFAULT_TABLE):
    """
    SQLite DB의 stock_prices 테이블을 읽어
    - 전체 데이터 중 거래량(Volume)이 가장 많았던 날
    - 종목별 최고가 대비 현재가 하락율(MDD)을 계산하여 출력합니다.
    """
    logger.info("=" * 50)
    logger.info("주가 데이터 분석 리포트 생성 시작")
    logger.info("=" * 50)

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "data", "processed", db_name)

    if not os.path.exists(db_path):
        logger.error(f"DB 파일을 찾을 수 없습니다: {db_path}")
        logger.error("먼저 main.py를 실행하여 데이터를 적재해 주세요.")
        return

    conn = sqlite3.connect(db_path)

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df["Date"] = pd.to_datetime(df["Date"])

        if df.empty:
            logger.error("테이블에 데이터가 없습니다.")
            return

        # 1. 전체 데이터 중 거래량(Volume)이 가장 많았던 날
        idx_max_vol = df["Volume"].idxmax()
        row_max = df.loc[idx_max_vol]
        logger.info("[거래량 최대일]")
        logger.info(
            f"  일자: {row_max['Date'].strftime('%Y-%m-%d') if hasattr(row_max['Date'], 'strftime') else row_max['Date']}, "
            f"종목: {row_max['Ticker']}, "
            f"거래량: {int(row_max['Volume']):,}"
        )

        # 2. 종목별 최고가 대비 현재가 하락율(MDD)
        # 현재가 = 각 종목의 가장 최근 종가(Close)
        # 최고가 = 각 종목의 기간 내 최고 High (또는 Close 기준 최고가)
        logger.info("[종목별 MDD (최고가 대비 현재가 하락율)]")
        for ticker in df["Ticker"].unique():
            sub = df[df["Ticker"] == ticker].sort_values("Date")
            if sub.empty:
                continue
            period_high = sub["High"].max()
            current_price = sub["Close"].iloc[-1]
            if period_high and period_high > 0:
                mdd_pct = (1 - current_price / period_high) * 100
                logger.info(f"  {ticker}: 최고가 {period_high:.2f} → 현재가 {current_price:.2f}, MDD {mdd_pct:.2f}%")
            else:
                logger.info(f"  {ticker}: 데이터 부족")

        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"리포트 생성 중 오류 발생: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    generate_report()
