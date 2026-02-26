import pandas as pd
import sqlite3
import os
from logger import setup_logger

logger = setup_logger()

def load_data(df: pd.DataFrame, db_name: str = "enterprise_dw.db", table_name: str = "users_data"):
    """
    정제된 유저 데이터를 SQLite DB에 적재합니다.
    """
    logger.info(f"데이터 적재(Load)를 시작합니다. 대상 테이블: {table_name}")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, db_name)

    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        logger.info(f"총 {len(df)}건 적재 완료 → {db_path}")
    except Exception as e:
        logger.error(f"데이터 적재 중 오류 발생: {e}")