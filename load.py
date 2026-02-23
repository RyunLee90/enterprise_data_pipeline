import pandas as pd
import sqlite3
import os
from logger import logger  # Phase 3에서 만든 로거 임포트
from validate import validate_with_pydantic

def load_data(df: pd.DataFrame, db_name: str = "enterprise_dw.db", table_name: str = "sales_data"):
    """
    정제된 데이터를 검증 후 SQLite DB에 적재합니다.
    """
    # [검문소] Pydantic 기반 검증 실패 시 적재 프로세스 원천 차단
    logger.info("데이터 무결성 검사(Pydantic)를 시작합니다...")
    if not validate_with_pydantic(df):
        logger.warning("데이터 품질 이슈로 인해 적재 프로세스를 중단합니다.")
        return

    logger.info(f"데이터 적재(Load)를 시작합니다. 대상 테이블: {table_name}")
    
    # 2. 경로 설정 (프로젝트 구조에 맞게 자동 생성)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, db_name)
    
    # 3. 데이터