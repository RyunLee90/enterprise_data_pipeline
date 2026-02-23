# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\report.py

import pandas as pd
import sqlite3
import os
from logger import logger

def generate_report(db_name: str = "enterprise_dw.db", table_name: str = "sales_data"):
    """
    SQLite 데이터베이스에 적재된 데이터를 읽어와 비즈니스 요약 리포트를 출력합니다.
    """
    logger.info("=" * 50)
    logger.info("비즈니스 요약 리포트 생성을 시작합니다.")
    logger.info("=" * 50)

    # 1. 데이터베이스 경로 설정 및 연결
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "data", "processed", db_name)
    
    # DB 파일이 진짜 있는지 확인하는 방어 로직
    if not os.path.exists(db_path):
        logger.error(f"데이터베이스 파일을 찾을 수 없습니다: {db_path}")
        logger.error("먼저 main.py를 실행하여 데이터를 적재해 주세요.")
        return

    conn = sqlite3.connect(db_path)

    try:
        # 2. SQL 쿼리를 통해 DB에서 데이터프레임으로 데이터 읽어오기
        # (이유: Pandas의 read_sql 기능을 쓰면 DB에 있는 테이블을 아주 쉽게 표 형태로 가져올 수 있습니다.)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        
        # 3. 비즈니스 리포트 생성 (데이터 집계 및 분석)
        
        # 3-1. 총 매출액 (만원 단위)
        total_revenue = df['amount_manwon'].sum()
        
        # 3-2. 거래 상태별 건수 (성공, 대기 등)
        # value_counts()는 특정 컬럼의 값들이 몇 개씩 있는지 세어주는 아주 유용한 함수입니다.
        status_counts = df['status'].value_counts()
        
        # 3-3. 평균 구매 고객 연령
        avg_age = df['user_age'].mean()

        # 4. 리포트 출력
        logger.info(f"총 분석 대상 거래 건수: {len(df)}건")
        logger.info(f"총 매출액: {total_revenue:,.1f} 만원") # ,.1f 는 천단위 콤마와 소수점 1자리까지 표시
        logger.info(f"평균 구매 고객 연령: {avg_age:.1f} 세")
        logger.info("[거래 상태별 건수]")
        logger.info("\n" + status_counts.to_string()) # 결과를 깔끔한 텍스트로 출력
        
    except Exception as e:
        logger.error(f"리포트 생성 중 문제가 발생했습니다: {e}")
        
    finally:
        # 5. 작업이 끝나면 반드시 DB 연결 닫기
        conn.close()
        logger.info("=" * 50)

if __name__ == "__main__":
    # 스크립트를 직접 실행하면 리포트를 생성합니다.
    generate_report()