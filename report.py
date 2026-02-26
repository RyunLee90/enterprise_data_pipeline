# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\report.py

import pandas as pd
import sqlite3
import os
from logger import setup_logger

logger = setup_logger()

def generate_report(db_name: str = "enterprise_dw.db", table_name: str = "users_data"):
    """
    SQLite DB에 적재된 유저 데이터를 읽어와 분석 리포트를 출력합니다.
    """
    logger.info("=" * 50)
    logger.info("유저 데이터 분석 리포트 생성 시작")
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

        # 1. 총 유저 수
        logger.info(f"총 수집 유저 수: {len(df)}명")

        # 2. 성별 분포
        gender_counts = df['gender'].value_counts()
        logger.info("[성별 분포]\n" + gender_counts.to_string())

        # 3. 국적(nat)별 유저 수
        nat_counts = df['nat'].value_counts()
        logger.info("[국적별 유저 수]\n" + nat_counts.to_string())

        # 4. 나이 통계
        logger.info(f"[나이 통계]")
        logger.info(f"  평균 나이: {df['age'].mean():.1f}세")
        logger.info(f"  최연소:    {df['age'].min()}세")
        logger.info(f"  최연장:    {df['age'].max()}세")

        # 5. 가입 연수 평균
        logger.info(f"  평균 가입 연수: {df['registered_years'].mean():.1f}년")

        # 6. 국가별 도시 TOP 3
        logger.info("[국가별 유저가 가장 많은 도시 TOP 3]")
        for country, group in df.groupby('country'):
            top_cities = group['city'].value_counts().head(3)
            logger.info(f"  {country}: {', '.join([f'{c}({n}명)' for c, n in top_cities.items()])}")

        # 7. 이메일 도메인 TOP 5
        top_domains = df['email_domain'].value_counts().head(5)
        logger.info("[이메일 도메인 TOP 5]\n" + top_domains.to_string())

    except Exception as e:
        logger.error(f"리포트 생성 중 오류 발생: {e}")

    finally:
        conn.close()
        logger.info("=" * 50)

if __name__ == "__main__":
    generate_report()