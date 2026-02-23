# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\load.py

import pandas as pd
import sqlite3
import os

def load_data(df: pd.DataFrame, db_name: str = "enterprise_dw.db", table_name: str = "sales_data"):
    """
    정제된 DataFrame을 관계형 데이터베이스(SQLite)의 테이블에 적재합니다.
    """
    print(f"[시스템] 데이터 적재(Load)를 시작합니다. 대상 테이블: {table_name}")
    
    # 1. 데이터베이스 저장 경로 설정
    # (이유: 스크립트 파일 기준으로 프로젝트 루트의 data/processed 폴더 안에 데이터베이스 파일을 생성하도록 경로를 지정합니다.)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 프로젝트 루트
    db_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(db_dir, exist_ok=True)  # 폴더가 없으면 생성
    db_path = os.path.join(db_dir, db_name)
    
    # 2. 데이터베이스 연결 (Connection)
    # (이유: 파이썬과 데이터베이스 사이에 데이터를 전송할 '통신 다리'를 놓는 과정입니다. 파일이 없으면 자동으로 생성됩니다.)
    conn = sqlite3.connect(db_path)
    
    try:
        # 3. 데이터 적재 (Pandas to SQL)
        # (이유: 복잡한 SQL 쿼리문 없이, Pandas의 to_sql 기능을 이용해 데이터프레임을 통째로 DB 테이블에 밀어 넣습니다.)
        # if_exists='replace': 이미 같은 이름의 테이블이 있다면 지우고 새로 덮어씁니다. (테스트용으로 적합)
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        print(f"[시스템] 적재 완료! '{db_path}'에 {len(df)}건의 데이터가 안전하게 저장되었습니다.")
        
    except Exception as e:
        print(f"[오류] 데이터 적재 중 문제가 발생했습니다: {e}")
        
    finally:
        # 4. 데이터베이스 연결 종료
        # (이유: 적재가 끝나면 반드시 통신 다리를 끊어주어야(close) 다른 프로그램이 DB에 접근할 때 락(Lock)이 걸리지 않습니다.)
        conn.close()

if __name__ == "__main__":
    # 이 적재 모듈이 잘 작동하는지 전체 흐름을 테스트합니다 (Extract -> Transform -> Load)
    from extract import extract_data
    from transform import transform_data
    
    # 파이프라인 흐름 테스트
    raw_df = extract_data("../data/raw/sample_data.csv")  # 1. 추출
    cleaned_df = transform_data(raw_df)                   # 2. 변환
    load_data(cleaned_df)                                 # 3. 적재