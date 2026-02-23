# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\main.py

import os
import time

# 우리가 직접 만든 E, T, L 모듈들을 부품처럼 가져옵니다.
from extract import extract_data
from transform import transform_data
from load import load_data

def run_pipeline():
    """
    ETL(Extract, Transform, Load) 전체 파이프라인을 순차적으로 실행하는 메인 함수입니다.
    """
    print("="*50)
    print("[시스템] 엔터프라이즈 데이터 파이프라인 가동을 시작합니다.")
    print("="*50)
    
    start_time = time.time() # 전체 소요 시간 측정을 위한 타이머 시작
    
    # 데이터 경로 설정 (Cursor가 잡아준 완벽한 절대경로 방식을 여기에도 적용합니다)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_path = os.path.join(base_dir, "data", "raw", "sample_data.csv")
    
    try:
        # Step 1: Extract (추출)
        print("\n▶ 1단계: 데이터 추출 진행 중...")
        raw_df = extract_data(raw_data_path)
        
        # Step 2: Transform (변환)
        print("\n▶ 2단계: 데이터 변환 진행 중...")
        cleaned_df = transform_data(raw_df)
        
        # Step 3: Load (적재)
        print("\n▶ 3단계: 데이터 적재 진행 중...")
        load_data(cleaned_df)
        
        # 파이프라인 성공 종료
        end_time = time.time()
        print("\n" + "="*50)
        print(f"[시스템] 파이프라인 실행 성공! (소요 시간: {end_time - start_time:.2f}초)")
        print("="*50)

    except Exception as e:
        # 어느 단계에서든 에러가 터지면 파이프라인을 즉시 멈추고 에러를 보고합니다.
        print("\n" + "!"*50)
        print(f"[치명적 오류] 파이프라인 실행 중 시스템이 중단되었습니다.")
        print(f"상세 원인: {e}")
        print("!"*50)

if __name__ == "__main__":
    run_pipeline()