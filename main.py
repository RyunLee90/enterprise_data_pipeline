# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\main.py

import logging
from logger import setup_logger
from extract_api import extract_from_api
from transform import transform_data
from validate import validate_with_pydantic
from load import load_data
from report import generate_report

def run_pipeline():
    logger = setup_logger()
    logger.info("="*50)
    logger.info("실시간 API 기반 엔터프라이즈 파이프라인 가동")
    logger.info("="*50)

    try:
        # ▶ 1단계: 외부 API 데이터 추출 (CSV 대신 API 호출)
        logger.info("▶ 1단계: 외부 REST API 데이터 추출 중...")
        raw_df = extract_from_api() # 우리가 만든 API 수집 함수 호출
        
        if raw_df is None or raw_df.empty:
            logger.error("데이터 수집 실패로 파이프라인을 중단합니다.")
            return

        # ▶ 2단계: 데이터 변환 (API 규격에 맞게 내부 로직은 유지)
        logger.info("▶ 2단계: 데이터 변환 및 정제 진행 중...")
        transformed_df = transform_data(raw_df)

        # ▶ 3단계: 검증 및 적재
        logger.info("▶ 3단계: 데이터 무결성 검증 및 DB 적재 중...")
        if validate_with_pydantic(transformed_df):
            load_data(transformed_df)
            
            # ▶ 4단계: 리포트 생성
            logger.info("▶ 4단계: 비즈니스 분석 리포트 생성 중...")
            generate_report()
            
            logger.info("="*50)
            logger.info("API 기반 파이프라인 실행 성공!")
            logger.info("="*50)

    except Exception as e:
        logger.error(f"파이프라인 가동 중 치명적 오류 발생: {e}")

if __name__ == "__main__":
    run_pipeline()