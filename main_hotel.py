# 호텔 데이터 파이프라인만 실행 (주식과 무관)
# 1) data/raw/hotel/ 엑셀(xlsx) 추출 → 개인정보 즉시 삭제
# 2) 노이즈 제거: 마켓 GRP/MICE, 실객실료 0원 필터 → 좋은 데이터만
# 3) OTA/요일별/월별/객실타입 분석 리포트
# 4) enterprise_dw.db 의 hotel_analytics 테이블에 적재

from logger import setup_logger
from extract import extract_hotel_res_list
from transform import transform_hotel_data
from report import generate_hotel_report
from load import load_hotel_analytics

def run_hotel_pipeline():
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("호텔 데이터 파이프라인 시작")
    logger.info("=" * 60)

    try:
        # 1. 추출 (개인정보 컬럼 즉시 drop)
        raw = extract_hotel_res_list()
        if raw is None or raw.empty:
            logger.error("호텔 데이터 추출 실패. data/raw/hotel/ 에 엑셀(Reservation List_20260301.xlsx) 또는 CSV를 두세요.")
            return

        # 2. 변환 (GRP/MICE 제거, 실객실료 0원 제거, 퇴실<입실 제거, 요일/월 추가)
        df = transform_hotel_data(raw)
        if df is None or df.empty:
            logger.error("변환 후 데이터가 없습니다.")
            return

        # 노이즈 제거 후 결과도 엑셀 저장 → 육안 확인용
        import pandas as pd
        import os
        from datetime import datetime
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_root = os.path.dirname(base_dir)
        out_dir = os.path.join(data_root, "data", "processed")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_path = os.path.join(out_dir, f"hotel_cleaned_for_review_{ts}.xlsx")
        df.to_excel(cleaned_path, index=False, engine="openpyxl")
        logger.info(f"Saved for review (after noise removal): {cleaned_path}")

        # 3. 비즈니스 로직 리포트 (로깅)
        generate_hotel_report(df)

        # 4. hotel_analytics 테이블에 적재
        load_hotel_analytics(df)

        logger.info("호텔 파이프라인 완료.")
    except FileNotFoundError as e:
        logger.error(f"파일 없음: {e}")
    except Exception as e:
        logger.error(f"파이프라인 오류: {e}")

if __name__ == "__main__":
    run_hotel_pipeline()
