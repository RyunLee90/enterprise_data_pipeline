# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\transform.py

import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    추출된 Raw 데이터를 입력받아 불필요한 데이터를 제거하고 새로운 분석용 열(Column)을 추가합니다.
    """
    print("[시스템] 데이터 변환(Transform)을 시작합니다...")
    
    # 1. 원본 데이터 보호를 위한 복사본 생성
    # (이유: 파이썬에서 원본 데이터프레임을 직접 수정하면 경고 에러가 나거나 데이터가 꼬일 수 있으므로, 항상 안전하게 복사본(copy)을 만들어 작업합니다.)
    processed_df = df.copy()
    
    # 2. 비즈니스 로직 1: 결제 실패('Failed') 데이터 필터링(제거)
    # (이유: 실질적인 매출 분석을 위해서는 'Completed(완료)'나 'Pending(대기)' 상태의 유의미한 거래만 남겨야 하기 때문입니다.)
    processed_df = processed_df[processed_df['status'] != 'Failed']
    
    # 3. 비즈니스 로직 2: 구매 금액 단위 변경 (새로운 파생 변수 생성)
    # (이유: 금액 단위가 너무 크면 분석가가 한눈에 파악하기 힘드므로, 기존 'purchase_amount'를 10,000으로 나누어 '만원' 단위의 새로운 열을 만듭니다.)
    processed_df['amount_manwon'] = processed_df['purchase_amount'] / 10000
    
    print(f"[시스템] 변환 완료! 정제된 유효 데이터 행(Row) 개수: {len(processed_df)}개 (실패건 제외됨)")
    
    return processed_df

if __name__ == "__main__":
    # 이 변환 모듈이 잘 작동하는지 자체 테스트하기 위한 임시 코드
    # (이유: 우리가 만든 정수기가 물을 잘 걸러내는지, 이전 단계(추출)의 코드를 불러와서 연결 테스트를 해보는 과정입니다.)
    from extract import extract_data  # 1단계에서 만든 추출 함수를 가져옵니다.
    
    # 1. 데이터 추출
    raw_data = extract_data("../data/raw/sample_data.csv")
    
    # 2. 데이터 변환
    cleaned_data = transform_data(raw_data)
    
    # 3. 결과 확인 (상위 5개 행만 출력하여 눈으로 검증)
    print("\n[미리보기] 정제된 데이터 상위 5건:")
    print(cleaned_data.head())