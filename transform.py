# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\transform.py

import pandas as pd
from logger import setup_logger

logger = setup_logger()

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    RandomUser API Raw 데이터를 분석용 평탄 구조로 변환합니다.
    중첩된 dict 컬럼들을 모두 펼쳐서 단순 컬럼으로 만듭니다.
    """
    logger.info("데이터 변환(Transform)을 시작합니다...")

    processed_df = df.copy()

    # 1. name 딕셔너리 → full_name (first + last)
    processed_df['full_name'] = processed_df['name'].apply(
        lambda x: f"{x.get('first', '')} {x.get('last', '')}" if isinstance(x, dict) else ''
    )

    # 2. location 딕셔너리 → city, state, country, postcode
    processed_df['city']     = processed_df['location'].apply(lambda x: x.get('city', '')    if isinstance(x, dict) else '')
    processed_df['state']    = processed_df['location'].apply(lambda x: x.get('state', '')   if isinstance(x, dict) else '')
    processed_df['country']  = processed_df['location'].apply(lambda x: x.get('country', '') if isinstance(x, dict) else '')
    processed_df['postcode'] = processed_df['location'].apply(
        lambda x: str(x.get('postcode', '')) if isinstance(x, dict) else ''
    )

    # 3. dob 딕셔너리 → age (나이)
    processed_df['age'] = processed_df['dob'].apply(
        lambda x: x.get('age', 0) if isinstance(x, dict) else 0
    )

    # 4. registered 딕셔너리 → registered_years (가입 연수)
    processed_df['registered_years'] = processed_df['registered'].apply(
        lambda x: x.get('age', 0) if isinstance(x, dict) else 0
    )

    # 5. email 도메인 추출 (파생 변수)
    processed_df['email_domain'] = processed_df['email'].apply(
        lambda x: x.split('@')[-1] if '@' in str(x) else ''
    )

    # 6. gender, nat(국적), phone, cell, email은 그대로 유지
    keep_cols = [
        'gender', 'full_name', 'email', 'email_domain',
        'phone', 'cell', 'nat',
        'city', 'state', 'country', 'postcode',
        'age', 'registered_years'
    ]
    processed_df = processed_df[keep_cols]

    logger.info(f"변환 완료! 최종 컬럼: {list(processed_df.columns)}")
    logger.info(f"총 {len(processed_df)}건 변환 성공")

    return processed_df

if __name__ == "__main__":
    from extract_api import extract_from_api
    raw_data = extract_from_api()
    if raw_data is not None:
        cleaned = transform_data(raw_data)
        print(cleaned.head().to_string())