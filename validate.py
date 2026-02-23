from pydantic import BaseModel, Field, ValidationError
from typing import List
import pandas as pd
from logger import logger

# 1. 데이터 규격 정의 (Schema)
# 현재 파이프라인에서 사용하는 컬럼 구조에 맞춰 스키마를 정의합니다.
class SalesSchema(BaseModel):
    transaction_id: int                              # 정수
    user_age: int                                    # 정수
    purchase_amount: float = Field(gt=0)            # 0보다 큰 실수/정수
    status: str                                      # 문자열
    amount_manwon: float = Field(gt=0)              # 0보다 큰 실수

def validate_with_pydantic(df: pd.DataFrame):
    """
    Pandas 데이터를 Pydantic 모델을 통해 검증합니다.
    """
    logger.info("Pydantic을 이용한 세계 표준 검증 시작...")
    
    # DataFrame을 딕셔너리 리스트로 변환
    records = df.to_dict(orient='records')
    
    try:
        # 2. 리스트 컴프리헨션을 이용한 전수 검사
        [SalesSchema(**record) for record in records]
        logger.info(f"총 {len(records)}건의 데이터 품질이 완벽합니다.")
        return True
    except ValidationError as e:
        # 3. 에러 발생 시 상세 정보 로그 출력
        logger.error("데이터 규격이 맞지 않습니다.")
        logger.error(e.json())
        return False

# 실행 예시 (나중에 load.py에서 호출)
# if validate_with_pydantic(cleaned_df): load_data(cleaned_df)