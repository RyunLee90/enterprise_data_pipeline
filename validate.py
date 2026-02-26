from pydantic import BaseModel, field_validator, ValidationError
import pandas as pd
from logger import setup_logger

logger = setup_logger()

# RandomUser API 데이터 규격 정의
class UserSchema(BaseModel):
    gender:           str
    full_name:        str
    email:            str
    email_domain:     str
    phone:            str
    cell:             str
    nat:              str
    city:             str
    state:            str
    country:          str
    postcode:         str
    age:              int
    registered_years: int

    @field_validator('email')
    @classmethod
    def email_must_contain_at(cls, v):
        if '@' not in v:
            raise ValueError(f"올바른 이메일 형식이 아닙니다: {v}")
        return v

    @field_validator('age')
    @classmethod
    def age_must_be_valid(cls, v):
        if not (0 < v < 120):
            raise ValueError(f"유효하지 않은 나이: {v}")
        return v

    @field_validator('gender')
    @classmethod
    def gender_must_be_valid(cls, v):
        if v not in ('male', 'female'):
            raise ValueError(f"유효하지 않은 성별값: {v}")
        return v


def validate_with_pydantic(df: pd.DataFrame) -> bool:
    """
    RandomUser 데이터를 UserSchema로 전수 검증합니다.
    """
    logger.info("Pydantic 유저 데이터 검증 시작...")

    required_cols = [
        'gender', 'full_name', 'email', 'email_domain', 'phone', 'cell',
        'nat', 'city', 'state', 'country', 'postcode', 'age', 'registered_years'
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(f"필수 컬럼 누락: {missing}")
        return False

    records = df[required_cols].to_dict(orient='records')
    errors = []

    for i, record in enumerate(records):
        try:
            UserSchema(**record)
        except ValidationError as e:
            errors.append(f"Row {i}: {e.errors()}")

    if errors:
        logger.error(f"검증 실패 {len(errors)}건:")
        for err in errors[:5]:  # 처음 5건만 출력
            logger.error(err)
        return False

    logger.info(f"총 {len(records)}건 검증 통과!")
    return True