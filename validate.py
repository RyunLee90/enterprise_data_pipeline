from pydantic import BaseModel, field_validator, ValidationError
import pandas as pd
from datetime import datetime
from logger import setup_logger

logger = setup_logger()


# 주가 데이터 규격 정의
class StockSchema(BaseModel):
    Date: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    Ticker: str

    @field_validator("Date", mode="before")
    @classmethod
    def date_to_datetime(cls, v):
        if hasattr(v, "to_pydatetime"):
            return v.to_pydatetime()
        return v

    @field_validator("Open", "High", "Low", "Close", "Volume")
    @classmethod
    def must_be_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("가격/거래량은 0 이상이어야 합니다.")
        return v


def validate_with_pydantic(df: pd.DataFrame) -> bool:
    """
    주가 데이터를 StockSchema로 검증합니다.
    """
    logger.info("Pydantic 주가 데이터 검증 시작...")

    required_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(f"필수 컬럼 누락: {missing}")
        return False

    records = df[required_cols].to_dict(orient="records")
    errors = []

    for i, record in enumerate(records):
        try:
            # pandas Timestamp를 그대로 사용
            StockSchema(**record)
        except ValidationError as e:
            errors.append(f"Row {i}: {e.errors()}")

    if errors:
        logger.error(f"검증 실패 {len(errors)}건:")
        for err in errors[:5]:
            logger.error(err)
        return False

    logger.info(f"총 {len(records)}건 검증 통과!")
    return True
