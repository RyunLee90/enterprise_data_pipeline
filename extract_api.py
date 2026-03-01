# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\extract_api.py

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# 6개 종목, 최근 2년치 일일 주가 (약 500일 x 6종목 = 약 3,000건)
TICKERS = ["AMZW", "NVDA", "AVGW", "TSLA", "AAPL", "MSFT"]


def extract_from_api():
    """
    yfinance를 사용하여 지정 종목의 최근 2년치 일일 주가 데이터를
    한 번에 가져와 Pandas DataFrame으로 반환합니다.
    """
    end = datetime.now()
    start = end - timedelta(days=365 * 2)

    print(f"\n[시스템] yfinance로 {len(TICKERS)}개 종목 주가 수집 중... (기간: {start.date()} ~ {end.date()})")

    try:
        # 여러 종목을 한 번에 다운로드 (단일 API 호출)
        data = yf.download(
            TICKERS,
            start=start,
            end=end,
            group_by="ticker",
            progress=False,
            auto_adjust=False,
            threads=True,
        )

        if data.empty:
            print("[오류] 수집된 데이터가 없습니다.")
            return None

        # MultiIndex 컬럼(여러 종목) → 평탄화: 각 행에 Ticker 추가
        if len(TICKERS) == 1:
            data.columns = [f"{c}" for c in data.columns]
            data["Ticker"] = TICKERS[0]
            data = data.reset_index()
        else:
            # group_by='ticker' → MultiIndex columns (Ticker, Open/High/Low/Close/Volume)
            if isinstance(data.columns, pd.MultiIndex):
                available = data.columns.get_level_values(0).unique().tolist()
            else:
                available = data.columns.tolist()
            frames = []
            for ticker in TICKERS:
                if ticker not in available:
                    continue
                sub = data[ticker][["Open", "High", "Low", "Close", "Volume"]].copy()
                sub = sub.reset_index()
                sub["Ticker"] = ticker
                frames.append(sub)
            data = pd.concat(frames, ignore_index=True)

        if data.empty:
            print("[오류] 유효한 종목 데이터가 없습니다.")
            return None

        # 컬럼명 통일 (Date, Open, High, Low, Close, Volume, Ticker)
        data = data.rename(columns={"Date": "Date"})
        if "Date" not in data.columns and data.index.name == "Date":
            data = data.reset_index()
        data = data[["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]]

        data["Date"] = pd.to_datetime(data["Date"]).dt.tz_localize(None)
        data = data.dropna(subset=["Close", "Volume"])

        print("=" * 50)
        print(f"[성공] 주가 데이터 {len(data)}건 수집 완료!")
        print(f"[미리보기] 컬럼: {list(data.columns)}")
        print("=" * 50)

        return data

    except Exception as e:
        print(f"[치명적 오류] 데이터 수집 실패: {e}")
        return None


if __name__ == "__main__":
    extract_from_api()
