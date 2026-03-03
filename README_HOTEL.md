# 호텔 데이터만 작업할 때 (주식과 무관)

이번에 하시는 작업은 **호텔 예약 데이터**만 다룹니다.  
주식(주가, yfinance, stock_prices) 관련 코드는 **사용하지 않습니다**.

---

## 호텔 작업 흐름

1. **엑셀 파일**  
   `C:\Users\PC\Desktop\Ryun\data\raw\hotel\Reservation List_20260301.xlsx`  
   (여기에 원본 예약 리스트 두기)

2. **노이즈 제거·정제**  
   - 개인정보 컬럼 즉시 삭제 (고객명, 전화번호, 이메일 등)  
   - 마켓코드 GRP, MICE 제거  
   - 실객실료 0원 행 제거  
   → **좋은 데이터만** 남김

3. **실행**  
   아래 명령만 실행하면 됩니다 (주식 파이프라인은 실행되지 않음).

   ```
   cd C:\Users\PC\Desktop\Ryun\enterprise_data_pipeline
   python main_hotel.py
   ```

4. **결과**  
   - 리포트: `data/processed/hotel_report_*.txt` + 로그  
   - 정제된 데이터: `data/processed/enterprise_dw.db` → **hotel_analytics** 테이블  
   - **육안 확인용 엑셀** (매 실행 시 생성):  
     - `data/processed/merged_hotel_확인용_YYYYMMDD_HHMMSS.xlsx` → 병합 직후(노이즈 제거 전)  
     - `data/processed/hotel_cleaned_확인용_YYYYMMDD_HHMMSS.xlsx` → 노이즈 제거 후  
     → Excel로 열어서 확인·직접 노이즈 제거 가능

---

## 주식 관련 (이번 작업에서 안 쓰는 것)

| 파일/기능 | 용도 | 이번 호텔 작업 |
|-----------|------|----------------|
| main.py | 주가 API 파이프라인 | 사용 안 함 |
| extract_api.py | yfinance 주가 수집 | 사용 안 함 |
| stock_prices 테이블 | 주가 저장 | 사용 안 함 |
| generate_report() (주가용) | 거래량/MDD 리포트 | 사용 안 함 |

**호텔만 하실 때는 `main_hotel.py`만 실행하시면 됩니다.**

---

## 호텔 관련만 보시면 되는 파일

| 파일 | 역할 |
|------|------|
| main_hotel.py | 호텔 파이프라인 **실행 진입점** |
| extract.py | `extract_hotel_res_list()` → 엑셀 읽기 + 개인정보 삭제 |
| transform.py | `transform_hotel_data()` → GRP/MICE·0원 제거 |
| report.py | `generate_hotel_report()` → OTA/요일별/월별/객실타입 분석 |
| load.py | `load_hotel_analytics()` → hotel_analytics 테이블 적재 |
| hotel_config.py | 객실 수 97실, 타입별 상수 |

엑셀만 두고 `python main_hotel.py` 실행하시면, 노이즈 제거된 호텔 데이터만 다루게 됩니다.
