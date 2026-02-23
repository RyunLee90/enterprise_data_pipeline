# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\generate_mock.py

import pandas as pd
import numpy as np
import os

def create_mock_data():
    """가상의 실무 데이터를 생성하여 raw 폴더에 저장합니다."""
    print("[시스템] 더미 데이터 생성을 시작합니다...")
    
    # 1. 저장할 경로 지정 
    # (이유: src 폴더에서 한 칸 위(..)로 올라가 data/raw 폴더에 저장하도록 상대 경로를 설정합니다.)
    save_path = "../data/raw/sample_data.csv"
    
    # 2. 가상 데이터 생성 
    # (이유: Pandas와 Numpy를 활용해 100줄짜리 가상의 거래 데이터를 표(DataFrame) 형태로 만듭니다.)
    np.random.seed(42) # (이유: 실행할 때마다 값이 바뀌지 않고 동일한 난수가 나오도록 고정합니다.)
    
    data = {
        'transaction_id': range(1, 101),
        'user_age': np.random.randint(20, 60, 100),       # 20~60세 사이의 랜덤 나이
        'purchase_amount': np.random.randint(10, 500, 100) * 1000, # 구매 금액
        'status': np.random.choice(['Completed', 'Pending', 'Failed'], 100) # 거래 상태
    }
    
    df = pd.DataFrame(data)
    
    # 3. CSV 파일로 저장
    # (이유: 인덱스 번호를 제외(index=False)하고 순수 데이터만 CSV 파일로 내보냅니다.)
    df.to_csv(save_path, index=False)
    print(f"[시스템] 데이터 생성 완료! 저장 위치: {save_path}")

if __name__ == "__main__":
    # 스크립트를 실행하면 즉시 함수가 작동하도록 호출합니다.
    create_mock_data()