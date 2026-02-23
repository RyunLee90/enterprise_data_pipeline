# 2. 파이썬 코드 작업: src 폴더 안에 extract.py 파일 생성
# Cursor IDE 좌측 탐색기에서 src 폴더를 우클릭하고 'New File'을 눌러 'extract.py'를 만드게.
# (이유: 향후 어떤 파일이든 이 코드 하나만 통과하면 안전하게 불러올 수 있도록 '추출 전용 기계'를 만드는 과정입니다.)
# 아래 코드를 extract.py에 그대로 복사해서 붙여넣게.

import pandas as pd
import os
from logger import logger

def extract_data(file_path: str) -> pd.DataFrame:
    """
    지정된 경로에서 데이터를 추출하여 Pandas DataFrame으로 반환합니다.
    """
    logger.info(f"데이터 추출을 시작합니다: {file_path}")
    
    # [방어 로직] 파일이 실제로 존재하는지 먼저 확인
    # (이유: 데이터가 없는데 무작정 읽으려다 프로그램 전체가 다운되는 치명적인 에러를 막기 위함입니다.)
    if not os.path.exists(file_path):
        logger.error(f"오류: '{file_path}' 경로에 데이터가 존재하지 않습니다.")
        raise FileNotFoundError(f"오류: '{file_path}' 경로에 데이터가 존재하지 않습니다.")
        
    # [추출 로직] 파일이 있다면 Pandas를 이용해 데이터를 메모리로 읽어옴
    # (이유: 방대한 CSV 데이터를 표(Table) 형태로 깔끔하게 구조화하여 분석하기 쉽게 만듭니다.)
    df = pd.read_csv(file_path)
    logger.info(f"추출 완료! 총 {len(df)}개의 행(Row)을 가져왔습니다.")
    
    return df

if __name__ == "__main__":
    # 이 스크립트가 잘 작동하는지 자체 테스트하기 위한 임시 코드
    # (이유: 내가 만든 '추출 기계'의 전원 버튼이 잘 켜지는지 단독으로 실행해보기 위함입니다.)
    sample_path = "../data/raw/sample_data.csv"
    # 현재는 sample_data.csv 파일이 없으므로 실행 시 우리가 만든 방어 로직(FileNotFoundError)이 작동할 것이네.