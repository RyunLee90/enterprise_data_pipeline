# 파일 위치: C:\Users\ryunl\Desktop\Projects\enterprise_data_pipeline\src\extract_api.py

import requests
import pandas as pd

def extract_from_api():
    """
    외부 REST API 서버와 통신하여 실시간 데이터를 수집하고
    Pandas DataFrame으로 변환하여 반환합니다.
    """
    # 연습용 오픈 API 주소 (가상의 고객 데이터 10건을 무료로 제공)
    url = "https://randomuser.me/api/?results=1000&nat=us,gb,fr,ca"
    
    print(f"\n[시스템] 외부 서버에 데이터 연결을 시도합니다... (URL: {url})")
    
    try:
        # 1. 서버의 문을 두드리고 데이터 요청 (최대 10초 대기)
        response = requests.get(url, timeout=10)
        
        # 2. 서버가 '200 OK' (정상) 신호를 보냈는지 확인
        if response.status_code == 200:
            # 3. JSON 응답에서 실제 데이터 리스트는 'results' 키 안에 있음
            data = response.json()
            results = data['results']

            # 4. 분석하기 편한 DataFrame으로 변환
            df = pd.DataFrame(results)

            print("="*50)
            print(f"[성공] 외부 데이터 {len(df)}건 수집 완료!")
            print(f"[미리보기] 컬럼 목록: {list(df.columns)}")
            print("="*50)

            return df
        else:
            # 404(페이지 없음), 500(서버 오류) 등의 에러 처리
            raise ConnectionError(f"[오류] 서버 비정상 응답. 상태 코드: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"[치명적 오류] API 서버와 통신할 수 없습니다: {e}")
        return None

if __name__ == "__main__":
    # 이 파일만 단독으로 실행했을 때 테스트가 돌아가도록 설정
    extract_from_api()