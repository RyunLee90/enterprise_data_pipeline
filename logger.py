import logging
import os
from datetime import datetime

def setup_logger():
    """프로젝트 전역에서 사용할 로거를 설정합니다."""
    # 1. 로그 저장 폴더 생성
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 2. 파일명 설정 (예: logs/20231027_pipeline.log)
    log_filename = f"{datetime.now().strftime('%Y%m%d')}_pipeline.log"
    log_path = os.path.join(log_dir, log_filename)

    # 3. 로거 설정
    logger = logging.getLogger("DataPipeline")
    logger.setLevel(logging.INFO)

    # 4. 출력 형식(Formatter) 정의
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 5. 파일 핸들러 (파일에 기록)
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 6. 콘솔 핸들러 (터미널에 출력)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 중복 추가 방지
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# 싱글톤 패턴처럼 사용하기 위해 객체 생성
logger = setup_logger()