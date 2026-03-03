# enterprise_dw.db 내용 확인용 스크립트
# 사용법: python view_db.py
# (enterprise_data_pipeline 폴더에서 실행)

import sqlite3
import os

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base, "data", "processed", "enterprise_dw.db")

    if not os.path.exists(db_path):
        print(f"DB 파일이 없습니다: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 테이블 목록
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print("=" * 60)
    print(f"DB 파일: {db_path}")
    print(f"테이블 목록: {tables}")
    print("=" * 60)

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM [{table}]")
        n = cur.fetchone()[0]
        print(f"\n[{table}] (총 {n}건)")
        cur.execute(f"PRAGMA table_info([{table}])")
        cols = [r[1] for r in cur.fetchall()]
        print(f"  컬럼: {cols}")
        if n > 0:
            cur.execute(f"SELECT * FROM [{table}] LIMIT 5")
            rows = cur.fetchall()
            print("  샘플 5행:")
            for i, row in enumerate(rows, 1):
                print(f"    {i}: {row}")

    conn.close()
    print("\n" + "=" * 60)
    print("가시적으로 보려면: python export_db_to_excel.py 실행")
    print("  → data/processed/enterprise_dw_보기용.xlsx 생성 → Excel로 열기")

if __name__ == "__main__":
    main()
