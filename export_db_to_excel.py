# DB 내용을 엑셀(xlsx)로 내보내서 가시적으로 볼 수 있게 함
# 사용법: python export_db_to_excel.py
# 결과: data/processed/enterprise_dw_보기용.xlsx 생성 → 더블클릭해서 Excel로 열기

import sqlite3
import os
import pandas as pd

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base, "data", "processed", "enterprise_dw.db")
    out_path = os.path.join(base, "data", "processed", "enterprise_dw_보기용.xlsx")

    if not os.path.exists(db_path):
        print(f"DB 파일이 없습니다: {db_path}")
        return

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]

    if not tables:
        conn.close()
        print("테이블이 없습니다.")
        return

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for table in tables:
            df = pd.read_sql(f'SELECT * FROM [{table}]', conn)
            sheet_name = table[:31] if len(table) > 31 else table
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    conn.close()

    print(f"저장 완료: {out_path}")
    print("이 파일을 더블클릭하면 Excel에서 가시적으로 확인할 수 있습니다.")

if __name__ == "__main__":
    main()
