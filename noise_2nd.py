"""noise_1st.xlsx 읽어서 Nts 365 제거, Ra Ty 'HOU' 제거, Arr Date 오래된 순 정렬 후 저장 (noise 2nd)."""
import pandas as pd
import os

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed = os.path.join(base, "data", "processed")
    path = os.path.join(processed, "noise_1st.xlsx")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No file: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    n_before = len(df)

    # 1. Nts 365 삭제
    if "Nts" in df.columns:
        nts = df["Nts"].astype(str).str.strip()
        df = df[(nts != "365") & (df["Nts"] != 365)].copy()
    n_after_nts = len(df)

    # 2. Ra Ty 'HOU' 삭제
    if "Ra Ty" in df.columns:
        df["Ra Ty"] = df["Ra Ty"].astype(str).str.strip()
        df = df[df["Ra Ty"] != "HOU"].copy()
    n_after_ra = len(df)

    # 3. Arr Date 오래된 순 정렬
    if "Arr Date" in df.columns:
        df["Arr Date"] = pd.to_datetime(df["Arr Date"], errors="coerce")
        df = df.sort_values("Arr Date", ascending=True).reset_index(drop=True)

    # 4. Rsvn No 중복 삭제 (같은 예약번호면 첫 행만 유지)
    if "Rsvn No" in df.columns:
        n_before_dedup = len(df)
        df = df.drop_duplicates(subset=["Rsvn No"], keep="first").reset_index(drop=True)
        n_after_dedup = len(df)
        if n_before_dedup > n_after_dedup:
            print(f"Rsvn No duplicates removed: {n_before_dedup - n_after_dedup} rows")

    out_path = os.path.join(processed, "noise_2nd.xlsx")
    df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved {len(df)} rows (was {n_before}, -Nts365: {n_after_nts}, -Ra Ty HOU: {n_after_ra}) -> {out_path}")

if __name__ == "__main__":
    main()
