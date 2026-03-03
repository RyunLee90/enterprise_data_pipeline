"""merged_hotel_for_review xlsx 읽어서 13개 컬럼만 남기고 저장 (noise 1st)."""
import pandas as pd
import os
import glob

REVIEW_COLS = [
    "Rsvn No", "Arr Date", "Dep Date", "Nts", "Rm Ty", "Rms",
    "Total Amount", "Ra Ty", "Ma Ty", "Nat", "Visit", "ADR", "account", "Account", "ACCOUNT",
]

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed = os.path.join(base, "data", "processed")
    pattern = os.path.join(processed, "merged_hotel_for_review_*.xlsx")
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError(f"No file: {pattern}")
    path = files[0]
    df = pd.read_excel(path, engine="openpyxl")
    keep = [c for c in REVIEW_COLS if c in df.columns]
    out = df[keep].copy()
    out_path = os.path.join(processed, "noise_1st.xlsx")
    out.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved {len(out)} rows, {len(keep)} columns -> {out_path}")

if __name__ == "__main__":
    main()
