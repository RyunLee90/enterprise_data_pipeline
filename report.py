"""
호텔 BI 분석 리포트 (noise_2nd.xlsx 기반)
7대 핵심 비즈니스 지표 계산 및 출력
"""
import pandas as pd
import os
from datetime import datetime
from logger import setup_logger
from hotel_config import INVENTORY_TOTAL, ROOM_TYPE_INVENTORY, get_days_in_month

logger = setup_logger()

NOISE_2ND_FILE = "noise_2nd.xlsx"


# ────────────────────────────────────────────────
# 데이터 로드 및 파생 변수 생성
# ────────────────────────────────────────────────
def _load_noise_2nd() -> pd.DataFrame:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, "data", "processed", NOISE_2ND_FILE)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    df["Arr Date"] = pd.to_datetime(df["Arr Date"], errors="coerce")
    df["ADR"] = pd.to_numeric(df["ADR"], errors="coerce")
    df["Total Amount"] = pd.to_numeric(df["Total Amount"], errors="coerce")
    df["Nts"] = pd.to_numeric(df["Nts"], errors="coerce")
    df["Visit"] = pd.to_numeric(df["Visit"], errors="coerce")

    # 파생 변수
    df["year"]    = df["Arr Date"].dt.year.astype("Int64")
    df["quarter"] = df["Arr Date"].dt.quarter.astype("Int64")
    df["month"]   = df["Arr Date"].dt.month.astype("Int64")
    df["dayofweek"] = df["Arr Date"].dt.dayofweek.astype("Int64")   # 0=Mon
    df["dow_name"]  = df["Arr Date"].dt.day_name()
    df["season"] = df["month"].map({
        12: "겨울", 1: "겨울", 2: "겨울",
        3:  "봄",   4: "봄",   5: "봄",
        6:  "여름", 7: "여름", 8: "여름",
        9:  "가을", 10:"가을", 11:"가을",
    })
    return df


# ────────────────────────────────────────────────
# 헬퍼
# ────────────────────────────────────────────────
def _sep(lines: list, title: str):
    lines.append("")
    lines.append("=" * 62)
    lines.append(f"  {title}")
    lines.append("=" * 62)

def _sub(lines: list, title: str):
    lines.append(f"\n  [{title}]")


# ────────────────────────────────────────────────
# ① OTA 점유율 (Account 기준)
# ────────────────────────────────────────────────
def _report_ota(df: pd.DataFrame, lines: list):
    _sep(lines, "① OTA 점유율 (Account 기준)")
    acc_col = "Account" if "Account" in df.columns else "account"
    if acc_col not in df.columns:
        lines.append("  Account 컬럼 없음")
        return

    for period, grp_cols in [("연도별", ["year"]),
                               ("분기별", ["year", "quarter"]),
                               ("월별",   ["year", "month"])]:
        _sub(lines, period)
        for key, sub in df.groupby(grp_cols):
            total = len(sub)
            if total == 0:
                continue
            label = "-".join(str(k) for k in (key if isinstance(key, tuple) else (key,)))
            if period == "분기별":
                yr, q = key
                label = f"{int(yr)} Q{int(q)}"
            elif period == "월별":
                yr, m = key
                label = f"{int(yr)}-{int(m):02d}"
            counts = sub[acc_col].fillna("(미기입)").value_counts()
            top5 = counts.head(5)
            lines.append(f"  ▶ {label} (총 {total}건)")
            for acc, cnt in top5.items():
                lines.append(f"      {acc}: {cnt}건 ({cnt/total*100:.1f}%)")


# ────────────────────────────────────────────────
# ② 장/단기 투숙 비중 (Nts 기준)
# ────────────────────────────────────────────────
def _report_stay_length(df: pd.DataFrame, lines: list):
    _sep(lines, "② 장/단기 투숙 비중 (Nts 기준)")

    def _cat(n):
        if pd.isna(n):
            return "(미기입)"
        if n <= 3:
            return "단기(Short) 1~3박"
        if n <= 6:
            return "중기(Mid) 4~6박"
        return "장기(Long) 7박+"

    df = df.copy()
    df["stay_cat"] = df["Nts"].apply(_cat)
    total = len(df)
    for cat, cnt in df["stay_cat"].value_counts().items():
        lines.append(f"  {cat}: {cnt}건 ({cnt/total*100:.1f}%)")


# ────────────────────────────────────────────────
# ③ 시계열 트렌드 (Arr Date 기준)
# ────────────────────────────────────────────────
def _report_timeseries(df: pd.DataFrame, lines: list):
    _sep(lines, "③ 시계열 트렌드 (Arr Date 기준)")

    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dow_kr    = {"Monday":"월","Tuesday":"화","Wednesday":"수","Thursday":"목",
                 "Friday":"금","Saturday":"토","Sunday":"일"}

    # 요일별
    _sub(lines, "요일별 예약 건수")
    dow_cnt = df.groupby("dow_name")["Rsvn No"].count().reindex(dow_order, fill_value=0)
    for d, cnt in dow_cnt.items():
        lines.append(f"  {dow_kr.get(d,d)}: {cnt}건")
    peak_dow = dow_cnt.idxmax()
    low_dow  = dow_cnt.idxmin()
    lines.append(f"  → 가장 많음: {dow_kr.get(peak_dow,peak_dow)} ({dow_cnt[peak_dow]}건)  |  가장 적음: {dow_kr.get(low_dow,low_dow)} ({dow_cnt[low_dow]}건)")

    # 월별
    _sub(lines, "월별 예약 건수")
    mon_cnt = df.groupby(["year","month"])["Rsvn No"].count()
    for (yr, m), cnt in mon_cnt.items():
        lines.append(f"  {int(yr)}-{int(m):02d}: {cnt}건")
    peak_m = mon_cnt.idxmax()
    low_m  = mon_cnt.idxmin()
    lines.append(f"  → 가장 많음: {int(peak_m[0])}-{int(peak_m[1]):02d} ({mon_cnt[peak_m]}건)  |  가장 적음: {int(low_m[0])}-{int(low_m[1]):02d} ({mon_cnt[low_m]}건)")

    # 시즌별
    _sub(lines, "시즌별 예약 건수")
    season_cnt = df.groupby("season")["Rsvn No"].count().sort_values(ascending=False)
    for s, cnt in season_cnt.items():
        lines.append(f"  {s}: {cnt}건")
    lines.append(f"  → 가장 많음: {season_cnt.idxmax()} ({season_cnt.max()}건)  |  가장 적음: {season_cnt.idxmin()} ({season_cnt.min()}건)")


# ────────────────────────────────────────────────
# ④ 객실 타입별 점유율 (Rm Ty 기준)
# ────────────────────────────────────────────────
def _report_room_occ(df: pd.DataFrame, lines: list):
    _sep(lines, "④ 객실 타입별 점유율 (Rm Ty 기준)")

    date_min = df["Arr Date"].min()
    date_max = df["Arr Date"].max()
    total_days = (date_max - date_min).days + 1
    lines.append(f"  분석 기간: {date_min.date()} ~ {date_max.date()} ({total_days}일)")

    rm_cnt = df.groupby("Rm Ty")["Nts"].sum()
    for rtype, nts in rm_cnt.sort_values(ascending=False).items():
        inventory = ROOM_TYPE_INVENTORY.get(rtype)
        if inventory:
            denominator = inventory * total_days
            occ = nts / denominator * 100
            lines.append(f"  {rtype}: 누적 {int(nts)}박, OCC {occ:.1f}% (보유 {inventory}실 × {total_days}일)")
        else:
            lines.append(f"  {rtype}: 누적 {int(nts)}박 (인벤토리 정보 없음)")


# ────────────────────────────────────────────────
# ⑤ 재무 지표 (Total Amount, ADR 기준)
# ────────────────────────────────────────────────
def _report_finance(df: pd.DataFrame, lines: list):
    _sep(lines, "⑤ 재무 지표 (Total Amount, ADR 기준)")

    total_revenue = df["Total Amount"].sum()
    mean_adr      = df["ADR"].mean()
    lines.append(f"  총 매출액 (Total Amount): {total_revenue:,.0f}원")
    lines.append(f"  평균 객실 단가 (ADR 평균): {mean_adr:,.0f}원")

    # 월별 세부
    _sub(lines, "월별 매출액 및 평균 ADR")
    monthly = df.groupby(["year","month"]).agg(
        매출액=("Total Amount","sum"),
        평균ADR=("ADR","mean"),
        건수=("Rsvn No","count"),
    )
    for (yr, m), row in monthly.iterrows():
        lines.append(f"  {int(yr)}-{int(m):02d}: 매출 {row['매출액']:,.0f}원 | ADR {row['평균ADR']:,.0f}원 | {row['건수']}건")


# ────────────────────────────────────────────────
# ⑥ 국적별 유입률 Top 5 (Nat 기준)
# ────────────────────────────────────────────────
def _report_nationality(df: pd.DataFrame, lines: list):
    _sep(lines, "⑥ 국적별 유입률 Top 5 (Nat 기준)")
    total = len(df)
    nat_cnt = df["Nat"].fillna("(미기입)").value_counts()
    for i, (nat, cnt) in enumerate(nat_cnt.head(5).items(), 1):
        lines.append(f"  {i}위 {nat}: {cnt}건 ({cnt/total*100:.1f}%)")


# ────────────────────────────────────────────────
# ⑦ 신규/재방문 비중 (Visit 기준)
# ────────────────────────────────────────────────
def _report_visit(df: pd.DataFrame, lines: list):
    _sep(lines, "⑦ 신규/재방문 비중 (Visit 기준)")

    df = df.copy()
    df["visit_cat"] = df["Visit"].apply(
        lambda v: "신규(New)" if v == 1 else ("재방문(Return)" if v >= 2 else "(미기입)")
    )
    total = len(df)
    for cat, cnt in df["visit_cat"].value_counts().items():
        lines.append(f"  {cat}: {cnt}건 ({cnt/total*100:.1f}%)")


# ────────────────────────────────────────────────
# 메인 리포트 실행
# ────────────────────────────────────────────────
def generate_hotel_report():
    logger.info("호텔 BI 리포트 생성 시작")
    df = _load_noise_2nd()
    logger.info(f"데이터 로드 완료: {len(df)}행")

    lines = []
    lines.append("=" * 62)
    lines.append("  호텔 BI 분석 리포트")
    lines.append(f"  생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  분석 데이터: {NOISE_2ND_FILE}  ({len(df):,}건)")
    lines.append(f"  전체 객실 수: {INVENTORY_TOTAL}실")
    lines.append("=" * 62)

    _report_ota(df, lines)
    _report_stay_length(df, lines)
    _report_timeseries(df, lines)
    _report_room_occ(df, lines)
    _report_finance(df, lines)
    _report_nationality(df, lines)
    _report_visit(df, lines)

    lines.append("")
    lines.append("=" * 62)
    lines.append("  리포트 종료")
    lines.append("=" * 62)

    report_text = "\n".join(lines)

    # 터미널 출력
    print(report_text)

    # 파일 저장
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(base, "data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(out_dir, f"hotel_report_bi_{ts}.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info(f"리포트 저장: {report_path}")
    return report_path


if __name__ == "__main__":
    generate_hotel_report()
