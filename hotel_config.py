# 호텔 마스터 정보: 전체 객실 수(Inventory) 및 타입별 상수

# 전체 객실 수
INVENTORY_TOTAL = 99

# 타입별 객실 수
ROOM_TYPE_INVENTORY = {
    "STW": 32,
    "DDB": 11,
    "TRP": 11,
    "FTW": 35,
    "FDT": 9,
    "ASS": 1,
}

# 점유율 계산 시 사용할 일수 (월별 OCC용)
def get_days_in_month(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]
