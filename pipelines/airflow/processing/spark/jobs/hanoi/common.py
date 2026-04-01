"""Shared utilities for Hanoi Tourism Silver builders.

Mirrors the pattern from silver/common.py in vuong.ngo's retail domain.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.column import Column
from pyspark.sql.window import Window


# ─── District extraction UDF ──────────────────────────────────────────────────

# Mapping keywords in formatted_address → quận/huyện chuẩn hoá
_DISTRICT_MAP = {
    "Hoàn Kiếm": ["Hoàn Kiếm", "Hoan Kiem"],
    "Ba Đình":   ["Ba Đình", "Ba Dinh"],
    "Đống Đa":   ["Đống Đa", "Dong Da"],
    "Hai Bà Trưng": ["Hai Bà Trưng", "Hai Ba Trung"],
    "Cầu Giấy":  ["Cầu Giấy", "Cau Giay"],
    "Tây Hồ":    ["Tây Hồ", "Tay Ho"],
    "Long Biên":  ["Long Biên", "Long Bien"],
    "Thanh Xuân": ["Thanh Xuân", "Thanh Xuan"],
    "Hoàng Mai":  ["Hoàng Mai", "Hoang Mai"],
    "Hà Đông":    ["Hà Đông", "Ha Dong"],
    "Nam Từ Liêm":["Nam Từ Liêm", "Nam Tu Liem"],
    "Bắc Từ Liêm":["Bắc Từ Liêm", "Bac Tu Liem"],
    "Gia Lâm":    ["Gia Lâm", "Gia Lam"],
    "Đông Anh":   ["Đông Anh", "Dong Anh"],
    "Sóc Sơn":    ["Sóc Sơn", "Soc Son"],
}


def _extract_district(address: str | None) -> str | None:
    """Extract Hà Nội district name from a formatted address string."""
    if not address:
        return None
    addr_lower = address.lower()
    for district, keywords in _DISTRICT_MAP.items():
        for kw in keywords:
            if kw.lower() in addr_lower:
                return district
    # Fallback: parse 'Quận X' or 'Huyện X' pattern
    match = re.search(r"(quận|huyện)\s+([\w\s]+?)(?:,|$)", address, re.IGNORECASE)
    if match:
        return match.group(2).strip().title()
    return "Không xác định"


extract_district_udf = F.udf(_extract_district, T.StringType())


# ─── Attraction type normaliser ───────────────────────────────────────────────

_TYPE_PRIORITY = [
    ("museum",              "Bảo tàng"),
    ("art_gallery",         "Phòng tranh"),
    ("hindu_temple",        "Đền / Chùa"),
    ("place_of_worship",    "Đền / Chùa"),
    ("church",              "Nhà thờ"),
    ("amusement_park",      "Công viên giải trí"),
    ("zoo",                 "Vườn thú"),
    ("aquarium",            "Thủy cung"),
    ("park",                "Công viên"),
    ("natural_feature",     "Danh thắng thiên nhiên"),
    ("tourist_attraction",  "Điểm tham quan"),
    ("point_of_interest",   "Điểm quan tâm"),
    ("establishment",       "Cơ sở"),
]


def _classify_type(types_json: str | None) -> str:
    """Return a human-readable Vietnamese category from the types JSON array."""
    if not types_json:
        return "Khác"
    try:
        import json
        types_list: list[str] = json.loads(types_json)
    except Exception:
        return "Khác"
    for raw_type, label in _TYPE_PRIORITY:
        if raw_type in types_list:
            return label
    return "Khác"


classify_type_udf = F.udf(_classify_type, T.StringType())


# ─── Surrogate key (same as retail pattern) ───────────────────────────────────

def surrogate_key(*cols: Column) -> Column:
    """Deterministic surrogate key using xxhash64 (same as silver/common.py)."""
    return F.abs(F.xxhash64(*cols))
