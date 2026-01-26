"""Normalization helpers."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional


DECIMAL_RE = re.compile(r"[^0-9,\.-]+")


def parse_decimal(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = DECIMAL_RE.sub("", text)
    if "," in text and "." in text:
        # Decide decimal separator by last occurrence.
        last_comma = text.rfind(",")
        last_dot = text.rfind(".")
        if last_comma > last_dot:
            # European format: 1.234,56 -> 1234.56
            text = text.replace(".", "").replace(",", ".")
        else:
            # US format: 1,234.56 -> 1234.56
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_date_to_iso(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return None
    # dd.mm.yyyy
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", text)
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    # yyyy-mm-dd
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if m:
        return text
    return None


def normalize_country(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    mapping = {
        "deutschland": "DE",
        "germany": "DE",
        "de": "DE",
    }
    key = text.lower()
    if key in mapping:
        return mapping[key]
    if len(text) == 2 and text.isalpha():
        return text.upper()
    return None


def normalize_vat_id(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return re.sub(r"\s+", "", text)


def normalize_email(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text
