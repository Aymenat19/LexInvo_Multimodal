"""Local PDF audit and enrichment for Azure JSON extractions."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pypdf import PdfReader

from lexinvo.utils.normalize import parse_date_to_iso, parse_decimal

INVOICE_NO_PATTERNS = [
    r"\b(?:Rechnungs-Nr\.?|Rechnungsnummer|Invoice No\.?|Invoice #|Rechnung Nr\.?)\b\s*[:#]?\s*([A-Za-z0-9/\\-]+)",
]

DATE_LABELS = [
    "Rechnungsdatum",
    "Invoice date",
    "Datum",
]

DUE_DATE_LABELS = [
    "Zahlbar bis",
    "Fällig",
    "Due date",
]

TOTAL_WITH_VAT_LABELS = [
    "Gesamtbetrag in EUR",
    "Gesamtbetrag",
    "Invoice total",
    "Total amount",
    "Bruttobetrag",
]

TOTAL_WITHOUT_VAT_LABELS = [
    "Zwischensumme",
    "Nettobetrag",
    "Gesamtsumme netto",
    "Berechnungsgrundlage",
    "Total without VAT",
    "Subtotal",
]

VAT_TOTAL_LABELS = [
    "MwSt",
    "USt",
    "VAT",
]

AMOUNT_DUE_LABELS = [
    "Zahlbetrag",
    "Amount due",
    "Betrag fällig",
]

VAT_ID_RE = re.compile(r"\b[A-Z]{2}(?:[\s\-]?\d){8,12}\b")


def extract_pdf_text(pdf_path: Path) -> str:
    if not pdf_path.exists():
        return ""
    reader = PdfReader(str(pdf_path))
    pages = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(text)
    return "\n".join(pages).strip()


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def _get_field_value(field: Dict[str, Any]) -> Optional[Any]:
    if "valueString" in field:
        return field.get("valueString")
    if "valueDate" in field:
        return field.get("valueDate")
    if "valueNumber" in field:
        return field.get("valueNumber")
    return field.get("content")


def _is_missing(field: Optional[Dict[str, Any]]) -> bool:
    if not field:
        return True
    return _get_field_value(field) in (None, "", [])


def _find_field(fields: Dict[str, Any], bt: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    for key, value in fields.items():
        if key.startswith(bt):
            return value, key
    return None, None


def _looks_like_terms(text: str) -> bool:
    if not text:
        return False
    # Require at least a date, a percentage, or a currency/amount.
    has_date = bool(re.search(r"\b\d{2}\.\d{2}\.\d{4}\b|\b\d{4}-\d{2}-\d{2}\b", text))
    has_percent = "%" in text
    has_amount = bool(re.search(r"\d+[\\.,]\\d{2}", text)) or "EUR" in text or "€" in text
    return has_date or has_percent or has_amount


def _is_incorrect(field: Dict[str, Any], extracted: object, value_type: str, text: str) -> bool:
    existing = _get_field_value(field)
    if existing in (None, "", []) or extracted is None:
        return False
    if value_type == "number":
        existing_num = parse_decimal(existing)
        extracted_num = parse_decimal(extracted)
        if existing_num is None or extracted_num is None:
            return False
        return abs(existing_num - extracted_num) > 0.01
    if value_type == "date":
        existing_iso = parse_date_to_iso(existing)
        extracted_iso = parse_date_to_iso(extracted)
        if existing_iso is None or extracted_iso is None:
            return False
        return existing_iso != extracted_iso
    existing_norm = _normalize_text(existing)
    extracted_norm = _normalize_text(extracted)
    if not existing_norm or not extracted_norm:
        return False
    # Only correct if extracted appears in PDF and existing does not.
    return extracted_norm in _normalize_text(text) and existing_norm not in _normalize_text(text)


def _extract_first(patterns: list[str], text: str) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def _find_code_near_label(labels: list[str], lines: list[str], *, max_lookahead: int = 4) -> Optional[str]:
    for idx, line in enumerate(lines):
        if not any(label.lower() in line.lower() for label in labels):
            continue
        for offset in range(1, max_lookahead + 1):
            if idx + offset >= len(lines):
                break
            candidate = lines[idx + offset].strip()
            if not candidate:
                continue
            if re.search(r"[0-9]", candidate):
                return candidate.split()[0]
    return None


def _find_due_date_from_text(lines: list[str]) -> Optional[str]:
    for idx, line in enumerate(lines):
        if "zahlbar bis" not in line.lower():
            continue
        for offset in range(0, 6):
            if idx + offset >= len(lines):
                break
            candidate = lines[idx + offset]
            match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", candidate)
            if match:
                return match.group(0)
    return None


def _find_date_after(labels: list[str], lines: list[str]) -> Optional[str]:
    for idx, line in enumerate(lines):
        for label in labels:
            if label.lower() in line.lower():
                date_match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", line)
                if date_match:
                    return date_match.group(0)
                date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", line)
                if date_match:
                    return date_match.group(0)
                for offset in range(1, 4):
                    if idx + offset >= len(lines):
                        break
                    candidate = lines[idx + offset]
                    date_match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", candidate)
                    if date_match:
                        return date_match.group(0)
                    date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", candidate)
                    if date_match:
                        return date_match.group(0)
    return None


def _find_amount_after(
    labels: list[str],
    lines: list[str],
    *,
    reject_percent: bool = False,
    reject_words: Optional[list[str]] = None,
) -> Optional[float]:
    for idx, line in enumerate(lines):
        for label in labels:
            if label.lower() in line.lower():
                if reject_percent and "%" in line:
                    continue
                if VAT_ID_RE.search(line):
                    continue
                if reject_words and any(word in line.lower() for word in reject_words):
                    continue
                parts = line.split(":")
                amount_text = parts[-1] if len(parts) > 1 else line.replace(label, "")
                amount = parse_decimal(amount_text)
                if amount is None and idx + 1 < len(lines):
                    amount = parse_decimal(lines[idx + 1])
                if amount is not None:
                    return amount
    return None


def _is_amount_only(line: str) -> bool:
    return bool(re.match(r"^-?\s*[0-9][0-9.,]*\s*(?:€)?\s*$", line))


def _amounts_near_label(label: str, lines: list[str], *, max_lookahead: int = 8) -> list[float]:
    for idx, line in enumerate(lines):
        if label.lower() not in line.lower():
            continue
        amounts = []
        for offset in range(1, max_lookahead + 1):
            if idx + offset >= len(lines):
                break
            candidate = lines[idx + offset]
            if not _is_amount_only(candidate):
                continue
            amount = parse_decimal(candidate)
            if amount is not None:
                amounts.append(amount)
        if amounts:
            return amounts
    return []


def _extract_currency(text: str) -> Optional[str]:
    if "EUR" in text:
        return "EUR"
    if "USD" in text or "$" in text:
        return "USD"
    if "GBP" in text:
        return "GBP"
    return None


def _extract_vat_id(text: str) -> Optional[str]:
    match = re.search(r"\b[A-Z]{2}\s?\d{8,12}\b", text)
    if match:
        return match.group(0).replace(" ", "")
    return None


def _set_field(
    fields: Dict[str, Any],
    bt: str,
    value: object,
    *,
    value_type: str,
    content: Optional[str] = None,
    reason: str,
) -> None:
    if value is None:
        return
    target_key = bt
    for key in fields.keys():
        if key.startswith(bt):
            target_key = key
            break
    payload: Dict[str, Any] = {
        "content": content if content is not None else str(value),
        "confidence": 0.25,
        "source": "pdf_audit",
        "reason": reason,
    }
    if value_type == "number":
        payload["valueNumber"] = float(value)
    elif value_type == "date":
        iso = parse_date_to_iso(value)
        if iso:
            payload["valueDate"] = iso
            payload["content"] = content if content is not None else iso
        else:
            payload["valueString"] = str(value)
    else:
        payload["valueString"] = str(value)
    fields[target_key] = payload


def audit_and_enrich(input_data: Dict[str, Any], pdf_path: Path) -> Tuple[Dict[str, Any], str]:
    text = extract_pdf_text(pdf_path)
    if not text:
        return input_data, ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    analyze = input_data.setdefault("analyzeResult", {})
    existing_content = analyze.get("content", "")
    if text and text not in existing_content:
        analyze["content"] = "\n".join([existing_content, text]).strip() if existing_content else text

    documents = analyze.setdefault("documents", [])
    if not documents:
        documents.append({"fields": {}})
    fields: Dict[str, Any] = documents[0].setdefault("fields", {})

    servicepaket_amounts = _amounts_near_label("Servicepaket", lines)
    servicepaket_amount = next((a for a in servicepaket_amounts if a >= 0), None)

    invoice_no = _extract_first(INVOICE_NO_PATTERNS, text) or _find_code_near_label(
        ["Nummer", "Rechnungs-Nr", "Rechnungsnummer", "Invoice No", "Invoice #"], lines
    )

    payment_terms = _extract_first([r"(Zahlbar.+)", r"(Payment terms.+)"], text)
    if payment_terms and not _looks_like_terms(payment_terms):
        payment_terms = None
    due_from_text = _find_due_date_from_text(lines)
    if payment_terms is None and due_from_text:
        payment_terms = f"Zahlbar bis {due_from_text}"

    candidates = {
        "BT-1": ("string", invoice_no),
        "BT-2": ("date", _find_date_after(DATE_LABELS, lines)),
        "BT-9": ("date", due_from_text or _find_date_after(DUE_DATE_LABELS, lines)),
        "BT-5": ("string", _extract_currency(text)),
        "BT-20": ("string", payment_terms),
        "BT-31": ("string", _extract_vat_id(text)),
        "BT-112": ("number", _find_amount_after(TOTAL_WITH_VAT_LABELS, lines)),
        "BT-109": ("number", _find_amount_after(TOTAL_WITHOUT_VAT_LABELS, lines, reject_words=["versand", "kostenlos", "ab"])),
        "BT-110": ("number", _find_amount_after(VAT_TOTAL_LABELS, lines, reject_percent=True, reject_words=["inkl"])),
        "BT-115": ("number", _find_amount_after(AMOUNT_DUE_LABELS, lines)),
        "BT-92": ("number", servicepaket_amount),
    }

    for bt, (value_type, extracted) in candidates.items():
        field, _ = _find_field(fields, bt)
        existing = _get_field_value(field) if field else None
        # Avoid overwriting richer payment terms with a shorter snippet.
        if bt == "BT-20" and existing and extracted:
            if len(str(existing)) >= len(str(extracted)):
                continue
        if _is_missing(field):
            _set_field(fields, bt, extracted, value_type=value_type, reason="missing")
        elif _is_incorrect(field, extracted, value_type, text):
            _set_field(fields, bt, extracted, value_type=value_type, reason="incorrect")

    voucher_amounts = _amounts_near_label("Eingelöster Gutschein", lines) or _amounts_near_label("Gutschein", lines)
    line_discount = next((a for a in voucher_amounts if a < 0), None)
    if line_discount is not None:
        amount = abs(line_discount)
        items = fields.get("Items", {}).get("valueArray", [])
        if items:
            item = items[0]
            value_object = item.setdefault("valueObject", {})
            value_object["BT-147_Invoice-line-allowance-amount"] = {
                "content": f"-{amount:.2f}" if line_discount < 0 else f"{amount:.2f}",
                "valueNumber": float(amount),
                "confidence": 0.25,
                "source": "pdf_audit",
                "reason": "missing",
            }

    return input_data, text
