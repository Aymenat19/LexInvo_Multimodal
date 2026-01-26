"""Azure JSON → initial canonical invoice (Phase 0)."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from lexinvo.core.btstore import empty_btvalue
from lexinvo.utils.normalize import parse_decimal
from lexinvo.core.models import BTValue, CanonicalInvoice, CanonicalLine

BT_RE = re.compile(r"(BT-\d+)")


def _extract_bt_code(key: str) -> str | None:
    match = BT_RE.search(key)
    if not match:
        return None
    return match.group(1)


def _extract_value(field: Dict[str, Any]) -> Tuple[Any, Any]:
    # Prefer content as raw_value when present; value* is normalized value.
    raw_value = field.get("content")
    value = None
    if "valueString" in field:
        value = field.get("valueString")
    elif "valueDate" in field:
        value = field.get("valueDate")
    elif "valueNumber" in field:
        value = field.get("valueNumber")
    if raw_value is None:
        raw_value = value
    return value, raw_value


def _group_for_bt(bt_registry: Dict[str, Any], bt: str) -> str:
    spec = bt_registry.get(bt, {})
    group = spec.get("group", "header")
    if group in {"totals", "allowances", "charges"}:
        return "totals"
    return "header"


def _init_scope(bt_registry: Dict[str, Any], group: str) -> Dict[str, BTValue]:
    return {bt: empty_btvalue(bt) for bt, spec in bt_registry.items() if spec.get("group") == group}


def load_azure(input_data: Dict[str, Any], bt_registry: Dict[str, Any]) -> CanonicalInvoice:
    header = {bt: empty_btvalue(bt) for bt, _spec in bt_registry.items() if _group_for_bt(bt_registry, bt) != "totals"}
    totals = {bt: empty_btvalue(bt) for bt, _spec in bt_registry.items() if _group_for_bt(bt_registry, bt) == "totals"}
    lines: List[CanonicalLine] = []

    analyze = input_data.get("analyzeResult", {})
    documents = analyze.get("documents", [])
    doc = documents[0] if documents else {}
    fields = doc.get("fields", {})

    # Header-level fields
    for key, field in fields.items():
        if key == "Items":
            continue
        bt_code = _extract_bt_code(key)
        if not bt_code:
            continue
        value, raw_value = _extract_value(field)
        status = "ok" if value not in (None, "") else "missing"
        record = header.get(bt_code) or totals.get(bt_code)
        if record:
            record.value = value
            record.raw_value = raw_value
            record.status = status
            record.source = field.get("source", "azure")
            record.confidence = field.get("confidence")
            record.evidence = {"path": f"analyzeResult.documents[0].fields.{key}"}

    # Line items
    items = fields.get("Items", {}).get("valueArray", [])
    for idx, item in enumerate(items, start=1):
        value_object = item.get("valueObject", {})
        line_store = {bt: empty_btvalue(bt) for bt, spec in bt_registry.items() if spec.get("group") == "line"}
        for key, field in value_object.items():
            bt_code = _extract_bt_code(key)
            if not bt_code or bt_code not in line_store:
                continue
            value, raw_value = _extract_value(field)
            status = "ok" if value not in (None, "") else "missing"
            record = line_store.get(bt_code)
            if record:
                record.value = value
                record.raw_value = raw_value
                record.status = status
                record.source = field.get("source", "azure")
                record.confidence = field.get("confidence")
                record.evidence = {
                    "path": f"analyzeResult.documents[0].fields.Items.valueArray[{idx-1}].{key}"
                }
        lines.append(CanonicalLine(line_id=idx, bt=line_store))

    # Generic arrays (e.g., PaymentDetails, Taxes, Allowances & Charges)
    def _apply_array_field(path_root: str, array_items: List[Dict[str, Any]]) -> None:
        for idx, item in enumerate(array_items):
            value_object = item.get("valueObject", {})
            for key, field in value_object.items():
                bt_code = _extract_bt_code(key)
                if not bt_code:
                    continue
                value, raw_value = _extract_value(field)
                record = header.get(bt_code) or totals.get(bt_code)
                if not record or record.value not in (None, ""):
                    continue
                record.value = value
                record.raw_value = raw_value
                record.status = "ok" if value not in (None, "") else "missing"
                record.source = field.get("source", "azure")
                record.confidence = field.get("confidence")
                record.evidence = {"path": f"{path_root}.valueArray[{idx}].{key}"}

    payment_details = fields.get("PaymentDetails", {}).get("valueArray", [])
    if payment_details:
        _apply_array_field("analyzeResult.documents[0].fields.PaymentDetails", payment_details)
        bt84_values: List[str] = []
        bt86_values: List[str] = []
        for item in payment_details:
            value_object = item.get("valueObject", {})
            for key, field in value_object.items():
                bt_code = _extract_bt_code(key)
                if bt_code not in {"BT-84", "BT-86"}:
                    continue
                value, raw_value = _extract_value(field)
                text = str(raw_value if raw_value is not None else value).strip()
                if not text:
                    continue
                if bt_code == "BT-84":
                    bt84_values.append(text)
                else:
                    for token in text.split():
                        if token.upper() != "NONE":
                            bt86_values.append(token)
        if bt84_values:
            record = header.get("BT-84") or totals.get("BT-84")
            if record:
                joined = "; ".join(dict.fromkeys(bt84_values))
                record.value = joined
                record.raw_value = joined
                record.status = "ok"
                record.source = "azure"
                record.evidence = {"path": "analyzeResult.documents[0].fields.PaymentDetails.valueArray"}
        if bt86_values:
            record = header.get("BT-86") or totals.get("BT-86")
            if record:
                joined = "; ".join(dict.fromkeys(bt86_values))
                record.value = joined
                record.raw_value = joined
                record.status = "ok"
                record.source = "azure"
                record.evidence = {"path": "analyzeResult.documents[0].fields.PaymentDetails.valueArray"}

    # Allowances & Charges (document-level)
    charge_amounts: List[float] = []
    allowance_amounts: List[float] = []
    allow_charge_items = fields.get("Allowances & Charges", {}).get("valueArray", [])
    if allow_charge_items:
        _apply_array_field("analyzeResult.documents[0].fields.Allowances & Charges", allow_charge_items)
    for idx, item in enumerate(allow_charge_items):
        value_object = item.get("valueObject", {})
        for key, field in value_object.items():
            bt_code = _extract_bt_code(key)
            if not bt_code:
                continue
            value, raw_value = _extract_value(field)
            record = totals.get(bt_code)
            if not record:
                continue
            if bt_code == "BT-99":
                amount = parse_decimal(value if value is not None else raw_value)
                if amount is not None and amount > 0:
                    charge_amounts.append(amount)
                continue
            if bt_code == "BT-92":
                amount = parse_decimal(value if value is not None else raw_value)
                if amount is not None and amount > 0:
                    allowance_amounts.append(amount)
                continue
            if record.value in (None, ""):
                record.value = value
                record.raw_value = raw_value
                record.status = "ok" if value not in (None, "") else "missing"
                record.source = field.get("source", "azure")
                record.confidence = field.get("confidence")
                record.evidence = {
                    "path": f"analyzeResult.documents[0].fields.Allowances & Charges.valueArray[{idx}].{key}"
                }

    if charge_amounts:
        bt99 = totals.get("BT-99")
        if bt99 and not bt99.value:
            total = round(sum(charge_amounts), 2)
            bt99.value = f"{total:.2f}"
            bt99.raw_value = bt99.value
            bt99.status = "ok"
            bt99.source = "azure"
            bt99.evidence = {"path": "analyzeResult.documents[0].fields.Allowances & Charges.valueArray"}

    if allowance_amounts:
        bt92 = totals.get("BT-92")
        if bt92 and not bt92.value:
            total = round(sum(allowance_amounts), 2)
            bt92.value = f"{total:.2f}"
            bt92.raw_value = bt92.value
            bt92.status = "ok"
            bt92.source = "azure"
            bt92.evidence = {"path": "analyzeResult.documents[0].fields.Allowances & Charges.valueArray"}

    # Taxes (document-level)
    vat_amounts: List[float] = []
    taxable_amounts: List[float] = []
    taxes_items = fields.get("Taxes", {}).get("valueArray", [])
    if taxes_items:
        _apply_array_field("analyzeResult.documents[0].fields.Taxes", taxes_items)
    for idx, item in enumerate(taxes_items):
        value_object = item.get("valueObject", {})
        for key, field in value_object.items():
            bt_code = _extract_bt_code(key)
            if not bt_code:
                continue
            value, raw_value = _extract_value(field)
            if bt_code == "BT-110":
                amount = parse_decimal(value if value is not None else raw_value)
                if amount is not None:
                    vat_amounts.append(amount)
                continue
            if bt_code == "BT-116":
                amount = parse_decimal(value if value is not None else raw_value)
                if amount is not None:
                    taxable_amounts.append(amount)
                continue
            record = totals.get(bt_code)
            if record and record.value in (None, ""):
                record.value = value
                record.raw_value = raw_value
                record.status = "ok" if value not in (None, "") else "missing"
                record.source = field.get("source", "azure")
                record.confidence = field.get("confidence")
                record.evidence = {
                    "path": f"analyzeResult.documents[0].fields.Taxes.valueArray[{idx}].{key}"
                }

    if vat_amounts:
        bt110 = totals.get("BT-110")
        if bt110 and not bt110.value:
            total = round(sum(vat_amounts), 2)
            bt110.value = f"{total:.2f}"
            bt110.raw_value = bt110.value
            bt110.status = "ok"
            bt110.source = "azure"
            bt110.evidence = {"path": "analyzeResult.documents[0].fields.Taxes.valueArray"}

    if taxable_amounts:
        bt116 = totals.get("BT-116")
        if bt116 and not bt116.value:
            total = round(sum(taxable_amounts), 2)
            bt116.value = f"{total:.2f}"
            bt116.raw_value = bt116.value
            bt116.status = "ok"
            bt116.source = "azure"
            bt116.evidence = {"path": "analyzeResult.documents[0].fields.Taxes.valueArray"}

    return CanonicalInvoice(header=header, lines=lines, totals=totals, raw=input_data, patches=[])
