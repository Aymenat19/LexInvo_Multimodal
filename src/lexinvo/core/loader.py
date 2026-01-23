"""Azure JSON → initial canonical invoice (Phase 0)."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from lexinvo.core.btstore import empty_btvalue
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
            record.source = "azure"
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
                record.source = "azure"
                record.confidence = field.get("confidence")
                record.evidence = {
                    "path": f"analyzeResult.documents[0].fields.Items.valueArray[{idx-1}].{key}"
                }
        lines.append(CanonicalLine(line_id=idx, bt=line_store))

    return CanonicalInvoice(header=header, lines=lines, totals=totals, raw=input_data, patches=[])
