"""Canonicalization helpers (BTValue + patch application)."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from lexinvo.core.models import BTValue, CanonicalInvoice, Patch


def empty_btvalue(bt: str) -> BTValue:
    return BTValue(bt=bt, value=None, raw_value=None, status="missing", source="azure")


def apply_patch(invoice: CanonicalInvoice, patch: Patch) -> None:
    if patch.scope == "header":
        record = invoice.header.get(patch.bt)
    elif patch.scope == "totals":
        record = invoice.totals.get(patch.bt)
    else:
        record = None
        for line in invoice.lines:
            if line.line_id == patch.line_id:
                record = line.bt.get(patch.bt)
                break

    if record is None:
        return

    old_value = record.value
    record.value = patch.new_value
    record.status = patch.status
    record.source = patch.source
    record.derivation = patch.derivation
    record.rule_id = patch.rule_id
    record.evidence = patch.evidence

    invoice.patches.append(
        {
            "bt": patch.bt,
            "scope": patch.scope,
            "line_id": patch.line_id,
            "old_value": old_value,
            "new_value": patch.new_value,
            "status": patch.status,
            "derivation": patch.derivation,
            "rule_id": patch.rule_id,
        }
    )


def btvalue_to_dict(value: BTValue) -> Dict[str, Any]:
    data = asdict(value)
    return data


def invoice_to_dict(invoice: CanonicalInvoice) -> Dict[str, Any]:
    return {
        "header": {bt: btvalue_to_dict(v) for bt, v in invoice.header.items()},
        "totals": {bt: btvalue_to_dict(v) for bt, v in invoice.totals.items()},
        "lines": [
            {
                "line_id": line.line_id,
                "bt": {bt: btvalue_to_dict(v) for bt, v in line.bt.items()},
            }
            for line in invoice.lines
        ],
        "raw": invoice.raw,
        "patches": invoice.patches,
    }
