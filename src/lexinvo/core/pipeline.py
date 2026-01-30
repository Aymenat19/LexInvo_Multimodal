"""End-to-end pipeline using canonicalization phases."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from lexinvo.core.btstore import apply_patch, invoice_to_dict
from lexinvo.core.gpt_enrich import enrich_with_gpt
from lexinvo.core.loader import load_azure
from lexinvo.core.pdf_audit import audit_and_enrich
from lexinvo.core.report import build_report
from lexinvo.core.rules_engine import phase1_normalize, phase2_derive, phase3_validate, phase4_resolve


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def run_pipeline(input_path: str | None, output_dir: str, config_dir: str, data_dir: str, pdf_path: str | None = None) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    input_data = _load_json(Path(input_path)) if input_path else {}
    bt_registry = _load_json(Path(config_dir) / "bt_registry.json")

    if pdf_path:
        input_data, _ = audit_and_enrich(input_data, Path(pdf_path))

    invoice = load_azure(input_data, bt_registry)

    use_gpt = os.getenv("LEXINVO_USE_GPT", "").lower() in {"1", "true", "yes"}
    if pdf_path and os.getenv("LEXINVO_USE_GPT", "") == "":
        use_gpt = True
    if use_gpt:
        model = os.getenv("LEXINVO_GPT_MODEL", "gpt-4o-mini")
        for patch in enrich_with_gpt(input_data, pdf_path, model):
            apply_patch(invoice, patch)

    for patch in phase1_normalize(invoice):
        apply_patch(invoice, patch)
    for patch in phase2_derive(invoice):
        apply_patch(invoice, patch)
    for patch in phase3_validate(invoice):
        apply_patch(invoice, patch)
    for patch in phase4_resolve(invoice):
        apply_patch(invoice, patch)
    # Re-run derivations/validations that depend on Phase 4 signals (e.g., BT-81)
    for patch in phase2_derive(invoice):
        apply_patch(invoice, patch)
    for patch in phase4_resolve(invoice):
        apply_patch(invoice, patch)
    for patch in phase3_validate(invoice):
        apply_patch(invoice, patch)

    corrections_report = build_report(invoice)

    canonical_invoice = invoice_to_dict(invoice)

    en16931_basic = {
        "profile": {
            "specification_identifier": invoice.header.get("BT-24").value if invoice.header.get("BT-24") else None,
            "invoice_type_code": invoice.header.get("BT-3").value if invoice.header.get("BT-3") else None,
        },
        "header": {
            "invoice_number": invoice.header.get("BT-1").value if invoice.header.get("BT-1") else None,
            "issue_date": invoice.header.get("BT-2").value if invoice.header.get("BT-2") else None,
            "currency": invoice.header.get("BT-5").value if invoice.header.get("BT-5") else None,
            "note_subject_code": invoice.header.get("BT-21").value if invoice.header.get("BT-21") else None,
            "payment_terms": invoice.header.get("BT-20").value if invoice.header.get("BT-20") else None,
        },
        "seller": {
            "name": invoice.header.get("BT-27").value if invoice.header.get("BT-27") else None,
            "vat_id": invoice.header.get("BT-31").value if invoice.header.get("BT-31") else None,
        },
        "buyer": {
            "name": invoice.header.get("BT-44").value if invoice.header.get("BT-44") else None,
        },
        "lines": [
            {bt: val.value for bt, val in line.bt.items()}
            for line in invoice.lines
        ],
        "totals": {
            "sum_line_net": invoice.totals.get("BT-106").value if invoice.totals.get("BT-106") else None,
            "total_without_vat": invoice.totals.get("BT-109").value if invoice.totals.get("BT-109") else None,
            "vat_total": invoice.totals.get("BT-110").value if invoice.totals.get("BT-110") else None,
            "total_with_vat": invoice.totals.get("BT-112").value if invoice.totals.get("BT-112") else None,
            "amount_due": invoice.totals.get("BT-115").value if invoice.totals.get("BT-115") else None,
        },
    }

    _write_json(out_dir / "canonical_invoice.json", canonical_invoice)
    _write_json(out_dir / "bt_store.json", canonical_invoice)
    _write_json(out_dir / "corrections_report.json", corrections_report)
    _write_json(out_dir / "en16931_basic.json", en16931_basic)
