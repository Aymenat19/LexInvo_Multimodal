"""LLM-based enrichment for invoice canonicalization."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI

from lexinvo.core.models import Patch
from lexinvo.core.pdf_audit import extract_pdf_text


SYSTEM_PROMPT = """You are an invoice extraction assistant.
Extract BT fields from the PDF and reconcile with the provided extracted JSON.
Return only JSON that matches the schema. Do not include explanations."""

USER_PROMPT = """Goal:
- Compare extracted JSON vs the PDF.
- Correct wrong values, fill missing values.
- Prefer explicit values in the PDF (totals, dates, VAT, payment terms).

Rules (basic subset):
- Normalize dates to YYYY-MM-DD.
- Normalize numbers to dot decimals (e.g., 1.947,75 -> 1947.75).
- BT-106 = sum of line net amounts (BT-131).
- If BT-131 missing: BT-131 = BT-146 * BT-129 * (1 - BT-138%).
- BT-107 = sum of BT-92 (invoice-level only).
- BT-108 = sum of BT-99 (invoice-level only).
- BT-109 = BT-106 + BT-108.
- If single VAT rate exists: BT-110 = BT-109 * VAT%.
- BT-112 = BT-109 + BT-110.
- If immediate payment (Vorkasse/online/etc) and BT-20 has Skonto X% (Y EUR): BT-92=Y, BT-94=X.
- If immediate payment: BT-113 = BT-112 - BT-107, BT-115 = BT-112 - BT-113 - BT-107.

Return header/totals/lines BT values only for fields you are confident about.
"""


def _json_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "header": {
                "type": "object",
                "additionalProperties": {"type": ["string", "number", "null"]},
            },
            "totals": {
                "type": "object",
                "additionalProperties": {"type": ["string", "number", "null"]},
            },
            "lines": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "line_id": {"type": "integer"},
                        "bt": {
                            "type": "object",
                            "additionalProperties": {"type": ["string", "number", "null"]},
                        },
                    },
                    "required": ["line_id", "bt"],
                },
            },
            "confidence_notes": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["header", "totals", "lines"],
    }


def _call_openai(model: str, pdf_text: str, extracted_json: Dict[str, Any]) -> Dict[str, Any]:
    client = OpenAI()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": USER_PROMPT
                + "\n\nPDF_TEXT:\n"
                + pdf_text[:200000]
                + "\n\nEXTRACTED_JSON:\n"
                + json.dumps(extracted_json, ensure_ascii=True)[:200000],
            },
        ],
        response_format={"type": "json_schema", "json_schema": {"name": "bt_extraction", "schema": _json_schema()}},
        temperature=0,
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def build_patches(llm_output: Dict[str, Any]) -> List[Patch]:
    patches: List[Patch] = []
    header = llm_output.get("header", {}) or {}
    totals = llm_output.get("totals", {}) or {}
    lines = llm_output.get("lines", []) or []

    for bt, value in header.items():
        if value is None or value == "":
            continue
        patches.append(
            Patch(
                scope="header",
                bt=bt,
                new_value=value,
                status="corrected",
                source="multimodal",
                derivation="LLM enrichment",
                rule_id="R-LLM-HEADER-001",
            )
        )

    for bt, value in totals.items():
        if value is None or value == "":
            continue
        patches.append(
            Patch(
                scope="totals",
                bt=bt,
                new_value=value,
                status="corrected",
                source="multimodal",
                derivation="LLM enrichment",
                rule_id="R-LLM-TOTALS-001",
            )
        )

    for line in lines:
        line_id = line.get("line_id")
        bts = line.get("bt", {})
        if line_id is None:
            continue
        for bt, value in bts.items():
            if value is None or value == "":
                continue
            patches.append(
                Patch(
                    scope="line",
                    bt=bt,
                    new_value=value,
                    line_id=int(line_id),
                    status="corrected",
                    source="multimodal",
                    derivation="LLM enrichment",
                    rule_id="R-LLM-LINE-001",
                )
            )

    return patches


def enrich_with_gpt(
    input_data: Dict[str, Any],
    pdf_path: Optional[str],
    model: str,
) -> List[Patch]:
    if not os.getenv("OPENAI_API_KEY"):
        return []
    if not pdf_path:
        pdf_text = ""
    else:
        pdf_text = extract_pdf_text(Path(pdf_path))
    if not pdf_text and not input_data:
        return []
    llm_output = _call_openai(model, pdf_text, input_data)
    return build_patches(llm_output)
