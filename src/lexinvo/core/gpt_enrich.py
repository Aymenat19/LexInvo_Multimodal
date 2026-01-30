"""LLM-based enrichment for invoice canonicalization."""

from __future__ import annotations

import base64
import json
import os
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI
from pypdfium2 import PdfDocument

from lexinvo.core.models import Patch
from lexinvo.core.pdf_audit import extract_pdf_text


SYSTEM_PROMPT = """You are an invoice extraction assistant.
Extract BT fields from the PDF and reconcile with the provided extracted JSON.
Return only JSON that matches the schema. Do not include explanations."""

BT_LIST = [
    "BT-24",
    "BT-1",
    "BT-3",
    "BT-2",
    "BT-21",
    "BT-126",
    "BT-157",
    "BT-153",
    "BT-154",
    "BT-147",
    "BT-146",
    "BT-149",
    "BT-129",
    "BT-130",
    "BT-152",
    "BT-138",
    "BT-131",
    "BT-128",
    "BT-27",
    "BT-30",
    "BT-38",
    "BT-35",
    "BT-36",
    "BT-162",
    "BT-37",
    "BT-40",
    "BT-39",
    "BT-34",
    "BT-31",
    "BT-32",
    "BT-46",
    "BT-44",
    "BT-53",
    "BT-50",
    "BT-51",
    "BT-163",
    "BT-52",
    "BT-55",
    "BT-54",
    "BT-48",
    "BT-14",
    "BT-13",
    "BT-12",
    "BT-11",
    "BT-71",
    "BT-70",
    "BT-78",
    "BT-75",
    "BT-76",
    "BT-77",
    "BT-80",
    "BT-79",
    "BT-72",
    "BT-16",
    "BT-5",
    "BT-81",
    "BT-84",
    "BT-86",
    "BT-116",
    "BT-73",
    "BT-74",
    "BT-92",
    "BT-98",
    "BT-20",
    "BT-9",
    "BT-89",
    "BT-106",
    "BT-108",
    "BT-107",
    "BT-109",
    "BT-110",
    "BT-115",
    "BT-25",
    "BT-151",
    "BT-148",
    "BT-99",
    "BT-102",
    "BT-103",
    "BT-83",
    "BT-112",
    "BT-113",
    "BT-94",
    "BT-97",
    "BT-93",
    "BT-100",
    "BT-104",
    "BT-28",
    "BT-29",
    "BT-47",
    "BT-49",
    "BT-165",
    "BT-82",
    "BT-144",
    "BT-145",
    "BT-59",
    "BT-60",
    "BT-61",
    "BT-62",
    "BT-63",
    "BT-64",
    "BT-65",
    "BT-164",
    "BT-66",
    "BT-67",
    "BT-68",
    "BT-69",
    "BT-10",
]

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
- Only set BT-93/BT-97/BT-98 when BT-92 is present.
- Only set BT-100/BT-102/BT-103/BT-104 when BT-99 is present.
- Only set BT-113/BT-115 when paid amount is explicitly stated OR immediate payment is confirmed by payment means (BT-81), not just from payment terms table.
- If immediate payment: BT-113 = BT-112 - BT-107, BT-115 = BT-112 - BT-113 - BT-107.

Only extract BT fields from this allowed list:
{bt_list}

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


def _render_pdf_images(pdf_path: Path, max_pages: int) -> List[str]:
    images: List[str] = []
    pdf = PdfDocument(str(pdf_path))
    page_count = min(len(pdf), max_pages)
    for i in range(page_count):
        page = pdf[i]
        bitmap = page.render(scale=2)
        pil_image = bitmap.to_pil()
        buffer = BytesIO()
        pil_image.save(buffer, format="PNG")
        images.append(base64.b64encode(buffer.getvalue()).decode("ascii"))
    return images


def _call_openai(model: str, pdf_text: str, extracted_json: Dict[str, Any], pdf_path: Optional[str]) -> Dict[str, Any]:
    client = OpenAI()
    use_vision = os.getenv("LEXINVO_USE_GPT_VISION", "1").lower() in {"1", "true", "yes"}
    max_pages = int(os.getenv("LEXINVO_GPT_MAX_PAGES", "2"))
    image_b64_list: List[str] = []
    if use_vision and pdf_path:
        try:
            image_b64_list = _render_pdf_images(Path(pdf_path), max_pages=max_pages)
        except Exception:
            image_b64_list = []

    content_parts: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": USER_PROMPT.format(bt_list=", ".join(BT_LIST))
            + "\n\nPDF_TEXT:\n"
            + pdf_text[:200000]
            + "\n\nEXTRACTED_JSON:\n"
            + json.dumps(extracted_json, ensure_ascii=True)[:200000],
        }
    ]
    for image_b64 in image_b64_list:
        content_parts.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{image_b64}"},
            }
        )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content_parts},
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


def enrich_with_gpt(input_data: Dict[str, Any], pdf_path: Optional[str], model: str) -> List[Patch]:
    if not os.getenv("OPENAI_API_KEY"):
        return []
    if not pdf_path:
        pdf_text = ""
    else:
        pdf_text = extract_pdf_text(Path(pdf_path))
    if not pdf_text and not input_data:
        return []
    llm_output = _call_openai(model, pdf_text, input_data, pdf_path)
    return build_patches(llm_output)
