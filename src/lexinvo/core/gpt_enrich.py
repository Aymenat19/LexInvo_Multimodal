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


SYSTEM_PROMPT = """You are an invoice extraction and correction assistant for EN16931 / ZUGFeRD / XRechnung.
Your task is to reconcile a PDF invoice against an extracted JSON and output corrected BT fields.
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

USER_PROMPT = """Context:
You are part of an invoice correction engine for EN16931 / ZUGFeRD / XRechnung.
Pipeline: read the PDF (visual + text), compare against the extracted JSON, and return corrected BT fields.
Only output fields you can see or derive from the invoice and the given rules. Do not invent data.
If the invoice has line items, always return them in the `lines` array with line_id starting at 1.
If no line items are visible, return an empty `lines` array.

BT field details (subset):
- BT-1 Invoice number
- BT-2 Invoice issue date
- BT-3 Invoice type code
- BT-5 Invoice currency code
- BT-9 Payment due date
- BT-10 Buyer reference
- BT-11 Project reference
- BT-12 Contract reference
- BT-13 Purchase order reference
- BT-14 Seller order reference
- BT-16 Despatch advice reference
- BT-20 Payment terms (full table/text)
- BT-21 Invoice note subject code
- BT-24 Specification identifier
- BT-25 Preceding invoice reference
- BT-27 Seller name
- BT-28 Seller trading name
- BT-29 Seller identifier
- BT-30 Seller legal registration identifier
- BT-31 Seller VAT identifier
- BT-32 Seller tax registration identifier
- BT-34 Seller electronic address
- BT-35 Seller address line 1
- BT-36 Seller address line 2
- BT-37 Seller city
- BT-38 Seller post code
- BT-39 Seller country subdivision
- BT-40 Seller country code
- BT-44 Buyer name
- BT-46 Buyer identifier
- BT-47 Buyer legal registration identifier
- BT-48 Buyer VAT identifier
- BT-49 Buyer electronic address
- BT-50 Buyer address line 1
- BT-51 Buyer address line 2
- BT-52 Buyer city
- BT-53 Buyer post code
- BT-54 Buyer country subdivision
- BT-55 Buyer country code
- BT-59 Payee name
- BT-60 Payee identifier
- BT-61 Payee legal registration identifier (scheme)
- BT-62 Buyer tax representative name
- BT-63 Seller tax representative VAT identifier
- BT-64 Tax representative address line 1
- BT-65 Tax representative address line 2
- BT-66 Tax representative city
- BT-67 Tax representative post code
- BT-68 Tax representative country subdivision
- BT-69 Tax representative country code
- BT-70 Deliver-to party name
- BT-71 Deliver-to location identifier
- BT-72 Actual delivery date
- BT-73 Invoicing period start date
- BT-74 Invoicing period end date
- BT-75 Deliver-to address line 1
- BT-76 Deliver-to address line 2
- BT-77 Deliver-to city
- BT-78 Deliver-to post code
- BT-79 Deliver-to country subdivision
- BT-80 Deliver-to country code
- BT-81 Payment means type code
- BT-82 Specified trade settlement payment means
- BT-83 Remittance information
- BT-84 Payment account identifier
- BT-86 Payment service provider identifier
- BT-89 Mandate reference identifier
- BT-92 Document-level allowance amount
- BT-93 Document-level allowance base amount
- BT-94 Document-level allowance percentage
- BT-97 Document-level allowance reason
- BT-98 Document-level allowance reason code
- BT-99 Document-level charge amount
- BT-100 Document-level charge base amount
- BT-102 Document-level charge VAT category code
- BT-103 Document-level charge VAT rate
- BT-104 Document-level charge reason
- BT-106 Sum of invoice line net amount
- BT-107 Sum of allowances (document level)
- BT-108 Sum of charges (document level)
- BT-109 Invoice total amount without VAT
- BT-110 Invoice total VAT amount
- BT-112 Invoice total amount with VAT
- BT-113 Paid amount
- BT-115 Amount due for payment
- BT-116 VAT category taxable amount
- BT-126 Invoice line identifier
- BT-128 Additional referenced document
- BT-129 Invoiced quantity
- BT-130 Invoiced quantity unit of measure
- BT-131 Invoice line net amount
- BT-138 Invoice line allowance percentage
- BT-144 Invoice line charge reason
- BT-145 Invoice line charge reason code
- BT-146 Item net price
- BT-147 Item price discount
- BT-148 Item gross price
- BT-149 Item price base quantity
- BT-151 Invoiced item VAT category code
- BT-152 Invoiced item VAT rate
- BT-153 Item name
- BT-154 Item description
- BT-157 Item standard identifier
- BT-162 Seller address line 3
- BT-163 Buyer address line 3
- BT-164 Tax representative address line 3
- BT-165 Deliver-to address line 3

Rules (subset logic, no tool logic):
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


def enrich_with_gpt(input_data: Dict[str, Any], pdf_path: Optional[str], model: str) -> tuple[List[Patch], Dict[str, Any]]:
    if not os.getenv("OPENAI_API_KEY"):
        return [], {}
    if not pdf_path:
        pdf_text = ""
    else:
        pdf_text = extract_pdf_text(Path(pdf_path))
    if not pdf_text and not input_data:
        return [], {}
    llm_output = _call_openai(model, pdf_text, input_data, pdf_path)
    return build_patches(llm_output), llm_output
