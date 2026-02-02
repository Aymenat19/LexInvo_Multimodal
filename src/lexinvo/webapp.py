"""Flask UI for uploading Azure JSON and running the pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, redirect, render_template, request, send_file, url_for

from lexinvo.core.pipeline import run_pipeline

app = Flask(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "input" / "azure_invoice.json"
PDF_PATH = PROJECT_ROOT / "input" / "invoice.pdf"
OUTPUT_DIR = PROJECT_ROOT / "output"
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
FEEDBACK_PATH = OUTPUT_DIR / "feedback.json"

MODEL_CHOICES = [
    "gpt-5.2",
    "gpt-5.1",
    "gpt-4.1",
    "gpt-4o",
    "gpt-4o-mini",
]

OUTPUT_FILES = {
    "canonical_invoice": "canonical_invoice.json",
    "corrections_report": "corrections_report.json",
    "en16931_basic": "en16931_basic.json",
    "bt_store": "bt_store.json",
}

RELEVANT_BTS = [
    "BT-3",
    "BT-5",
    "BT-20",
    "BT-126",
    "BT-153",
    "BT-154",
    "BT-157",
    "BT-129",
    "BT-130",
    "BT-146",
    "BT-148",
    "BT-131",
    "BT-149",
    "BT-147",
    "BT-138",
    "BT-151",
    "BT-152",
    "BT-144",
    "BT-145",
    "BT-92",
    "BT-93",
    "BT-94",
    "BT-97",
    "BT-98",
    "BT-99",
    "BT-100",
    "BT-102",
    "BT-103",
    "BT-104",
    "BT-106",
    "BT-107",
    "BT-108",
    "BT-109",
    "BT-110",
    "BT-112",
    "BT-113",
    "BT-115",
    "BT-116",
]


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"error": "Invalid JSON output", "path": str(path)}


def _read_registry() -> Dict[str, Any]:
    return _read_json(CONFIG_DIR / "bt_registry.json")


def _bt_sort_key(bt: str) -> int:
    try:
        return int(bt.replace("BT-", ""))
    except ValueError:
        return 9999


def _build_corrections(corrections_report: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = corrections_report.get("entries", [])
    rows = []
    for entry in entries:
        rows.append(
            {
                "bt": entry.get("bt"),
                "name": "",
                "old": entry.get("old_value"),
                "new": entry.get("new_value"),
                "status": entry.get("status"),
                "line_id": entry.get("line_id"),
            }
        )
    return rows


def _feedback_key(bt: str, line_id: Any) -> str:
    return f"{bt}:{line_id if line_id is not None else 'header'}"


def _build_en16931_basic_from_canonical(canonical: Dict[str, Any]) -> Dict[str, Any]:
    header = canonical.get("header", {})
    totals = canonical.get("totals", {})
    lines = canonical.get("lines", [])
    return {
        "profile": {
            "specification_identifier": header.get("BT-24", {}).get("value"),
            "invoice_type_code": header.get("BT-3", {}).get("value"),
        },
        "header": {
            "invoice_number": header.get("BT-1", {}).get("value"),
            "issue_date": header.get("BT-2", {}).get("value"),
            "currency": header.get("BT-5", {}).get("value"),
            "note_subject_code": header.get("BT-21", {}).get("value"),
            "payment_terms": header.get("BT-20", {}).get("value"),
        },
        "seller": {
            "name": header.get("BT-27", {}).get("value"),
            "vat_id": header.get("BT-31", {}).get("value"),
        },
        "buyer": {
            "name": header.get("BT-44", {}).get("value"),
        },
        "lines": [
            {bt: val.get("value") for bt, val in (line.get("bt", {}) or {}).items()}
            for line in lines
        ],
        "totals": {
            "sum_line_net": totals.get("BT-106", {}).get("value"),
            "total_without_vat": totals.get("BT-109", {}).get("value"),
            "vat_total": totals.get("BT-110", {}).get("value"),
            "total_with_vat": totals.get("BT-112", {}).get("value"),
            "amount_due": totals.get("BT-115", {}).get("value"),
        },
    }


def _apply_feedback_to_outputs() -> None:
    if not FEEDBACK_PATH.exists():
        return
    feedbacks = _read_json(FEEDBACK_PATH)
    canonical_path = OUTPUT_DIR / OUTPUT_FILES["canonical_invoice"]
    if not canonical_path.exists():
        return
    canonical = _read_json(canonical_path)
    if not canonical:
        return
    for key, entry in feedbacks.items():
        if entry.get("status") != "incorrect":
            continue
        correct_value = entry.get("correct_value")
        if correct_value is None or correct_value == "":
            continue
        try:
            bt, line_id = key.split(":", 1)
        except ValueError:
            continue
        if line_id == "header":
            record = canonical.get("header", {}).get(bt)
        else:
            record = None
            for line in canonical.get("lines", []):
                if str(line.get("line_id")) == line_id:
                    record = (line.get("bt", {}) or {}).get(bt)
                    break
        if not record:
            continue
        record["value"] = correct_value
        record["status"] = "corrected"
        record["source"] = "user"
    (OUTPUT_DIR / OUTPUT_FILES["canonical_invoice"]).write_text(
        json.dumps(canonical, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    (OUTPUT_DIR / OUTPUT_FILES["bt_store"]).write_text(
        json.dumps(canonical, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    en16931_basic = _build_en16931_basic_from_canonical(canonical)
    (OUTPUT_DIR / OUTPUT_FILES["en16931_basic"]).write_text(
        json.dumps(en16931_basic, indent=2, ensure_ascii=True), encoding="utf-8"
    )


def _has_value(record: Dict[str, Any]) -> bool:
    return record.get("raw_value") is not None or record.get("value") is not None


def _build_relevant(canonical: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    seen = set()
    header = canonical.get("header", {})
    totals = canonical.get("totals", {})
    for bt in RELEVANT_BTS:
        record = header.get(bt) or totals.get(bt)
        if record:
            key = (bt, None)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "bt": bt,
                    "name": record.get("bt", ""),
                    "old": record.get("raw_value"),
                    "new": record.get("value"),
                    "status": record.get("status"),
                    "line_id": None,
                }
            )

    for line in canonical.get("lines", []):
        line_id = line.get("line_id")
        bts = line.get("bt", {})
        for bt in RELEVANT_BTS:
            record = bts.get(bt)
            if not record or not _has_value(record):
                continue
            key = (bt, line_id)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "bt": bt,
                    "name": record.get("bt", ""),
                    "old": record.get("raw_value"),
                    "new": record.get("value"),
                    "status": record.get("status"),
                    "line_id": line_id,
                }
            )
    return rows


def _build_all_rows(canonical: Dict[str, Any], registry: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    header = canonical.get("header", {})
    totals = canonical.get("totals", {})
    lines = canonical.get("lines", [])

    for bt in sorted(registry.keys(), key=_bt_sort_key):
        group = registry.get(bt, {}).get("group", "header")
        if group == "line":
            continue
        record = header.get(bt) or totals.get(bt) or {}
        rows.append(
            {
                "bt": bt,
                "name": registry.get(bt, {}).get("name", ""),
                "line_id": "-",
                "old": record.get("raw_value"),
                "new": record.get("value"),
                "status": record.get("status"),
            }
        )

    line_bts = [bt for bt, spec in registry.items() if spec.get("group") == "line"]
    line_bts_sorted = sorted(line_bts, key=_bt_sort_key)
    for line in lines:
        line_id = line.get("line_id")
        bts = line.get("bt", {})
        for bt in line_bts_sorted:
            record = bts.get(bt, {})
            rows.append(
                {
                    "bt": bt,
                    "name": registry.get(bt, {}).get("name", ""),
                    "line_id": line_id,
                    "old": record.get("raw_value"),
                    "new": record.get("value"),
                    "status": record.get("status"),
                }
            )

    return rows


@app.get("/")
def index():
    default_model = os.getenv("LEXINVO_GPT_MODEL", "gpt-4o-mini")
    return render_template(
        "index.html",
        model_choices=MODEL_CHOICES,
        default_model=default_model if default_model in MODEL_CHOICES else MODEL_CHOICES[0],
    )


@app.post("/run")
def run():
    upload = request.files.get("azure_json")
    json_path = None
    if upload and upload.filename:
        INPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        upload.save(INPUT_PATH)
        json_path = str(INPUT_PATH)
    pdf_upload = request.files.get("invoice_pdf")
    pdf_path = None
    if pdf_upload and pdf_upload.filename:
        INPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        pdf_upload.save(PDF_PATH)
        pdf_path = str(PDF_PATH)

    selected_model = request.form.get("gpt_model")
    if selected_model in MODEL_CHOICES:
        os.environ["LEXINVO_GPT_MODEL"] = selected_model

    run_pipeline(
        input_path=json_path,
        output_dir=str(OUTPUT_DIR),
        config_dir=str(CONFIG_DIR),
        data_dir=str(DATA_DIR),
        pdf_path=pdf_path,
    )

    return redirect(url_for("success"))


@app.get("/success")
def success():
    outputs = {}
    for key, filename in OUTPUT_FILES.items():
        outputs[key] = _read_json(OUTPUT_DIR / filename)

    canonical = outputs.get("canonical_invoice", {})
    registry = _read_registry()
    corrections = _build_corrections(outputs.get("corrections_report", {}))
    relevant = _build_relevant(canonical)
    all_rows = _build_all_rows(canonical, registry)
    feedbacks = _read_json(FEEDBACK_PATH) if FEEDBACK_PATH.exists() else {}
    if isinstance(feedbacks, dict):
        for fb_key, fb_value in list(feedbacks.items()):
            if isinstance(fb_value, str):
                feedbacks[fb_key] = {"status": fb_value}

    return render_template(
        "success.html",
        corrections=corrections,
        relevant=relevant,
        all_rows=all_rows,
        feedbacks=feedbacks,
    )


@app.post("/feedback")
def feedback():
    feedbacks: Dict[str, Dict[str, str]] = {}
    for key, value in request.form.items():
        if not key.startswith("feedback__"):
            continue
        feedback_key = key.replace("feedback__", "", 1)
        feedbacks.setdefault(feedback_key, {})["status"] = value
    for key, value in request.form.items():
        if not key.startswith("correct__"):
            continue
        feedback_key = key.replace("correct__", "", 1)
        if value:
            feedbacks.setdefault(feedback_key, {})["correct_value"] = value
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FEEDBACK_PATH.write_text(json.dumps(feedbacks, indent=2, ensure_ascii=True), encoding="utf-8")
    return redirect(url_for("success"))


@app.post("/rerun")
def rerun():
    json_path = str(INPUT_PATH) if INPUT_PATH.exists() else None
    pdf_path = str(PDF_PATH) if PDF_PATH.exists() else None

    run_pipeline(
        input_path=json_path,
        output_dir=str(OUTPUT_DIR),
        config_dir=str(CONFIG_DIR),
        data_dir=str(DATA_DIR),
        pdf_path=pdf_path,
    )

    _apply_feedback_to_outputs()
    return redirect(url_for("success"))


@app.get("/download/en16931_basic")
def download_en16931_basic():
    path = OUTPUT_DIR / OUTPUT_FILES["en16931_basic"]
    if not path.exists():
        return redirect(url_for("success"))
    return send_file(path, as_attachment=True, download_name="en16931_basic.json")


if __name__ == "__main__":
    app.run(debug=True, port=8000)
