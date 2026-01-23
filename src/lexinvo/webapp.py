"""Flask UI for uploading Azure JSON and running the pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, redirect, render_template, request, send_file, url_for

from lexinvo.core.pipeline import run_pipeline

app = Flask(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "input" / "azure_invoice.json"
OUTPUT_DIR = PROJECT_ROOT / "output"
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"

OUTPUT_FILES = {
    "canonical_invoice": "canonical_invoice.json",
    "corrections_report": "corrections_report.json",
    "en16931_basic": "en16931_basic.json",
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
    return render_template("index.html")


@app.post("/run")
def run():
    upload = request.files.get("azure_json")
    if upload and upload.filename:
        INPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        upload.save(INPUT_PATH)

    run_pipeline(
        input_path=str(INPUT_PATH),
        output_dir=str(OUTPUT_DIR),
        config_dir=str(CONFIG_DIR),
        data_dir=str(DATA_DIR),
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

    return render_template(
        "success.html",
        corrections=corrections,
        relevant=relevant,
        all_rows=all_rows,
    )


@app.get("/download/en16931_basic")
def download_en16931_basic():
    path = OUTPUT_DIR / OUTPUT_FILES["en16931_basic"]
    if not path.exists():
        return redirect(url_for("success"))
    return send_file(path, as_attachment=True, download_name="en16931_basic.json")


if __name__ == "__main__":
    app.run(debug=True, port=8000)
