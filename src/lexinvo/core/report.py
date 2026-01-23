"""Corrections report generation from patches."""

from __future__ import annotations

from typing import Dict

from lexinvo.core.models import CanonicalInvoice


def build_report(invoice: CanonicalInvoice) -> Dict[str, object]:
    return {
        "entries": invoice.patches,
    }
