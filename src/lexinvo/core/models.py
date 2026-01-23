"""Canonical data models and patch definition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional


@dataclass
class BTValue:
    bt: str
    value: Any
    raw_value: Any = None
    status: Literal[
        "ok",
        "missing",
        "corrected",
        "derived",
        "wrong_formal",
        "wrong_semantic",
        "wrong_math",
        "ambiguous",
    ] = "missing"
    source: Literal["azure", "rule", "derived", "multimodal", "user"] = "azure"
    confidence: Optional[float] = None
    derivation: Optional[str] = None
    evidence: Optional[dict] = None
    rule_id: Optional[str] = None


@dataclass
class CanonicalLine:
    line_id: int
    bt: Dict[str, BTValue]


@dataclass
class Patch:
    scope: Literal["header", "line", "totals"]
    bt: str
    new_value: Any
    line_id: Optional[int] = None
    status: str = "corrected"
    source: str = "rule"
    derivation: str = ""
    rule_id: str = ""
    evidence: Optional[dict] = None


@dataclass
class CanonicalInvoice:
    header: Dict[str, BTValue]
    lines: List[CanonicalLine]
    totals: Dict[str, BTValue]
    raw: dict
    patches: List[dict]
