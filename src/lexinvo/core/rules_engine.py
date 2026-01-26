"""Canonicalization rules by phase (v1)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from lexinvo.core.models import BTValue, CanonicalInvoice, Patch
from lexinvo.utils.normalize import normalize_country, normalize_email, normalize_vat_id, parse_date_to_iso, parse_decimal

TOLERANCE = 0.02

DE_STATE_CODES = {
    "Baden-Württemberg": "DE-BW",
    "Bayern": "DE-BY",
    "Berlin": "DE-BE",
    "Brandenburg": "DE-BB",
    "Bremen": "DE-HB",
    "Hamburg": "DE-HH",
    "Hessen": "DE-HE",
    "Mecklenburg-Vorpommern": "DE-MV",
    "Niedersachsen": "DE-NI",
    "Nordrhein-Westfalen": "DE-NW",
    "Rheinland-Pfalz": "DE-RP",
    "Saarland": "DE-SL",
    "Sachsen": "DE-SN",
    "Sachsen-Anhalt": "DE-ST",
    "Schleswig-Holstein": "DE-SH",
    "Thüringen": "DE-TH",
}

# Source: https://cebus.net/de/plz-bundesland.htm
DE_POSTCODE_RANGES = [
    (1001, 1936, "Sachsen"),
    (1941, 1998, "Brandenburg"),
    (2601, 2999, "Sachsen"),
    (3001, 3253, "Brandenburg"),
    (4001, 4579, "Sachsen"),
    (4581, 4639, "Thüringen"),
    (4641, 4889, "Sachsen"),
    (4891, 4938, "Brandenburg"),
    (6001, 6548, "Sachsen-Anhalt"),
    (6551, 6578, "Thüringen"),
    (6601, 6928, "Sachsen-Anhalt"),
    (7301, 7919, "Thüringen"),
    (7919, 7919, "Sachsen"),
    (7919, 7919, "Thüringen"),
    (7919, 7919, "Sachsen"),
    (7920, 7950, "Thüringen"),
    (7951, 7951, "Sachsen"),
    (7952, 7952, "Thüringen"),
    (7952, 7952, "Sachsen"),
    (7953, 7980, "Thüringen"),
    (7982, 7982, "Sachsen"),
    (7985, 7985, "Thüringen"),
    (7985, 7985, "Sachsen"),
    (7985, 7989, "Thüringen"),
    (8001, 9669, "Sachsen"),
    (10001, 14330, "Berlin"),
    (14401, 14715, "Brandenburg"),
    (14715, 14715, "Sachsen-Anhalt"),
    (14723, 16949, "Brandenburg"),
    (17001, 17256, "Mecklenburg-Vorpommern"),
    (17258, 17258, "Brandenburg"),
    (17258, 17259, "Mecklenburg-Vorpommern"),
    (17261, 17291, "Brandenburg"),
    (17301, 17309, "Mecklenburg-Vorpommern"),
    (17309, 17309, "Brandenburg"),
    (17309, 17321, "Mecklenburg-Vorpommern"),
    (17321, 17321, "Brandenburg"),
    (17321, 17322, "Mecklenburg-Vorpommern"),
    (17326, 17326, "Brandenburg"),
    (17328, 17331, "Mecklenburg-Vorpommern"),
    (17335, 17335, "Brandenburg"),
    (17335, 17335, "Mecklenburg-Vorpommern"),
    (17337, 17337, "Brandenburg"),
    (17337, 19260, "Mecklenburg-Vorpommern"),
    (19271, 19273, "Niedersachsen"),
    (19273, 19273, "Mecklenburg-Vorpommern"),
    (19273, 19306, "Mecklenburg-Vorpommern"),
    (19307, 19357, "Brandenburg"),
    (19357, 19417, "Mecklenburg-Vorpommern"),
    (20001, 21037, "Hamburg"),
    (21039, 21039, "Schleswig-Holstein"),
    (21039, 21170, "Hamburg"),
    (21202, 21449, "Niedersachsen"),
    (21451, 21521, "Schleswig-Holstein"),
    (21522, 21522, "Niedersachsen"),
    (21524, 21529, "Schleswig-Holstein"),
    (21601, 21789, "Niedersachsen"),
    (22001, 22113, "Hamburg"),
    (22113, 22113, "Schleswig-Holstein"),
    (22115, 22143, "Hamburg"),
    (22145, 22145, "Schleswig-Holstein"),
    (22145, 22145, "Hamburg"),
    (22145, 22145, "Schleswig-Holstein"),
    (22147, 22786, "Hamburg"),
    (22801, 23919, "Schleswig-Holstein"),
    (23921, 23999, "Mecklenburg-Vorpommern"),
    (24001, 25999, "Schleswig-Holstein"),
    (26001, 27478, "Niedersachsen"),
    (27483, 27498, "Schleswig-Holstein"),
    (27499, 27499, "Hamburg"),
    (27501, 27580, "Bremen"),
    (27607, 27809, "Niedersachsen"),
    (28001, 28779, "Bremen"),
    (28784, 29399, "Niedersachsen"),
    (29401, 29416, "Sachsen-Anhalt"),
    (29431, 31868, "Niedersachsen"),
    (32001, 33829, "Nordrhein-Westfalen"),
    (34001, 34329, "Hessen"),
    (34331, 34353, "Niedersachsen"),
    (34355, 34355, "Hessen"),
    (34355, 34355, "Niedersachsen"),
    (34356, 34399, "Hessen"),
    (34401, 34439, "Nordrhein-Westfalen"),
    (34441, 36399, "Hessen"),
    (36401, 36469, "Thüringen"),
    (37001, 37194, "Niedersachsen"),
    (37194, 37195, "Hessen"),
    (37197, 37199, "Niedersachsen"),
    (37201, 37299, "Hessen"),
    (37301, 37359, "Thüringen"),
    (37401, 37649, "Niedersachsen"),
    (37651, 37688, "Nordrhein-Westfalen"),
    (37689, 37691, "Niedersachsen"),
    (37692, 37696, "Nordrhein-Westfalen"),
    (37697, 38479, "Niedersachsen"),
    (38481, 38489, "Sachsen-Anhalt"),
    (38501, 38729, "Niedersachsen"),
    (38801, 39649, "Sachsen-Anhalt"),
    (40001, 48432, "Nordrhein-Westfalen"),
    (48442, 48465, "Niedersachsen"),
    (48466, 48477, "Nordrhein-Westfalen"),
    (48478, 48480, "Niedersachsen"),
    (48481, 48485, "Nordrhein-Westfalen"),
    (48486, 48488, "Niedersachsen"),
    (48489, 48496, "Nordrhein-Westfalen"),
    (48497, 48531, "Niedersachsen"),
    (48541, 48739, "Nordrhein-Westfalen"),
    (49001, 49459, "Niedersachsen"),
    (49461, 49549, "Nordrhein-Westfalen"),
    (49551, 49849, "Niedersachsen"),
    (50101, 51597, "Nordrhein-Westfalen"),
    (51598, 51598, "Rheinland-Pfalz"),
    (51601, 53359, "Nordrhein-Westfalen"),
    (53401, 53579, "Rheinland-Pfalz"),
    (53581, 53604, "Nordrhein-Westfalen"),
    (53614, 53619, "Rheinland-Pfalz"),
    (53621, 53949, "Nordrhein-Westfalen"),
    (54181, 55239, "Rheinland-Pfalz"),
    (55240, 55252, "Hessen"),
    (55253, 56869, "Rheinland-Pfalz"),
    (57001, 57489, "Nordrhein-Westfalen"),
    (57501, 57648, "Rheinland-Pfalz"),
    (58001, 59966, "Nordrhein-Westfalen"),
    (59969, 59969, "Hessen"),
    (59969, 59969, "Nordrhein-Westfalen"),
    (60001, 63699, "Hessen"),
    (63701, 63774, "Bayern"),
    (63776, 63776, "Hessen"),
    (63776, 63928, "Bayern"),
    (63928, 63928, "Baden-Württemberg"),
    (63930, 63939, "Bayern"),
    (64201, 64753, "Hessen"),
    (64754, 64754, "Baden-Württemberg"),
    (64754, 65326, "Hessen"),
    (65326, 65326, "Rheinland-Pfalz"),
    (65327, 65391, "Hessen"),
    (65391, 65391, "Rheinland-Pfalz"),
    (65392, 65556, "Hessen"),
    (65558, 65582, "Rheinland-Pfalz"),
    (65583, 65620, "Hessen"),
    (65621, 65626, "Rheinland-Pfalz"),
    (65627, 65627, "Hessen"),
    (65629, 65629, "Rheinland-Pfalz"),
    (65701, 65936, "Hessen"),
    (66001, 66459, "Saarland"),
    (66461, 66509, "Rheinland-Pfalz"),
    (66511, 66839, "Saarland"),
    (66841, 67829, "Rheinland-Pfalz"),
    (68001, 68312, "Baden-Württemberg"),
    (68501, 68519, "Hessen"),
    (68520, 68549, "Baden-Württemberg"),
    (68601, 68649, "Hessen"),
    (68701, 69234, "Baden-Württemberg"),
    (69235, 69239, "Hessen"),
    (69240, 69429, "Baden-Württemberg"),
    (69430, 69431, "Hessen"),
    (69434, 69434, "Baden-Württemberg"),
    (69434, 69434, "Hessen"),
    (69435, 69469, "Baden-Württemberg"),
    (69479, 69488, "Hessen"),
    (69489, 69502, "Baden-Württemberg"),
    (69503, 69509, "Hessen"),
    (69510, 69514, "Baden-Württemberg"),
    (69515, 69518, "Hessen"),
    (70001, 74592, "Baden-Württemberg"),
    (74594, 74594, "Bayern"),
    (74594, 76709, "Baden-Württemberg"),
    (76711, 76891, "Rheinland-Pfalz"),
    (77601, 79879, "Baden-Württemberg"),
    (80001, 87490, "Bayern"),
    (87491, 87491, "Außerhalb der BRD"),
    (87493, 87561, "Bayern"),
    (87567, 87569, "Außerhalb der BRD"),
    (87571, 87789, "Bayern"),
    (88001, 88099, "Baden-Württemberg"),
    (88101, 88146, "Bayern"),
    (88147, 88147, "Baden-Württemberg"),
    (88147, 88179, "Bayern"),
    (88181, 89079, "Baden-Württemberg"),
    (89081, 89081, "Bayern"),
    (89081, 89085, "Baden-Württemberg"),
    (89087, 89087, "Bayern"),
    (89090, 89198, "Baden-Württemberg"),
    (89201, 89449, "Bayern"),
    (89501, 89619, "Baden-Württemberg"),
    (90001, 96489, "Bayern"),
    (96501, 96529, "Thüringen"),
    (97001, 97859, "Bayern"),
    (97861, 97877, "Baden-Württemberg"),
    (97888, 97892, "Bayern"),
    (97893, 97896, "Baden-Württemberg"),
    (97896, 97896, "Bayern"),
    (97897, 97900, "Baden-Württemberg"),
    (97901, 97909, "Bayern"),
    (97911, 97999, "Baden-Württemberg"),
    (98501, 99998, "Thüringen"),
]


def _bt(scope: Dict[str, BTValue], bt: str) -> Optional[BTValue]:
    return scope.get(bt)


def _line(invoice: CanonicalInvoice, line_id: int) -> Optional[Dict[str, BTValue]]:
    for line in invoice.lines:
        if line.line_id == line_id:
            return line.bt
    return None


def _normalize_de_postcode(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text.startswith("D-"):
        text = text[2:]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 5:
        return None
    return int(digits[:5])


def _de_subdivision_from_postcode(value: object) -> Optional[str]:
    post_code = _normalize_de_postcode(value)
    if post_code is None:
        return None
    matches = {state for start, end, state in DE_POSTCODE_RANGES if start <= post_code <= end}
    matches.discard("Außerhalb der BRD")
    if len(matches) != 1:
        return None
    return DE_STATE_CODES.get(next(iter(matches)))


def _make_patch(
    scope: str,
    bt: str,
    new_value: Any,
    *,
    status: str,
    source: str,
    derivation: str,
    rule_id: str,
    line_id: int | None = None,
    evidence: Optional[dict] = None,
) -> Patch:
    return Patch(
        scope=scope,
        bt=bt,
        new_value=new_value,
        line_id=line_id,
        status=status,
        source=source,
        derivation=derivation,
        rule_id=rule_id,
        evidence=evidence,
    )


def _extract_vat_id(value: object) -> Optional[str]:
    if value is None:
        return None
    text = normalize_vat_id(value)
    if not text:
        return None
    # Prefer pattern: two letters followed by digits.
    import re

    match = re.search(r"([A-Z]{2}\d+)", text)
    if match:
        return match.group(1)
    return text


def _extract_registration_id(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    if not text.strip():
        return None
    import re

    match = re.search(r"\b(HR[AB]\s*\d+)\b", text)
    if match:
        return match.group(1).strip()
    # Fallback to first line/segment
    return text.splitlines()[0].strip()


def _normalize_tax_registration(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Remove labels like "Nr." and keep digits with separators.
    import re

    text = re.sub(r"\bNr\.?\b", "", text, flags=re.IGNORECASE).strip()
    # Keep digits and separators /.- and spaces
    cleaned = re.sub(r"[^0-9/.\- ]+", "", text).strip()
    return cleaned or None


def _extract_dates_from_text(text: str) -> List[str]:
    import re

    dates = []
    for match in re.findall(r"\b\d{2}\.\d{2}\.\d{4}\b", text):
        iso = parse_date_to_iso(match)
        if iso:
            dates.append(iso)
    return dates


def _extract_days_from_text(text: str) -> Optional[int]:
    import re

    stripped = text.strip()
    if stripped.isdigit():
        try:
            return int(stripped)
        except ValueError:
            return None
    matches = re.findall(r"(\d+)\s*Tage", text, flags=re.IGNORECASE)
    if not matches:
        return None
    try:
        return max(int(m) for m in matches)
    except ValueError:
        return None


def _add_days(iso_date: str, days: int) -> Optional[str]:
    try:
        base = datetime.strptime(iso_date, "%Y-%m-%d")
    except ValueError:
        return None
    return (base + timedelta(days=days)).strftime("%Y-%m-%d")


# Phase 1: Normalization

def phase1_normalize(invoice: CanonicalInvoice) -> List[Patch]:
    patches: List[Patch] = []

    bt2 = _bt(invoice.header, "BT-2")
    if bt2 and bt2.value:
        normalized = parse_date_to_iso(bt2.raw_value or bt2.value)
        if normalized and normalized != bt2.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-2",
                    normalized,
                    status="corrected",
                    source="rule",
                    derivation="Normalized date to ISO",
                    rule_id="R-HDR-DATE-001",
                    evidence=bt2.evidence,
                )
            )

    bt31 = _bt(invoice.header, "BT-31")
    if bt31 and bt31.value:
        normalized_vat = _extract_vat_id(bt31.value)
        if normalized_vat and normalized_vat != bt31.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-31",
                    normalized_vat,
                    status="corrected",
                    source="rule",
                    derivation="Normalized VAT ID",
                    rule_id="R-HDR-VAT-001",
                    evidence=bt31.evidence,
                )
            )

    bt30 = _bt(invoice.header, "BT-30")
    if bt30 and bt30.value:
        normalized_reg = _extract_registration_id(bt30.value)
        if normalized_reg and normalized_reg != bt30.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-30",
                    normalized_reg,
                    status="corrected",
                    source="rule",
                    derivation="Normalized registration identifier to single value",
                    rule_id="R-HDR-REG-001",
                    evidence=bt30.evidence,
                )
            )

    bt34 = _bt(invoice.header, "BT-34")
    if bt34 and bt34.value:
        normalized_email = normalize_email(bt34.value)
        if normalized_email and normalized_email != bt34.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-34",
                    normalized_email,
                    status="corrected",
                    source="rule",
                    derivation="Normalized email",
                    rule_id="R-HDR-EMAIL-001",
                    evidence=bt34.evidence,
                )
            )

    bt32 = _bt(invoice.header, "BT-32")
    if bt32 and bt32.value:
        normalized_tax = _normalize_tax_registration(bt32.value)
        if normalized_tax and normalized_tax != bt32.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-32",
                    normalized_tax,
                    status="corrected",
                    source="rule",
                    derivation="Normalized tax registration identifier",
                    rule_id="R-HDR-TAXREG-001",
                    evidence=bt32.evidence,
                )
            )

    bt55 = _bt(invoice.header, "BT-55")
    if bt55 and bt55.value:
        normalized_country = normalize_country(bt55.value)
        if normalized_country and normalized_country != bt55.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-55",
                    normalized_country,
                    status="corrected",
                    source="rule",
                    derivation="Normalized country to ISO2",
                    rule_id="R-HDR-COUNTRY-BUYER-001",
                    evidence=bt55.evidence,
                )
            )

    amount_bts = {
        "BT-92",
        "BT-93",
        "BT-94",
        "BT-99",
        "BT-100",
        "BT-103",
        "BT-106",
        "BT-107",
        "BT-108",
        "BT-109",
        "BT-110",
        "BT-112",
        "BT-113",
        "BT-115",
        "BT-116",
    }
    line_amount_bts = {"BT-131", "BT-146", "BT-147", "BT-148", "BT-149"}

    for bt in amount_bts:
        record = invoice.totals.get(bt)
        if record and isinstance(record.value, str):
            numeric = parse_decimal(record.value)
            if numeric is not None:
                normalized = f"{numeric:.2f}"
                if normalized != record.value:
                    patches.append(
                        _make_patch(
                            "totals",
                            bt,
                            normalized,
                            status="corrected",
                            source="rule",
                            derivation="Normalized amount format",
                            rule_id="R-TOT-AMOUNT-NORM-001",
                            evidence=record.evidence,
                        )
                    )

    for line in invoice.lines:
        for bt in line_amount_bts:
            record = line.bt.get(bt)
            if record and isinstance(record.value, str):
                numeric = parse_decimal(record.value)
                if numeric is not None:
                    normalized = f"{numeric:.2f}"
                    if normalized != record.value:
                        patches.append(
                            _make_patch(
                                "line",
                                bt,
                                normalized,
                                line_id=line.line_id,
                                status="corrected",
                                source="rule",
                                derivation="Normalized amount format",
                                rule_id="R-LINE-AMOUNT-NORM-001",
                                evidence=record.evidence,
                            )
                        )

    return patches


# Phase 2: Deterministic derivation

def phase2_derive(invoice: CanonicalInvoice) -> List[Patch]:
    patches: List[Patch] = []

    # Line identifiers
    for line in invoice.lines:
        bt126 = line.bt.get("BT-126")
        if bt126 and not bt126.value:
            patches.append(
                _make_patch(
                    "line",
                    "BT-126",
                    str(line.line_id),
                    line_id=line.line_id,
                    status="derived",
                    source="derived",
                    derivation="Assigned line number as identifier",
                    rule_id="R-LINE-ID-001",
                    evidence={"from": "line_index", "line_id": line.line_id},
                )
            )

    # Country code/subdivision heuristics from post codes
    bt53 = _bt(invoice.header, "BT-53")
    bt55 = _bt(invoice.header, "BT-55")
    bt54 = _bt(invoice.header, "BT-54")
    if bt53 and bt53.value:
        post_code = str(bt53.value).strip()
        if bt55 and not bt55.value and post_code:
            if post_code.startswith("D-") or post_code.isdigit() and len(post_code) == 5:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-55",
                        "DE",
                        status="derived",
                        source="derived",
                        derivation="Derived country code from buyer post code",
                        rule_id="R-HDR-COUNTRY-BUYER-POST-001",
                        evidence=bt53.evidence,
                    )
                )
        if bt54 and not bt54.value:
            subdivision = _de_subdivision_from_postcode(bt53.value)
            if subdivision:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-54",
                        subdivision,
                        status="derived",
                        source="derived",
                        derivation="Derived country subdivision from German buyer post code",
                        rule_id="R-HDR-SUBDIV-BUYER-POST-001",
                        evidence=bt53.evidence,
                    )
                )

    bt38 = _bt(invoice.header, "BT-38")
    bt40 = _bt(invoice.header, "BT-40")
    bt39 = _bt(invoice.header, "BT-39")
    if bt38 and bt38.value:
        post_code = str(bt38.value).strip()
        is_de_post = post_code.startswith("D-") or (post_code.isdigit() and len(post_code) == 5)
        if bt40 and not bt40.value and is_de_post:
            patches.append(
                _make_patch(
                    "header",
                    "BT-40",
                    "DE",
                    status="derived",
                    source="derived",
                    derivation="Derived country code from seller post code",
                    rule_id="R-HDR-COUNTRY-SELLER-POST-001",
                    evidence=bt38.evidence,
                )
            )
        if bt39 and not bt39.value:
            subdivision = _de_subdivision_from_postcode(bt38.value)
            if subdivision:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-39",
                        subdivision,
                        status="derived",
                        source="derived",
                        derivation="Derived country subdivision from German seller post code",
                        rule_id="R-HDR-SUBDIV-SELLER-POST-001",
                        evidence=bt38.evidence,
                    )
                )

    bt67 = _bt(invoice.header, "BT-67")
    bt68 = _bt(invoice.header, "BT-68")
    if bt67 and bt67.value and bt68 and not bt68.value:
        subdivision = _de_subdivision_from_postcode(bt67.value)
        if subdivision:
            patches.append(
                _make_patch(
                    "header",
                    "BT-68",
                    subdivision,
                    status="derived",
                    source="derived",
                    derivation="Derived country subdivision from German tax representative post code",
                    rule_id="R-HDR-SUBDIV-TAXREP-POST-001",
                    evidence=bt67.evidence,
                )
            )

    bt78 = _bt(invoice.header, "BT-78")
    bt80 = _bt(invoice.header, "BT-80")
    bt79 = _bt(invoice.header, "BT-79")
    if bt78 and bt78.value:
        post_code = str(bt78.value).strip()
        if bt80 and not bt80.value:
            if post_code.startswith("D-") or post_code.isdigit() and len(post_code) == 5:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-80",
                        "DE",
                        status="derived",
                        source="derived",
                        derivation="Derived country code from delivery post code",
                        rule_id="R-HDR-COUNTRY-DELIVERY-POST-001",
                        evidence=bt78.evidence,
                    )
                )
        if bt79 and not bt79.value:
            subdivision = _de_subdivision_from_postcode(bt78.value)
            if subdivision:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-79",
                        subdivision,
                        status="derived",
                        source="derived",
                        derivation="Derived country subdivision from German delivery post code",
                        rule_id="R-HDR-SUBDIV-DELIVERY-POST-001",
                    evidence=bt78.evidence,
                )
            )

    # Skonto in payment terms when immediate payment (BT-81)
    bt20 = _bt(invoice.header, "BT-20")
    bt81 = _bt(invoice.header, "BT-81")
    bt92 = _bt(invoice.totals, "BT-92")
    bt94 = _bt(invoice.totals, "BT-94")
    if bt20 and bt20.value and bt81 and bt81.value:
        instant_tokens = {"vorkasse", "credit card", "kreditkarte", "paypal", "ebay", "klarna", "kaufland", "amazon", "online"}
        if any(token in str(bt81.value).lower() for token in instant_tokens):
            import re

            terms_text = str(bt20.value).replace(",", ".")
            percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%\s*skonto", terms_text, re.IGNORECASE)
            amount_match = re.search(r"\(([-\d\.]+)\s*(?:eur|€)?\)", terms_text, re.IGNORECASE)
            skonto_percent = parse_decimal(percent_match.group(1)) if percent_match else None
            skonto_amount = parse_decimal(amount_match.group(1)) if amount_match else None
            if skonto_percent is not None and bt94 and not bt94.value:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-94",
                        f"{skonto_percent:.2f}",
                        status="derived",
                        source="derived",
                        derivation="Skonto percentage from payment terms",
                        rule_id="R-PAY-SKONTO-007",
                        evidence=bt20.evidence,
                    )
                )
            if skonto_amount is not None and bt92 and not bt92.value:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-92",
                        f"{skonto_amount:.2f}",
                        status="derived",
                        source="derived",
                        derivation="Skonto amount from payment terms",
                        rule_id="R-PAY-SKONTO-008",
                        evidence=bt20.evidence,
                    )
                )
                bt107 = _bt(invoice.totals, "BT-107")
                if bt107 and not bt107.value:
                    patches.append(
                        _make_patch(
                            "totals",
                            "BT-107",
                            f"{skonto_amount:.2f}",
                            status="derived",
                            source="derived",
                            derivation="Sum of document-level allowances",
                            rule_id="R-TOT-ALLOW-001",
                            evidence=bt20.evidence,
                        )
                    )

    # VAT category from VAT rate
    for line in invoice.lines:
        bt151 = line.bt.get("BT-151")
        bt152 = line.bt.get("BT-152")
        rate = parse_decimal(bt152.value) if bt152 and bt152.value else None
        if bt151 and not bt151.value and rate is not None:
            category = "S" if rate > 0 else "Z"
            patches.append(
                _make_patch(
                    "line",
                    "BT-151",
                    category,
                    line_id=line.line_id,
                    status="derived",
                    source="derived",
                    derivation="Derived VAT category from rate",
                    rule_id="R-LINE-VATCAT-001",
                    evidence=bt152.evidence if bt152 else None,
                )
            )

    # Derive BT-131 when missing: BT-146 * BT-129 * (1 - BT-138%)
    for line in invoice.lines:
        bt131 = line.bt.get("BT-131")
        if not bt131 or bt131.value:
            continue
        qty = parse_decimal(line.bt.get("BT-129").value) if line.bt.get("BT-129") else None
        unit_price = parse_decimal(line.bt.get("BT-146").value) if line.bt.get("BT-146") else None
        discount_pct = parse_decimal(line.bt.get("BT-138").value) if line.bt.get("BT-138") else None
        if qty is None or unit_price is None:
            continue
        line_total = unit_price * qty
        if discount_pct is not None:
            line_total = line_total * (1 - (discount_pct / 100))
        line_total = round(line_total, 2)
        patches.append(
            _make_patch(
                "line",
                "BT-131",
                f"{line_total:.2f}",
                line_id=line.line_id,
                status="derived",
                source="derived",
                derivation="BT-146 * BT-129 * (1 - BT-138%)",
                rule_id="R-LINE-NET-001",
            )
        )

    # BT-106 sum of line net amounts
    line_net_values = []
    for line in invoice.lines:
        value = parse_decimal(line.bt.get("BT-131").value) if line.bt.get("BT-131") else None
        if value is None:
            continue
        net_value = value
        # If BT-131 likely pre-discount and line allowance exists, subtract it.
        has_discount_pct = parse_decimal(line.bt.get("BT-138").value) if line.bt.get("BT-138") else None
        allowance = parse_decimal(line.bt.get("BT-147").value) if line.bt.get("BT-147") else None
        if has_discount_pct is None and allowance is not None:
            net_value = net_value - allowance
        line_net_values.append(net_value)
    if line_net_values:
        total = round(sum(line_net_values), 2)
        bt106 = _bt(invoice.totals, "BT-106")
        if bt106 and not bt106.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-106",
                    f"{total:.2f}",
                    status="derived",
                    source="derived",
                    derivation="Sum of line net amounts (line allowances applied)",
                    rule_id="R-TOT-SUMS-001",
                )
            )

    bt99 = _bt(invoice.totals, "BT-99")
    bt92 = _bt(invoice.totals, "BT-92")
    bt108 = _bt(invoice.totals, "BT-108")
    bt107 = _bt(invoice.totals, "BT-107")
    charge_amount = parse_decimal(bt99.value) if bt99 and bt99.value else None
    allowance_amount = parse_decimal(bt92.value) if bt92 and bt92.value else None
    if bt108 and not bt108.value and charge_amount is not None:
        patches.append(
            _make_patch(
                "totals",
                "BT-108",
                f"{charge_amount:.2f}",
                status="derived",
                source="derived",
                derivation="Derived from document charge amount",
                rule_id="R-TOT-SUMS-002",
            )
        )
    if bt107 and not bt107.value and allowance_amount is not None:
        patches.append(
            _make_patch(
                "totals",
                "BT-107",
                f"{allowance_amount:.2f}",
                status="derived",
                source="derived",
                derivation="Derived from document allowance amount",
                rule_id="R-TOT-SUMS-003",
            )
        )

    # Normalize duplicate currency tokens (e.g., "EUR EUR")
    bt5 = _bt(invoice.header, "BT-5")
    if bt5 and bt5.value and isinstance(bt5.value, str):
        tokens = [t for t in bt5.value.strip().split() if t]
        if len(tokens) > 1 and tokens[0] == tokens[1]:
            patches.append(
                _make_patch(
                    "header",
                    "BT-5",
                    tokens[0],
                    status="corrected",
                    source="rule",
                    derivation="Removed duplicate currency token",
                    rule_id="R-HDR-CURRENCY-DEDUP-001",
                    evidence=bt5.evidence,
                )
            )

    # Normalize duplicate dates (e.g., "03.12.2020 03.12.2020")
    bt72 = _bt(invoice.header, "BT-72")
    if bt72 and bt72.value and isinstance(bt72.value, str):
        tokens = [t for t in bt72.value.strip().split() if t]
        if len(tokens) > 1 and tokens[0] == tokens[1]:
            patches.append(
                _make_patch(
                    "header",
                    "BT-72",
                    tokens[0],
                    status="corrected",
                    source="rule",
                    derivation="Removed duplicate date token",
                    rule_id="R-HDR-DATE-DEDUP-001",
                    evidence=bt72.evidence,
                )
            )

    bt106_val = parse_decimal(_bt(invoice.totals, "BT-106").value) if _bt(invoice.totals, "BT-106") else None
    bt107_val = parse_decimal(_bt(invoice.totals, "BT-107").value) if _bt(invoice.totals, "BT-107") else 0.0
    bt108_val = parse_decimal(_bt(invoice.totals, "BT-108").value) if _bt(invoice.totals, "BT-108") else 0.0

    bt109 = _bt(invoice.totals, "BT-109")
    bt110 = _bt(invoice.totals, "BT-110")
    bt112 = _bt(invoice.totals, "BT-112")
    bt115 = _bt(invoice.totals, "BT-115")
    bt113 = _bt(invoice.totals, "BT-113")

    if bt106_val is not None and bt109 and not bt109.value:
        total_without_vat = round(bt106_val - (bt107_val or 0.0) + (bt108_val or 0.0), 2)
        patches.append(
            _make_patch(
                "totals",
                "BT-109",
                f"{total_without_vat:.2f}",
                status="derived",
                source="derived",
                derivation="BT-106 - BT-107 + BT-108",
                rule_id="R-TOT-GRAND-001",
            )
        )

    bt109_val = parse_decimal(bt109.value) if bt109 and bt109.value else None
    bt110_val = parse_decimal(bt110.value) if bt110 and bt110.value else None
    bt112_val = parse_decimal(bt112.value) if bt112 and bt112.value else None
    bt113_val = parse_decimal(bt113.value) if bt113 and bt113.value else None

    if bt109_val is not None and bt110_val is not None and bt112 and not bt112.value:
        total_with_vat = round(bt109_val + bt110_val, 2)
        patches.append(
            _make_patch(
                "totals",
                "BT-112",
                f"{total_with_vat:.2f}",
                status="derived",
                source="derived",
                derivation="BT-109 + BT-110",
                rule_id="R-TOT-GRAND-002",
            )
        )

    if bt112_val is not None and bt109_val is not None and bt110 and not bt110.value:
        vat_total = round(bt112_val - bt109_val, 2)
        patches.append(
            _make_patch(
                "totals",
                "BT-110",
                f"{vat_total:.2f}",
                status="derived",
                source="derived",
                derivation="BT-112 - BT-109",
                rule_id="R-TOT-GRAND-003",
            )
        )

    if bt112_val is not None and bt113_val is not None and bt115 and not bt115.value:
        due = round(bt112_val - bt113_val - (bt107_val or 0.0), 2)
        patches.append(
            _make_patch(
                "totals",
                "BT-115",
                f"{due:.2f}",
                status="derived",
                source="derived",
                derivation="BT-112 - BT-113 - BT-107",
                rule_id="R-TOT-GRAND-005",
            )
        )

    if bt109_val is not None and bt110 and not bt110.value:
        vat_rate_values = []
        for line in invoice.lines:
            rate = parse_decimal(line.bt.get("BT-152").value) if line.bt.get("BT-152") else None
            if rate is not None:
                vat_rate_values.append(rate)
        if vat_rate_values:
            rate_set = {round(r, 2) for r in vat_rate_values}
            if len(rate_set) == 1:
                rate = next(iter(rate_set))
                vat_total = round(bt109_val * (rate / 100), 2)
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-110",
                        f"{vat_total:.2f}",
                        status="derived",
                        source="derived",
                        derivation=f"BT-109 * {rate:.2f}%",
                        rule_id="R-TOT-VAT-001",
                    )
                )

    bt116 = _bt(invoice.totals, "BT-116")
    taxable = None
    if bt109_val is not None:
        taxable = bt109_val
    elif bt106_val is not None:
        taxable = round(bt106_val - (bt107_val or 0.0) + (bt108_val or 0.0), 2)
    if bt116 and not bt116.value and taxable is not None:
        categories = {
            line.bt.get("BT-151").value
            for line in invoice.lines
            if line.bt.get("BT-151") and line.bt.get("BT-151").value
        }
        if len(categories) <= 1:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-116",
                    f"{taxable:.2f}",
                    status="derived",
                    source="derived",
                    derivation="Taxable amount from total without VAT (single VAT category)",
                    rule_id="R-TOT-TAXABLE-001",
                )
            )

    return patches


# Phase 3: Consistency validation & correction

def phase3_validate(invoice: CanonicalInvoice) -> List[Patch]:
    patches: List[Patch] = []

    bt106_val = parse_decimal(_bt(invoice.totals, "BT-106").value) if _bt(invoice.totals, "BT-106") else None
    bt107_val = parse_decimal(_bt(invoice.totals, "BT-107").value) if _bt(invoice.totals, "BT-107") else 0.0
    bt108_val = parse_decimal(_bt(invoice.totals, "BT-108").value) if _bt(invoice.totals, "BT-108") else 0.0
    bt116_val = parse_decimal(_bt(invoice.totals, "BT-116").value) if _bt(invoice.totals, "BT-116") else None
    bt109 = _bt(invoice.totals, "BT-109")
    bt110 = _bt(invoice.totals, "BT-110")
    bt112 = _bt(invoice.totals, "BT-112")
    bt113 = _bt(invoice.totals, "BT-113")
    bt115 = _bt(invoice.totals, "BT-115")

    bt109_val = parse_decimal(bt109.value) if bt109 and bt109.value else None
    bt110_val = parse_decimal(bt110.value) if bt110 and bt110.value else None
    bt112_val = parse_decimal(bt112.value) if bt112 and bt112.value else None
    bt113_val = parse_decimal(bt113.value) if bt113 and bt113.value else None

    # Sum of line net amounts vs BT-106
    line_net_values = []
    for line in invoice.lines:
        value = parse_decimal(line.bt.get("BT-131").value) if line.bt.get("BT-131") else None
        if value is not None:
            line_net_values.append(value)
    if line_net_values:
        computed = round(sum(line_net_values), 2)
        if bt106_val is not None and abs(bt106_val - computed) > TOLERANCE:
            bt106 = _bt(invoice.totals, "BT-106")
            if bt106:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-106",
                        f"{computed:.2f}",
                        status="wrong_math",
                        source="rule",
                        derivation="Sum of line net amounts (BT-131)",
                        rule_id="R-TOT-CHECK-004",
                    )
                )

    if bt106_val is not None:
        computed = round(bt106_val - (bt107_val or 0.0) + (bt108_val or 0.0), 2)
        if bt109_val is not None and abs(bt109_val - computed) > TOLERANCE and bt109:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-109",
                    f"{computed:.2f}",
                    status="wrong_math",
                    source="rule",
                    derivation="BT-106 - BT-107 + BT-108",
                    rule_id="R-TOT-CHECK-001",
                )
            )

    if bt109_val is not None and bt110_val is not None:
        computed = round(bt109_val + bt110_val, 2)
        if bt112_val is not None and abs(bt112_val - computed) > TOLERANCE and bt112:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-112",
                    f"{computed:.2f}",
                    status="wrong_math",
                    source="rule",
                    derivation="BT-109 + BT-110",
                    rule_id="R-TOT-CHECK-002",
                )
            )

    if bt112_val is not None and bt113_val is not None:
        computed = round(bt112_val - bt113_val - (bt107_val or 0.0), 2)
        if bt115 and bt115.value and abs(parse_decimal(bt115.value) - computed) > TOLERANCE:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-115",
                    f"{computed:.2f}",
                    status="wrong_math",
                    source="rule",
                    derivation="BT-112 - BT-113 - BT-107",
                    rule_id="R-TOT-CHECK-003",
                )
            )

    # Taxable amount consistency when single VAT category
    if bt116_val is not None and bt109_val is not None:
        categories = {
            line.bt.get("BT-151").value
            for line in invoice.lines
            if line.bt.get("BT-151") and line.bt.get("BT-151").value
        }
        if len(categories) <= 1 and abs(bt116_val - bt109_val) > TOLERANCE:
            bt116 = _bt(invoice.totals, "BT-116")
            if bt116:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-116",
                        f"{bt109_val:.2f}",
                        status="wrong_math",
                        source="rule",
                        derivation="Taxable amount equals total without VAT (single VAT category)",
                        rule_id="R-TOT-CHECK-005",
                    )
                )

    return patches


# Phase 4: Ambiguity resolution (controlled)

def phase4_resolve(invoice: CanonicalInvoice) -> List[Patch]:
    patches: List[Patch] = []

    content = invoice.raw.get("analyzeResult", {}).get("content", "")
    lines_list = [ln.strip() for ln in content.splitlines() if ln.strip()]

    def _find_amount_after(label: str) -> Optional[float]:
        for idx, line in enumerate(lines_list):
            if label.lower() in line.lower():
                parts = line.split(":")
                amount_text = parts[-1] if len(parts) > 1 else line.replace(label, "")
                amount = parse_decimal(amount_text)
                if amount is None and idx + 1 < len(lines_list):
                    amount = parse_decimal(lines_list[idx + 1])
                return amount
        return None

    # Currency inference (BT-5)
    bt5 = _bt(invoice.header, "BT-5")
    if bt5 and not bt5.value:
        if "EUR" in content or "€" in content:
            patches.append(
                _make_patch(
                    "header",
                    "BT-5",
                    "EUR",
                    status="corrected",
                    source="rule",
                    derivation="Detected currency token in full text",
                    rule_id="R-HDR-CURRENCY-001",
                    evidence={"from": "full_text", "path": "analyzeResult.content"},
                )
            )

    # Seller country from VAT prefix (BT-40)
    bt31 = _bt(invoice.header, "BT-31")
    bt40 = _bt(invoice.header, "BT-40")
    vat = normalize_vat_id(bt31.value) if bt31 and bt31.value else None
    if bt40 and vat and len(vat) >= 2 and bt40.value != vat[:2]:
        patches.append(
            _make_patch(
                "header",
                "BT-40",
                vat[:2],
                status="corrected",
                source="rule",
                derivation="Derived from seller VAT prefix",
                rule_id="R-HDR-COUNTRY-SELLER-001",
                evidence=bt31.evidence if bt31 else None,
            )
        )

    # Total with VAT from text if missing (BT-112)
    bt112 = _bt(invoice.totals, "BT-112")
    if bt112 and not bt112.value:
        total_with_vat = _find_amount_after("Gesamtbetrag in EUR") or _find_amount_after("Gesamtbetrag")
        if total_with_vat is not None:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-112",
                    f"{total_with_vat:.2f}",
                    status="corrected",
                    source="rule",
                    derivation="Extracted total with VAT from totals block",
                    rule_id="R-TOT-EXTRACT-001",
                    evidence={"from": "full_text_totals", "path": "analyzeResult.content"},
                )
            )

    # Document-level charges from text (BT-99)
    import re

    charge_patterns = [
        re.compile(r"\bversandkosten\b", re.IGNORECASE),
        re.compile(r"\bporto\b", re.IGNORECASE),
        re.compile(r"\bshipping\b", re.IGNORECASE),
        re.compile(r"\bdelivery charge\b", re.IGNORECASE),
        re.compile(r"\bfreight\b", re.IGNORECASE),
    ]
    charge_amounts = []
    evidence_snippets = []
    seen_lines = set()
    for idx, line in enumerate(lines_list):
        normalized_line = " ".join(line.lower().split())
        if normalized_line in seen_lines:
            continue
        seen_lines.add(normalized_line)
        if "versandart" in line.lower():
            continue
        if "%" in line:
            continue
        matched = any(pattern.search(line) for pattern in charge_patterns)
        if not matched:
            continue
        parts = line.split(":")
        amount_text = parts[-1] if len(parts) > 1 else line
        amount = parse_decimal(amount_text)
        if amount is None and idx + 1 < len(lines_list):
            amount = parse_decimal(lines_list[idx + 1])
            if amount is not None:
                evidence_snippets.append(f"{line} {lines_list[idx + 1]}")
        if amount is not None:
            charge_amounts.append(amount)
            if line not in evidence_snippets:
                evidence_snippets.append(line)
    if charge_amounts:
        total_charges = round(sum(charge_amounts), 2)
        patches.append(
            _make_patch(
                "totals",
                "BT-99",
                f"{total_charges:.2f}",
                status="corrected",
                source="rule",
                derivation="Summed document-level charges from totals section",
                rule_id="R-TOT-CHARGE-003",
                evidence={
                    "from": "full_text_totals",
                    "snippet": " | ".join(evidence_snippets),
                    "path": "analyzeResult.content",
                },
            )
        )
        # Fill charge detail fields when missing
        bt100 = _bt(invoice.totals, "BT-100")
        bt102 = _bt(invoice.totals, "BT-102")
        bt103 = _bt(invoice.totals, "BT-103")
        bt104 = _bt(invoice.totals, "BT-104")
        bt108 = _bt(invoice.totals, "BT-108")
        if bt100 and not bt100.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-100",
                    f"{total_charges:.2f}",
                    status="derived",
                    source="derived",
                    derivation="Charge base set to charge amount",
                    rule_id="R-TOT-CHARGE-004",
                )
            )
        if bt102 and not bt102.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-102",
                    "S",
                    status="derived",
                    source="derived",
                    derivation="Standard VAT category for charges",
                    rule_id="R-TOT-CHARGE-005",
                )
            )
        if bt103 and not bt103.value:
            # Try to detect VAT rate from totals (e.g., "19% MwSt.")
            vat_rate = None
            for line in lines_list:
                if "mwst" in line.lower() or "vat" in line.lower():
                    for token in line.replace(",", ".").split():
                        if token.endswith("%"):
                            vat_rate = parse_decimal(token.strip("%"))
                            break
            if vat_rate is not None:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-103",
                        f"{vat_rate:.2f}",
                        status="derived",
                        source="derived",
                        derivation="Detected VAT rate in totals",
                        rule_id="R-TOT-CHARGE-006",
                    )
                )
        if bt104 and not bt104.value and evidence_snippets:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-104",
                    evidence_snippets[0].split(":")[0],
                    status="derived",
                    source="derived",
                    derivation="Charge reason from totals label",
                    rule_id="R-TOT-CHARGE-007",
                )
            )
        if bt108 and not bt108.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-108",
                    f"{total_charges:.2f}",
                    status="derived",
                    source="derived",
                    derivation="Sum of document-level charges",
                    rule_id="R-TOT-CHARGE-008",
                )
            )

    # Instant payment heuristics (BT-9, BT-113, BT-115) with Skonto handling
    bt81 = _bt(invoice.header, "BT-81")
    bt2 = _bt(invoice.header, "BT-2")
    bt9 = _bt(invoice.header, "BT-9")
    bt20 = _bt(invoice.header, "BT-20")
    bt112 = _bt(invoice.totals, "BT-112")
    bt113 = _bt(invoice.totals, "BT-113")
    bt115 = _bt(invoice.totals, "BT-115")
    bt92 = _bt(invoice.totals, "BT-92")
    bt94 = _bt(invoice.totals, "BT-94")
    bt93 = _bt(invoice.totals, "BT-93")
    bt97 = _bt(invoice.totals, "BT-97")
    bt98 = _bt(invoice.totals, "BT-98")
    bt107 = _bt(invoice.totals, "BT-107")
    instant_tokens = {"vorkasse", "credit card", "kreditkarte", "paypal", "ebay", "klarna", "kaufland", "amazon", "online"}
    content_lower = content.lower()
    instant_payment = False
    if bt81 and bt81.value:
        instant_payment = any(token in str(bt81.value).lower() for token in instant_tokens)
    if not instant_payment and bt20 and bt20.value:
        instant_payment = any(token in str(bt20.value).lower() for token in instant_tokens)
    if not instant_payment and any(token in content_lower for token in instant_tokens):
        instant_payment = True
        if bt81 and not bt81.value:
            if "vorkasse" in content_lower:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-81",
                        "Vorkasse",
                        status="derived",
                        source="derived",
                        derivation="Detected payment means in full text",
                        rule_id="R-HDR-PAYMEANS-LOCAL-001",
                        evidence={"from": "full_text", "path": "analyzeResult.content"},
                    )
                )
            elif "paypal" in content_lower:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-81",
                        "PayPal",
                        status="derived",
                        source="derived",
                        derivation="Detected payment means in full text",
                        rule_id="R-HDR-PAYMEANS-LOCAL-002",
                        evidence={"from": "full_text", "path": "analyzeResult.content"},
                    )
                )
            elif "kreditkarte" in content_lower or "credit card" in content_lower:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-81",
                        "Credit card",
                        status="derived",
                        source="derived",
                        derivation="Detected payment means in full text",
                        rule_id="R-HDR-PAYMEANS-LOCAL-003",
                        evidence={"from": "full_text", "path": "analyzeResult.content"},
                    )
                )

    # Payment due date from explicit dates or day terms (choose latest date if multiple)
    if bt9:
        existing_text = str(bt9.value) if bt9.value else ""
        dates = _extract_dates_from_text(existing_text)
        if dates:
            latest = max(dates)
            if bt9.value != latest:
                patches.append(
                    _make_patch(
                        "header",
                        "BT-9",
                        latest,
                        status="corrected",
                        source="rule",
                        derivation="Selected latest payment due date",
                        rule_id="R-HDR-DUEDATE-002",
                        evidence=bt9.evidence,
                    )
                )
        else:
            days = _extract_days_from_text(existing_text)
            if days and bt2 and bt2.value:
                due = _add_days(bt2.value, days)
                if due and bt9.value != due:
                    patches.append(
                        _make_patch(
                            "header",
                            "BT-9",
                            due,
                            status="derived",
                            source="derived",
                            derivation=f"Invoice date + {days} days",
                            rule_id="R-HDR-DUEDATE-003",
                            evidence=bt9.evidence,
                        )
                    )

    if bt9 and not bt9.value and bt20 and bt20.value:
        terms_text = str(bt20.value)
        dates = _extract_dates_from_text(terms_text)
        if dates:
            latest = max(dates)
            patches.append(
                _make_patch(
                    "header",
                    "BT-9",
                    latest,
                    status="derived",
                    source="derived",
                    derivation="Derived due date from payment terms (latest date)",
                    rule_id="R-HDR-DUEDATE-004",
                    evidence=bt20.evidence,
                )
            )
        else:
            days = _extract_days_from_text(terms_text)
            if days and bt2 and bt2.value:
                due = _add_days(bt2.value, days)
                if due:
                    patches.append(
                        _make_patch(
                            "header",
                            "BT-9",
                            due,
                            status="derived",
                            source="derived",
                            derivation=f"Payment terms: invoice date + {days} days",
                            rule_id="R-HDR-DUEDATE-005",
                            evidence=bt20.evidence,
                        )
                    )

    if instant_payment:
        # Payment due date = invoice date when paid immediately
        if bt9 and not bt9.value and bt2 and bt2.value:
            patches.append(
                _make_patch(
                    "header",
                    "BT-9",
                    bt2.value,
                    status="derived",
                    source="derived",
                    derivation="Instant payment: due date equals invoice date",
                    rule_id="R-HDR-DUEDATE-001",
                )
            )

        total_with_vat = parse_decimal(bt112.value) if bt112 and bt112.value else None
        if total_with_vat is None:
            total_with_vat = _find_amount_after("Gesamtbetrag in EUR") or _find_amount_after("Gesamtbetrag")

        amount_after_skonto = (
            _find_amount_after("Gesamtbetrag abzgl. Skonto in EUR")
            or _find_amount_after("Gesamtbetrag abzgl. Skonto")
            or _find_amount_after("Gesamtbetrag abzl. Skonto")
        )

        # If payment terms include Skonto percentage, derive BT-94 and BT-92
        skonto_percent = None
        skonto_amount = None
        if bt20 and bt20.value:
            import re

            terms_text = str(bt20.value).replace(",", ".")
            percent_match = re.search(r"(\\d+(?:\\.\\d+)?)\\s*%\\s*skonto", terms_text, re.IGNORECASE)
            amount_match = re.search(r"\\(([-\\d\\.]+)\\s*(?:eur|€)?\\)", terms_text, re.IGNORECASE)
            if percent_match:
                skonto_percent = parse_decimal(percent_match.group(1))
            if amount_match:
                skonto_amount = parse_decimal(amount_match.group(1))
        for line in lines_list:
            if "skonto" in line.lower():
                normalized = line.replace(",", ".")
                # Match "2% Skonto" or "2 % Skonto"
                import re

                match = re.search(r"(\\d+(?:\\.\\d+)?)\\s*%\\s*skonto", normalized, re.IGNORECASE)
                if match:
                    skonto_percent = parse_decimal(match.group(1))
                    break

        allowance = None
        if total_with_vat is not None and amount_after_skonto is not None:
            allowance = round(total_with_vat - amount_after_skonto, 2)
            if allowance < 0 or allowance > total_with_vat:
                allowance = None
        if allowance is not None and total_with_vat:
            if skonto_percent is None:
                skonto_percent = round((allowance / total_with_vat) * 100, 2)
        if allowance is None and skonto_amount is not None:
            allowance = skonto_amount

        if skonto_percent is not None:
            if bt94 and not bt94.value:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-94",
                        f"{skonto_percent:.2f}",
                        status="derived",
                        source="derived",
                        derivation="Detected Skonto percentage in payment terms",
                        rule_id="R-PAY-SKONTO-001",
                    )
                )
        if bt92 and total_with_vat is not None:
            existing_allowance = parse_decimal(bt92.value) if bt92 and bt92.value else None
            if allowance is None and skonto_percent is not None:
                allowance = round(total_with_vat * (skonto_percent / 100), 2)
            if allowance is not None and (existing_allowance is None or existing_allowance == 0):
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-92",
                        f"{allowance:.2f}",
                        status="derived",
                        source="derived",
                        derivation="BT-112 minus amount after Skonto" if amount_after_skonto is not None else f"BT-112 * {skonto_percent:.2f}% Skonto",
                        rule_id="R-PAY-SKONTO-002",
                    )
                )
        if bt93 and not bt93.value and total_with_vat is not None:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-93",
                    f"{total_with_vat:.2f}",
                    status="derived",
                    source="derived",
                    derivation="Allowance base = total with VAT",
                    rule_id="R-PAY-SKONTO-003",
                )
            )
        if bt97 and not bt97.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-97",
                    "Skonto",
                    status="derived",
                    source="derived",
                    derivation="Detected Skonto in payment terms",
                    rule_id="R-PAY-SKONTO-004",
                )
            )
        if bt98 and not bt98.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-98",
                    "SKONTO",
                    status="derived",
                    source="derived",
                    derivation="Allowance reason code placeholder for Skonto",
                    rule_id="R-PAY-SKONTO-005",
                )
            )

        # Ensure sum of allowances (BT-107)
        if bt107 and not bt107.value and bt92 and bt92.value:
            patches.append(
                _make_patch(
                    "totals",
                    "BT-107",
                    bt92.value,
                    status="derived",
                    source="derived",
                    derivation="Sum of document-level allowances",
                    rule_id="R-TOT-ALLOW-001",
                )
            )

        # Paid amount equals total with VAT minus allowance (if any), or explicit amount after Skonto
        if bt113 and (not bt113.value or bt113.status in {"derived", "wrong_math"}) and total_with_vat is not None:
            if amount_after_skonto is not None:
                paid = round(amount_after_skonto, 2)
                derivation = "Extracted amount after Skonto from totals block"
                rule_id = "R-HDR-PAID-004"
            else:
                allowance_val = parse_decimal(bt107.value) if bt107 and bt107.value else 0.0
                paid = round(total_with_vat - allowance_val, 2)
                derivation = "Instant payment: BT-112 - BT-107"
                rule_id = "R-HDR-PAID-002"
            patches.append(
                _make_patch(
                    "totals",
                    "BT-113",
                    f"{paid:.2f}",
                    status="derived",
                    source="derived",
                    derivation=derivation,
                    rule_id=rule_id,
                )
            )
        if bt115 and (not bt115.value or bt115.status in {"derived", "wrong_math"}) and total_with_vat is not None:
            allowance_val = parse_decimal(bt107.value) if bt107 and bt107.value else 0.0
            if bt113 and bt113.value:
                computed_due = round(total_with_vat - parse_decimal(bt113.value) - allowance_val, 2)
            else:
                computed_due = 0.0
            patches.append(
                _make_patch(
                    "totals",
                    "BT-115",
                    f"{computed_due:.2f}",
                    status="derived",
                    source="derived",
                    derivation="BT-112 - BT-113 - BT-107",
                    rule_id="R-HDR-PAID-003",
                )
            )

    # If due date is in the future and no paid amount, amount due equals total with VAT
    if bt115 and not bt115.value and bt113 and not bt113.value and bt112 and bt112.value and bt9 and bt9.value:
        today = datetime.utcnow().date()
        due_iso = parse_date_to_iso(bt9.value)
        if due_iso:
            try:
                due_date = datetime.strptime(due_iso, "%Y-%m-%d").date()
            except ValueError:
                due_date = None
            if due_date and due_date > today:
                patches.append(
                    _make_patch(
                        "totals",
                        "BT-115",
                        bt112.value,
                        status="derived",
                        source="derived",
                        derivation="Due date in future and no paid amount; amount due equals total with VAT",
                        rule_id="R-TOT-DUE-001",
                        evidence=bt9.evidence,
                    )
                )

    # Net/gross ambiguity resolution (line amounts)
    total_without_vat = parse_decimal(_bt(invoice.totals, "BT-109").value) if _bt(invoice.totals, "BT-109") else None
    doc_allowances = parse_decimal(_bt(invoice.totals, "BT-107").value) if _bt(invoice.totals, "BT-107") else 0.0
    doc_charges = parse_decimal(_bt(invoice.totals, "BT-108").value) if _bt(invoice.totals, "BT-108") else 0.0
    line_amounts = []
    line_allowances = 0.0
    for line in invoice.lines:
        amount = parse_decimal(line.bt.get("BT-131").value) if line.bt.get("BT-131") else None
        if amount is not None:
            line_amounts.append(amount)
        allowance = parse_decimal(line.bt.get("BT-147").value) if line.bt.get("BT-147") else None
        if allowance is not None:
            line_allowances += allowance
    sum_line_amounts = sum(line_amounts) if line_amounts else None
    treat_gross = False
    if (doc_allowances or doc_charges or line_allowances):
        treat_gross = False
    elif total_without_vat is not None and sum_line_amounts is not None:
        expected_sum = total_without_vat + (doc_allowances or 0.0) + (line_allowances or 0.0) - (doc_charges or 0.0)
        if sum_line_amounts > expected_sum * 1.001:
            treat_gross = True

    for line in invoice.lines:
        bts = line.bt
        qty = parse_decimal(bts.get("BT-129").value) if bts.get("BT-129") else None
        vat_rate = parse_decimal(bts.get("BT-152").value) if bts.get("BT-152") else None
        unit_price = parse_decimal(bts.get("BT-146").value) if bts.get("BT-146") else None
        line_amount = parse_decimal(bts.get("BT-131").value) if bts.get("BT-131") else None

        if treat_gross and vat_rate and vat_rate > 0 and line_amount is not None:
            factor = 1 + (vat_rate / 100)
            net_amount = round(line_amount / factor, 2)
            patches.append(
                _make_patch(
                    "line",
                    "BT-131",
                    f"{net_amount:.2f}",
                    line_id=line.line_id,
                    status="corrected",
                    source="rule",
                    derivation=f"{line_amount} / (1+{vat_rate}%)",
                    rule_id="R-LINE-NETGROSS-001",
                )
            )
            if unit_price is not None:
                net_unit = round(unit_price / factor, 2)
                patches.append(
                    _make_patch(
                        "line",
                        "BT-146",
                        f"{net_unit:.2f}",
                        line_id=line.line_id,
                        status="corrected",
                        source="rule",
                        derivation=f"{unit_price} / (1+{vat_rate}%)",
                        rule_id="R-LINE-NETGROSS-001",
                    )
                )
                patches.append(
                    _make_patch(
                        "line",
                        "BT-148",
                        f"{unit_price:.2f}",
                        line_id=line.line_id,
                        status="corrected",
                        source="rule",
                        derivation="Set gross price from extracted unit price",
                        rule_id="R-LINE-NETGROSS-001",
                    )
                )

        # Default UOM if missing and quantity exists
        bt130 = bts.get("BT-130")
        if bt130 and not bt130.value and qty is not None:
            patches.append(
                _make_patch(
                    "line",
                    "BT-130",
                    "C62",
                    line_id=line.line_id,
                    status="corrected",
                    source="rule",
                    derivation="Defaulted unit to pieces",
                    rule_id="R-LINE-UOM-001",
                )
            )

    return patches


def run_all_phases(invoice: CanonicalInvoice) -> List[Patch]:
    patches: List[Patch] = []
    patches.extend(phase1_normalize(invoice))
    patches.extend(phase2_derive(invoice))
    patches.extend(phase3_validate(invoice))
    patches.extend(phase4_resolve(invoice))
    # Re-run derivations that depend on Phase 4 signals (e.g., BT-81)
    patches.extend(phase2_derive(invoice))
    patches.extend(phase3_validate(invoice))
    return patches
