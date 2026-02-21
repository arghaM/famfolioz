"""
Holdings parser for CDSL CAS statements.

This module parses mutual fund holdings from the holdings summary section,
handling multi-line scheme names, various formats, and segregated portfolios.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple

from cas_parser.models import Holding

logger = logging.getLogger(__name__)


@dataclass
class ParsingContext:
    """
    Context maintained during holdings parsing.

    Attributes:
        current_amc: Current Asset Management Company being parsed
        current_registrar: Current registrar (CAMS, KFintech, etc.)
        pending_scheme_lines: Lines being accumulated for multi-line scheme name
        current_folio: Current folio number being parsed
    """
    current_amc: Optional[str] = None
    current_registrar: Optional[str] = None
    pending_scheme_lines: List[str] = field(default_factory=list)
    current_folio: Optional[str] = None
    current_isin: Optional[str] = None


class HoldingsParser:
    """
    Parser for mutual fund holdings section in CAS statements.

    Handles various formats and edge cases including:
    - Multi-line scheme names
    - Segregated portfolios
    - Different AMC/registrar groupings
    - Various date and number formats
    """

    # Regex patterns
    ISIN_PATTERN = re.compile(r"\b(INF[A-Z0-9]{9})\b")
    FOLIO_PATTERN = re.compile(
        r"(?i)folio\s*(?:no\.?|number)?\s*:?\s*([A-Z0-9/]+(?:\s*/\s*[A-Z0-9]+)?)"
    )
    UNITS_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})*(?:\.\d{3,4}))")
    NAV_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})*\.\d{2,4})")
    VALUE_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})*\.\d{2})")
    DATE_PATTERN = re.compile(
        r"(\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4})"
    )

    # AMC name patterns
    AMC_PATTERNS = [
        re.compile(r"(?i)(.*?(?:mutual\s*fund|amc|asset\s*management))"),
        re.compile(r"(?i)^(.*?(?:Mutual Fund))"),
    ]

    # Registrar patterns
    REGISTRAR_PATTERNS = [
        re.compile(r"(?i)(CAMS|KFintech|KFin|Franklin|Karvy)"),
    ]

    # Segregated portfolio marker
    SEGREGATED_PATTERN = re.compile(r"(?i)segregated|seg\.?\s*portfolio")

    def __init__(self):
        """Initialize the holdings parser."""
        self.context = ParsingContext()

    def parse(self, lines: List[str], nav_date: Optional[date] = None) -> List[Holding]:
        """
        Parse holdings from the holdings section lines.

        Args:
            lines: Text lines from the holdings section.
            nav_date: Default NAV date if not found in content.

        Returns:
            List of parsed Holding objects.
        """
        self.context = ParsingContext()
        holdings: List[Holding] = []

        logger.info(f"Parsing holdings from {len(lines)} lines")

        i = 0
        while i < len(lines):
            line = lines[i]

            # Check for AMC header
            amc = self._extract_amc(line)
            if amc:
                self.context.current_amc = amc
                logger.debug(f"Found AMC: {amc}")
                i += 1
                continue

            # Check for registrar
            registrar = self._extract_registrar(line)
            if registrar:
                self.context.current_registrar = registrar
                logger.debug(f"Found registrar: {registrar}")

            # Check for folio number
            folio_match = self.FOLIO_PATTERN.search(line)
            if folio_match:
                new_folio = folio_match.group(1).strip()
                # Reset ISIN context if folio changes (new scheme)
                if self.context.current_folio and self.context.current_folio != new_folio:
                    logger.debug(f"Folio changed from {self.context.current_folio} to {new_folio}, resetting ISIN")
                    self.context.current_isin = None
                self.context.current_folio = new_folio
                logger.debug(f"Found folio: {self.context.current_folio}")

            # Check for ISIN - primary indicator of a holding entry
            isin_match = self.ISIN_PATTERN.search(line)
            if isin_match:
                # Try to parse a complete holding
                holding, lines_consumed = self._parse_holding_block(
                    lines[i:], nav_date
                )
                if holding:
                    holdings.append(holding)
                    logger.debug(f"Parsed holding: {holding.scheme_name[:50]}...")
                i += max(1, lines_consumed)
            else:
                i += 1

        logger.info(f"Parsed {len(holdings)} holdings")

        # Log all unique scheme-ISIN pairs for verification
        seen_isins = {}
        for h in holdings:
            if h.isin and h.isin not in seen_isins:
                seen_isins[h.isin] = h.scheme_name
                logger.info(f"PARSED: ISIN={h.isin} -> Scheme='{h.scheme_name[:60]}' (folio={h.folio})")
            elif h.isin and seen_isins.get(h.isin) != h.scheme_name:
                logger.warning(
                    f"ISIN CONFLICT: {h.isin} has multiple scheme names! "
                    f"'{seen_isins[h.isin][:40]}' vs '{h.scheme_name[:40]}'"
                )

        return holdings

    def _parse_holding_block(
        self, lines: List[str], default_nav_date: Optional[date]
    ) -> Tuple[Optional[Holding], int]:
        """
        Parse a holding block starting with an ISIN line.

        Args:
            lines: Lines starting from the ISIN line.
            default_nav_date: Default NAV date to use.

        Returns:
            Tuple of (Holding or None, number of lines consumed).
        """
        if not lines:
            return None, 0

        # First line should contain ISIN
        first_line = lines[0]
        isin_match = self.ISIN_PATTERN.search(first_line)
        if not isin_match:
            return None, 0

        isin = isin_match.group(1)
        is_segregated = bool(self.SEGREGATED_PATTERN.search(first_line))

        # Extract scheme name - may be multi-line
        scheme_name, scheme_lines = self._extract_scheme_name(lines)

        # Extract folio if on this line or nearby
        folio = self.context.current_folio
        for line in lines[:min(5, len(lines))]:
            folio_match = self.FOLIO_PATTERN.search(line)
            if folio_match:
                folio = folio_match.group(1).strip()
                break

        # Extract numeric values (units, NAV, value)
        units, nav, current_value, nav_date = self._extract_numeric_values(
            lines[:min(8, len(lines))], default_nav_date
        )

        # Validate we have minimum required data
        if not all([scheme_name, isin, folio, units is not None, nav is not None]):
            logger.warning(
                f"Incomplete holding data: scheme={scheme_name[:30] if scheme_name else 'None'}, "
                f"isin={isin}, folio={folio}, units={units}, nav={nav}"
            )
            # Still create holding with available data
            if not scheme_name or units is None or nav is None:
                return None, scheme_lines

        # Calculate current value if not found
        if current_value is None and units is not None and nav is not None:
            current_value = units * nav

        holding = Holding(
            scheme_name=scheme_name or "",
            isin=isin,
            folio=folio or "",
            units=units or Decimal("0"),
            nav=nav or Decimal("0"),
            nav_date=nav_date or date.today(),
            current_value=current_value or Decimal("0"),
            registrar=self.context.current_registrar,
            amc=self.context.current_amc,
            is_segregated=is_segregated,
        )

        return holding, scheme_lines

    def _extract_scheme_name(self, lines: List[str]) -> Tuple[str, int]:
        """
        Extract scheme name which may span multiple lines.

        Args:
            lines: Lines to search for scheme name.

        Returns:
            Tuple of (scheme name, number of lines consumed).
        """
        scheme_parts: List[str] = []
        lines_consumed = 0

        for i, line in enumerate(lines[:5]):  # Look at up to 5 lines
            # Stop at numeric data lines (units, NAV, etc.)
            if re.search(r"^\s*\d+[,.\d]*\s+\d+[,.\d]*\s+\d+[,.\d]*", line):
                break

            # Stop at next ISIN (new holding)
            if i > 0 and self.ISIN_PATTERN.search(line):
                break

            # Extract text that looks like scheme name
            # Remove ISIN, folio references, and pure numeric portions
            cleaned = line
            cleaned = self.ISIN_PATTERN.sub("", cleaned)
            cleaned = self.FOLIO_PATTERN.sub("", cleaned)
            cleaned = re.sub(r"\b\d{1,3}(?:,\d{3})*\.\d+\b", "", cleaned)
            cleaned = re.sub(r"(?i)registrar\s*:", "", cleaned)
            cleaned = re.sub(r"^\s*[-:]\s*", "", cleaned)
            cleaned = cleaned.strip()

            if cleaned and len(cleaned) > 3:
                scheme_parts.append(cleaned)
            lines_consumed = i + 1

            # If we have substantial scheme name, stop
            if len(" ".join(scheme_parts)) > 40:
                break

        scheme_name = " ".join(scheme_parts)
        # Final cleanup
        scheme_name = re.sub(r"\s+", " ", scheme_name).strip()
        scheme_name = re.sub(r"^[-–—]\s*", "", scheme_name)

        return scheme_name, max(1, lines_consumed)

    def _extract_numeric_values(
        self, lines: List[str], default_nav_date: Optional[date]
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[date]]:
        """
        Extract units, NAV, current value, and NAV date from lines.

        Args:
            lines: Lines to search for numeric values.
            default_nav_date: Default NAV date.

        Returns:
            Tuple of (units, nav, current_value, nav_date).
        """
        units: Optional[Decimal] = None
        nav: Optional[Decimal] = None
        current_value: Optional[Decimal] = None
        nav_date: Optional[date] = default_nav_date

        all_numbers: List[Tuple[Decimal, str]] = []

        for line in lines:
            # Extract NAV date
            date_match = self.DATE_PATTERN.search(line)
            if date_match:
                try:
                    nav_date = datetime.strptime(
                        date_match.group(1), "%d-%b-%Y"
                    ).date()
                except ValueError:
                    pass

            # Extract all decimal numbers
            for match in re.finditer(r"(\d{1,3}(?:,\d{3})*\.\d{2,4})", line):
                try:
                    num_str = match.group(1).replace(",", "")
                    num = Decimal(num_str)
                    all_numbers.append((num, match.group(1)))
                except InvalidOperation:
                    pass

        # Heuristic assignment based on typical value ranges
        # Units: typically has 3-4 decimal places
        # NAV: typically 2-4 decimal places, range 1-10000
        # Current Value: typically 2 decimal places, larger amounts

        for num, raw in all_numbers:
            decimal_places = len(raw.split(".")[-1]) if "." in raw else 0

            if units is None and decimal_places >= 3:
                # Likely units (3+ decimal places)
                units = num
            elif nav is None and decimal_places in (2, 3, 4) and Decimal("1") <= num <= Decimal("10000"):
                # Likely NAV (reasonable range for NAV)
                nav = num
            elif current_value is None and decimal_places == 2 and num > Decimal("100"):
                # Likely current value (2 decimal places, larger amount)
                current_value = num

        # Try a different approach if needed: look for labeled values
        for line in lines:
            if nav is None and re.search(r"(?i)nav\s*:?\s*", line):
                nav_match = re.search(r"(?i)nav\s*:?\s*(\d+(?:,\d{3})*\.\d+)", line)
                if nav_match:
                    try:
                        nav = Decimal(nav_match.group(1).replace(",", ""))
                    except InvalidOperation:
                        pass

            if units is None and re.search(r"(?i)units?\s*:?\s*", line):
                units_match = re.search(r"(?i)units?\s*:?\s*(\d+(?:,\d{3})*\.\d+)", line)
                if units_match:
                    try:
                        units = Decimal(units_match.group(1).replace(",", ""))
                    except InvalidOperation:
                        pass

        return units, nav, current_value, nav_date

    def _extract_amc(self, line: str) -> Optional[str]:
        """
        Extract AMC name from a line.

        Args:
            line: Line to check.

        Returns:
            AMC name if found, otherwise None.
        """
        # Common AMC name patterns
        amc_keywords = [
            "Mutual Fund", "AMC", "Asset Management",
            "HDFC", "ICICI", "SBI", "Axis", "Kotak",
            "Nippon", "Tata", "UTI", "Aditya Birla",
            "DSP", "Franklin", "Mirae", "Parag Parikh",
        ]

        for keyword in amc_keywords:
            if re.search(rf"(?i)\b{re.escape(keyword)}\b", line):
                # Check if this looks like an AMC header
                if re.search(r"(?i)mutual\s*fund", line):
                    # Clean up the AMC name
                    amc_name = re.sub(r"(?i)(registrar\s*:.*|advisor\s*:.*)", "", line)
                    amc_name = re.sub(r"\s+", " ", amc_name).strip()
                    if len(amc_name) > 5:
                        return amc_name
        return None

    def _extract_registrar(self, line: str) -> Optional[str]:
        """
        Extract registrar name from a line.

        Args:
            line: Line to check.

        Returns:
            Registrar name if found, otherwise None.
        """
        for pattern in self.REGISTRAR_PATTERNS:
            match = pattern.search(line)
            if match:
                return match.group(1).strip()
        return None


def parse_holdings(
    lines: List[str], nav_date: Optional[date] = None
) -> List[Holding]:
    """
    Convenience function to parse holdings from text lines.

    Args:
        lines: Text lines from the holdings section.
        nav_date: Default NAV date if not found in content.

    Returns:
        List of parsed Holding objects.
    """
    parser = HoldingsParser()
    return parser.parse(lines, nav_date)
