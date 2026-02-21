"""
Transaction parser for CDSL CAS statements.

This module parses transaction history from CAS statements, handling
various transaction types including purchases, redemptions, switches,
dividends, STT, and segregated portfolio entries.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from cas_parser.models import Transaction, TransactionType

logger = logging.getLogger(__name__)


@dataclass
class TransactionContext:
    """
    Context maintained during transaction parsing.

    Attributes:
        current_scheme: Current scheme name being parsed
        current_folio: Current folio number
        current_isin: Current ISIN
        current_amc: Current AMC name
    """
    current_scheme: Optional[str] = None
    current_folio: Optional[str] = None
    current_isin: Optional[str] = None
    current_amc: Optional[str] = None


class TransactionTypeDetector:
    """
    Detects transaction type from description text.

    Uses keyword matching and pattern recognition to categorize
    transactions into appropriate types.
    """

    # Transaction type patterns (order matters - more specific first)
    TYPE_PATTERNS: List[Tuple[TransactionType, List[str]]] = [
        (TransactionType.SIP, [
            r"(?i)systematic\s*investment",
            r"(?i)\bSIP\b",
            r"(?i)auto\s*debit",
        ]),
        (TransactionType.STP_IN, [
            r"(?i)STP\s*-?\s*in",
            r"(?i)systematic\s*transfer.*in",
        ]),
        (TransactionType.STP_OUT, [
            r"(?i)STP\s*-?\s*out",
            r"(?i)systematic\s*transfer.*out",
        ]),
        (TransactionType.SWITCH_IN, [
            r"(?i)switch\s*-?\s*in",
            r"(?i)switched\s*in",
            r"(?i)switch\s*from",
        ]),
        (TransactionType.SWITCH_OUT, [
            r"(?i)switch\s*-?\s*out",
            r"(?i)switched\s*out",
            r"(?i)switch\s*to",
        ]),
        (TransactionType.DIVIDEND_REINVESTMENT, [
            r"(?i)dividend\s*reinvest",
            r"(?i)reinvest.*dividend",
            r"(?i)div\.\s*reinv",
        ]),
        (TransactionType.DIVIDEND_PAYOUT, [
            r"(?i)dividend\s*payout",
            r"(?i)dividend\s*pay",
            r"(?i)div\.\s*payout",
        ]),
        (TransactionType.STT, [
            r"(?i)\bSTT\b",
            r"(?i)securities\s*transaction\s*tax",
        ]),
        (TransactionType.STAMP_DUTY, [
            r"(?i)stamp\s*duty",
        ]),
        (TransactionType.CHARGES, [
            r"(?i)exit\s*load",
            r"(?i)expense\s*ratio",
            r"(?i)management\s*fee",
            r"(?i)charges?",
        ]),
        (TransactionType.SEGREGATED_PORTFOLIO, [
            r"(?i)segregat",
            r"(?i)seg\.\s*portfolio",
        ]),
        (TransactionType.BONUS, [
            r"(?i)bonus",
        ]),
        (TransactionType.TRANSFER_IN, [
            r"(?i)transfer\s*-?\s*in",
            r"(?i)transmission",
        ]),
        (TransactionType.TRANSFER_OUT, [
            r"(?i)transfer\s*-?\s*out",
        ]),
        (TransactionType.REDEMPTION, [
            r"(?i)redemption",
            r"(?i)redeem",
            r"(?i)withdrawal",
        ]),
        (TransactionType.PURCHASE, [
            r"(?i)purchase",
            r"(?i)subscription",
            r"(?i)new\s*investment",
            r"(?i)additional\s*purchase",
        ]),
    ]

    def __init__(self):
        """Initialize the type detector with compiled patterns."""
        self.compiled_patterns: List[Tuple[TransactionType, List[re.Pattern]]] = [
            (tx_type, [re.compile(p) for p in patterns])
            for tx_type, patterns in self.TYPE_PATTERNS
        ]

    def detect(self, description: str, units: Decimal) -> TransactionType:
        """
        Detect transaction type from description and units.

        Args:
            description: Transaction description text.
            units: Number of units (positive or negative).

        Returns:
            Detected TransactionType.
        """
        description = description.strip()

        # Check pattern matches
        for tx_type, patterns in self.compiled_patterns:
            for pattern in patterns:
                if pattern.search(description):
                    return tx_type

        # Fallback based on units sign
        if units < 0:
            return TransactionType.REDEMPTION
        elif units > 0:
            return TransactionType.PURCHASE

        return TransactionType.UNKNOWN


class TransactionsParser:
    """
    Parser for transaction history section in CAS statements.

    Handles various transaction formats and edge cases including:
    - Multiple date formats
    - Multi-line transactions
    - Various transaction types
    - Running balance tracking
    - STT and charges (not treated as redemptions)
    """

    # Regex patterns
    DATE_PATTERNS = [
        re.compile(r"(\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4})"),
        re.compile(r"(\d{2}/\d{2}/\d{4})"),
        re.compile(r"(\d{4}-\d{2}-\d{2})"),
    ]
    ISIN_PATTERN = re.compile(r"\b(INF[A-Z0-9]{9})\b")
    FOLIO_PATTERN = re.compile(
        r"(?i)folio\s*(?:no\.?|number)?\s*:?\s*([A-Z0-9/]+(?:\s*/\s*[A-Z0-9]+)?)"
    )
    AMOUNT_PATTERN = re.compile(
        r"(?:Rs\.?|INR|₹)?\s*(-?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
    )
    UNITS_PATTERN = re.compile(r"(-?\d{1,3}(?:,\d{3})*\.\d{3,4})")
    NAV_PATTERN = re.compile(r"(\d{1,3}(?:,\d{3})*\.\d{2,4})")
    BALANCE_PATTERN = re.compile(r"(?:balance|bal\.?)\s*:?\s*(\d+\.\d{3,4})", re.IGNORECASE)

    def __init__(self):
        """Initialize the transactions parser."""
        self.context = TransactionContext()
        self.type_detector = TransactionTypeDetector()

    def parse(self, lines: List[str]) -> List[Transaction]:
        """
        Parse transactions from the transaction section lines.

        Args:
            lines: Text lines from the transaction section.

        Returns:
            List of parsed Transaction objects.
        """
        self.context = TransactionContext()
        transactions: List[Transaction] = []

        logger.info(f"Parsing transactions from {len(lines)} lines")

        i = 0
        while i < len(lines):
            line = lines[i]

            # Update context from header lines
            self._update_context(line)

            # Check for ISIN (new scheme section)
            isin_match = self.ISIN_PATTERN.search(line)
            if isin_match:
                self.context.current_isin = isin_match.group(1)
                # Extract scheme name from this line
                scheme_name = self._extract_scheme_name(line)
                if scheme_name:
                    self.context.current_scheme = scheme_name
                i += 1
                continue

            # Check for folio
            folio_match = self.FOLIO_PATTERN.search(line)
            if folio_match:
                self.context.current_folio = folio_match.group(1).strip()

            # Try to parse a transaction line
            tx_date = self._extract_date(line)
            if tx_date:
                # This might be a transaction line
                transaction, lines_consumed = self._parse_transaction_block(
                    lines[i:], tx_date
                )
                if transaction:
                    transactions.append(transaction)
                    logger.debug(
                        f"Parsed transaction: {tx_date} {transaction.transaction_type.value}"
                    )
                i += max(1, lines_consumed)
            else:
                i += 1

        logger.info(f"Parsed {len(transactions)} transactions")
        return transactions

    def _update_context(self, line: str) -> None:
        """
        Update parsing context from header/section lines.

        Args:
            line: Line to check for context updates.
        """
        # Check for folio
        folio_match = self.FOLIO_PATTERN.search(line)
        if folio_match:
            new_folio = folio_match.group(1).strip()
            # Reset ISIN and scheme context if folio changes
            if self.context.current_folio and self.context.current_folio != new_folio:
                logger.debug(f"Folio changed from {self.context.current_folio} to {new_folio}, resetting context")
                self.context.current_isin = None
                self.context.current_scheme = None
            self.context.current_folio = new_folio

        # Check for ISIN
        isin_match = self.ISIN_PATTERN.search(line)
        if isin_match:
            self.context.current_isin = isin_match.group(1)

    def _extract_date(self, line: str) -> Optional[date]:
        """
        Extract date from a line, trying multiple formats.

        Args:
            line: Line to search for date.

        Returns:
            Parsed date or None.
        """
        for pattern in self.DATE_PATTERNS:
            match = pattern.search(line)
            if match:
                date_str = match.group(1)
                try:
                    if "-" in date_str and not date_str[0].isdigit() or len(date_str.split("-")[0]) == 2:
                        # DD-Mon-YYYY or DD-MM-YYYY
                        if any(m in date_str for m in ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]):
                            return datetime.strptime(date_str, "%d-%b-%Y").date()
                    if "/" in date_str:
                        # DD/MM/YYYY
                        return datetime.strptime(date_str, "%d/%m/%Y").date()
                    if date_str.startswith("20") and "-" in date_str:
                        # YYYY-MM-DD
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue
        return None

    def _extract_scheme_name(self, line: str) -> Optional[str]:
        """
        Extract scheme name from a line containing ISIN.

        Args:
            line: Line to extract scheme name from.

        Returns:
            Scheme name or None.
        """
        # Remove ISIN and folio references
        cleaned = line
        cleaned = self.ISIN_PATTERN.sub("", cleaned)
        cleaned = self.FOLIO_PATTERN.sub("", cleaned)
        cleaned = re.sub(r"\d{1,3}(?:,\d{3})*\.\d+", "", cleaned)
        cleaned = re.sub(r"(?i)registrar\s*:.*", "", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        cleaned = re.sub(r"^[-:]+\s*", "", cleaned)

        if cleaned and len(cleaned) > 3:
            return cleaned
        return None

    def _parse_transaction_block(
        self, lines: List[str], tx_date: date
    ) -> Tuple[Optional[Transaction], int]:
        """
        Parse a transaction block starting with a date line.

        Args:
            lines: Lines starting from the transaction date line.
            tx_date: Extracted transaction date.

        Returns:
            Tuple of (Transaction or None, number of lines consumed).
        """
        if not lines:
            return None, 0

        first_line = lines[0]

        # Extract description (text portion after date)
        description = self._extract_description(first_line)

        # Look for numeric values in this line and potentially next few lines
        amount, units, nav, balance = self._extract_transaction_values(
            lines[:min(3, len(lines))]
        )

        # If no units found, this might not be a transaction line
        if units is None:
            return None, 1

        # Detect transaction type
        tx_type = self.type_detector.detect(description, units)

        # For STT/charges, units might be 0 or very small
        if tx_type in (TransactionType.STT, TransactionType.STAMP_DUTY, TransactionType.CHARGES):
            # These don't affect unit balance significantly
            if balance is None:
                balance = Decimal("0")

        # Create transaction
        transaction = Transaction(
            date=tx_date,
            description=description,
            transaction_type=tx_type,
            units=units,
            balance_units=balance or Decimal("0"),
            folio=self.context.current_folio or "",
            scheme_name=self.context.current_scheme or "",
            isin=self.context.current_isin or "",
            amount=amount,
            nav=nav,
        )

        return transaction, 1

    def _extract_description(self, line: str) -> str:
        """
        Extract transaction description from a line.

        Args:
            line: Line containing the transaction.

        Returns:
            Extracted description text.
        """
        # Remove date patterns
        description = line
        for pattern in self.DATE_PATTERNS:
            description = pattern.sub("", description)

        # Remove numeric values (keep words)
        description = re.sub(r"\b\d{1,3}(?:,\d{3})*\.\d+\b", "", description)
        description = re.sub(r"\b\d{1,3}(?:,\d{3})+\b", "", description)

        # Remove currency symbols
        description = re.sub(r"(?:Rs\.?|INR|₹)", "", description)

        # Clean up
        description = re.sub(r"\s+", " ", description).strip()
        description = re.sub(r"^[-–—:]+\s*", "", description)
        description = re.sub(r"\s*[-–—:]+$", "", description)

        return description

    def _validate_and_fix_transaction_values(
        self,
        amount: Optional[Decimal],
        units: Optional[Decimal],
        nav: Optional[Decimal],
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """
        Cross-validate amount, units, and NAV using the identity:
            amount = |units| × nav

        Detects and fixes a single corrupt value when the other two are consistent.
        """
        if amount is None or units is None or nav is None:
            return amount, units, nav

        abs_units = abs(units)
        abs_amount = abs(amount)

        # Step 1: NAV range check
        if nav <= 0 or nav > Decimal("100000"):
            if abs_amount > 0 and abs_units > 0:
                recomputed_nav = abs_amount / abs_units
                if Decimal("1") <= recomputed_nav <= Decimal("100000"):
                    logger.warning(
                        f"Correcting NAV from {nav} to {recomputed_nav} "
                        f"(amount={amount}, units={units})"
                    )
                    nav = recomputed_nav
                else:
                    logger.warning(
                        f"NAV={nav} is out of range but recomputed NAV={recomputed_nav} "
                        f"also invalid — leaving as-is"
                    )

        # Step 2: Cross-validate amount vs units × nav
        if nav > 0 and abs_units > 0:
            expected = abs_units * nav
            if expected > 0:
                ratio = abs_amount / expected
                if ratio >= Decimal("100"):
                    corrected_amount = expected
                    if amount < 0:
                        corrected_amount = -corrected_amount
                    logger.warning(
                        f"Correcting amount from {amount} to {corrected_amount} "
                        f"(units={units}, nav={nav}, ratio={ratio})"
                    )
                    amount = corrected_amount
                elif ratio <= Decimal("0.01"):
                    corrected_units = abs_amount / nav
                    if units < 0:
                        corrected_units = -corrected_units
                    logger.warning(
                        f"Correcting units from {units} to {corrected_units} "
                        f"(amount={amount}, nav={nav}, ratio={ratio})"
                    )
                    units = corrected_units

        return amount, units, nav

    def _extract_transaction_values(
        self, lines: List[str]
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """
        Extract amount, units, NAV, and balance from transaction lines.

        Args:
            lines: Lines to search for values.

        Returns:
            Tuple of (amount, units, nav, balance).
        """
        amount: Optional[Decimal] = None
        units: Optional[Decimal] = None
        nav: Optional[Decimal] = None
        balance: Optional[Decimal] = None

        all_text = " ".join(lines)

        # Look for explicit balance
        balance_match = self.BALANCE_PATTERN.search(all_text)
        if balance_match:
            try:
                balance = Decimal(balance_match.group(1).replace(",", ""))
            except InvalidOperation:
                pass

        # Collect all decimal numbers from lines
        numbers: List[Tuple[Decimal, str, int]] = []  # (value, raw_string, decimal_places)

        for line in lines:
            # Match numbers: optional minus OR parenthesized for negatives
            # Examples: 54,972.00  -5,000.000  (54,972.00)  (5,000.000)
            for match in re.finditer(
                r"(\(?\-?\d{1,3}(?:,\d{3})*\.\d{2,4}\)?)", line
            ):
                raw = match.group(1)
                # Convert parenthesized notation to negative
                is_paren_negative = raw.startswith("(") and raw.endswith(")")
                if is_paren_negative:
                    raw = raw[1:-1]  # strip parens
                decimal_places = len(raw.split(".")[-1])
                try:
                    num = Decimal(raw.replace(",", ""))
                    if is_paren_negative:
                        num = -num
                    numbers.append((num, raw, decimal_places))
                except InvalidOperation:
                    pass

        # Heuristic assignment based on decimal places and values
        # Units: 3-4 decimal places
        # NAV: 2-4 decimal places, typically 10-1000
        # Amount: 2 decimal places, larger values
        # Balance: 3-4 decimal places

        for num, raw, decimal_places in numbers:
            abs_num = abs(num)

            if units is None and decimal_places >= 3:
                units = num
            elif nav is None and decimal_places in (2, 3, 4) and Decimal("1") <= abs_num <= Decimal("10000"):
                # Could be NAV
                if units is not None and abs_num < abs(units):
                    nav = num
            elif amount is None and decimal_places == 2 and abs_num > Decimal("100"):
                amount = abs_num  # Amount is typically positive
            elif balance is None and decimal_places >= 3 and num >= 0:
                balance = num

        # If we only have one 3+ decimal number and no clear units, use it as units
        three_plus_decimals = [n for n in numbers if n[2] >= 3]
        if units is None and len(three_plus_decimals) >= 1:
            # Take the one that's more likely units based on position/context
            for num, _, _ in three_plus_decimals:
                if units is None:
                    units = num
                elif balance is None and num >= 0:
                    balance = num

        # Cross-validate and fix corrupt values
        amount, units, nav = self._validate_and_fix_transaction_values(amount, units, nav)

        return amount, units, nav, balance


def parse_transactions(lines: List[str]) -> List[Transaction]:
    """
    Convenience function to parse transactions from text lines.

    Args:
        lines: Text lines from the transaction section.

    Returns:
        List of parsed Transaction objects.
    """
    parser = TransactionsParser()
    return parser.parse(lines)


def classify_transaction(description: str, units: Decimal) -> TransactionType:
    """
    Classify a transaction based on its description and units.

    Args:
        description: Transaction description text.
        units: Number of units (positive or negative).

    Returns:
        Classified TransactionType.
    """
    detector = TransactionTypeDetector()
    return detector.detect(description, units)
