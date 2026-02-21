"""
NPS (National Pension System) Statement Parser.

This module parses NPS account statement PDFs and extracts:
- Subscriber information (PRAN, name, etc.)
- Investment Details (Total Contribution, Withdrawal, Current Valuation, Gain/Loss)
- Scheme holdings (Scheme Name, Units, NAV, Value)
- Transaction history (contributions/redemptions)

Focuses on "Investment Details as on" section and scheme tables.
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Optional, Tuple, Union

from extractor import PDFExtractor, ExtractedDocument
from nps_models import (
    NPSSubscriber, NPSScheme, NPSTransaction, NPSStatement,
    NPSValidationResult, ContributionType, NPSSchemeType
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> Optional[date]:
    """Parse a date string in various formats."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try different formats (including 2-digit year formats)
    formats = [
        '%d-%m-%Y', '%d/%m/%Y', '%d-%b-%Y', '%d %b %Y',
        '%d-%B-%Y', '%d %B %Y', '%m-%d-%Y', '%m/%d/%Y',
        '%Y-%m-%d', '%d.%m.%Y',
        # 2-digit year formats
        '%d-%b-%y', '%d/%m/%y', '%d-%m-%y', '%d %b %y',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def parse_decimal(value_str: str) -> Optional[Decimal]:
    """Parse a string to Decimal, handling Indian number format."""
    if not value_str:
        return None

    try:
        # Remove commas and currency symbols
        cleaned = value_str.replace(',', '').replace('Rs.', '').replace('Rs', '')
        cleaned = cleaned.replace('INR', '').replace('₹', '').strip()
        cleaned = cleaned.replace('(', '-').replace(')', '')  # Handle negative in parens
        # Remove any spaces
        cleaned = cleaned.replace(' ', '')
        if cleaned:
            return Decimal(cleaned)
        return None
    except (InvalidOperation, ValueError):
        logger.warning(f"Could not parse decimal: {value_str}")
        return None


def detect_scheme_type(text: str) -> NPSSchemeType:
    """Detect NPS scheme type from text."""
    text_upper = text.upper()

    # Check for scheme type indicators
    if re.search(r'\bE\b.*(?:TIER|EQUITY)', text_upper) or re.search(r'SCHEME[\s\-]*E\b', text_upper):
        return NPSSchemeType.SCHEME_E
    if re.search(r'\bC\b.*(?:TIER|CORP)', text_upper) or re.search(r'SCHEME[\s\-]*C\b', text_upper):
        return NPSSchemeType.SCHEME_C
    if re.search(r'\bG\b.*(?:TIER|GOV)', text_upper) or re.search(r'SCHEME[\s\-]*G\b', text_upper):
        return NPSSchemeType.SCHEME_G
    if re.search(r'\bA\b.*(?:TIER|ALT)', text_upper) or re.search(r'SCHEME[\s\-]*A\b', text_upper):
        return NPSSchemeType.SCHEME_A

    # Fallback to keyword matching
    if 'EQUITY' in text_upper:
        return NPSSchemeType.SCHEME_E
    if 'CORPORATE' in text_upper or 'BOND' in text_upper:
        return NPSSchemeType.SCHEME_C
    if 'GOVERNMENT' in text_upper or 'GILT' in text_upper or 'GOV' in text_upper:
        return NPSSchemeType.SCHEME_G
    if 'ALTERNATE' in text_upper or 'ALTERNATIVE' in text_upper:
        return NPSSchemeType.SCHEME_A

    return NPSSchemeType.UNKNOWN


def detect_contribution_type(text: str) -> ContributionType:
    """Detect contribution type from text."""
    text_upper = text.upper()

    if 'EMPLOYER' in text_upper:
        return ContributionType.EMPLOYER
    if 'VOLUNTARY' in text_upper or 'VCF' in text_upper:
        return ContributionType.VOLUNTARY
    if 'TIER II' in text_upper or 'TIER-II' in text_upper or 'TIER 2' in text_upper:
        return ContributionType.TIER_II
    if 'EMPLOYEE' in text_upper or 'EE CONT' in text_upper:
        return ContributionType.EMPLOYEE

    return ContributionType.UNKNOWN


def detect_pfm(text: str) -> str:
    """Detect Pension Fund Manager from text."""
    pfm_patterns = [
        (r'\bSBI\b', 'SBI'),
        (r'\bLIC\b', 'LIC'),
        (r'\bHDFC\b', 'HDFC'),
        (r'\bICICI\b', 'ICICI'),
        (r'\bUTI\b', 'UTI'),
        (r'\bKOTAK\b', 'Kotak'),
        (r'\bADITYA\s*BIRLA\b|\bABSL\b', 'Aditya Birla'),
        (r'\bTATA\b', 'Tata'),
        (r'\bNPS\s*TRUST\b', 'NPS Trust'),
    ]

    text_upper = text.upper()
    for pattern, pfm in pfm_patterns:
        if re.search(pattern, text_upper):
            return pfm

    return ""


def generate_nps_tx_hash(pran: str, tx_date: str, scheme_type: str,
                          amount: float, units: float) -> str:
    """
    Generate a deterministic hash for NPS transaction deduplication.
    """
    data = f"{pran}|{tx_date}|{scheme_type}|{amount:.2f}|{units:.4f}"
    return hashlib.md5(data.encode()).hexdigest()


class NPSParser:
    """
    Parser for NPS account statement PDFs.

    Focuses on extracting:
    1. Investment Details as on section (Total Contribution, Withdrawal, Current Valuation, Gain/Loss)
    2. Scheme table (Scheme Name, Total Units, Latest NAV, Value at NAV)
    3. Contribution/Redemption transactions
    """

    def __init__(self, password: Optional[str] = None):
        self.password = password
        self.extractor = PDFExtractor(password=password)

    def parse(self, pdf_path: Union[str, Path]) -> NPSStatement:
        """Parse an NPS statement PDF."""
        pdf_path = Path(pdf_path)
        logger.info(f"Parsing NPS statement: {pdf_path}")

        document = self.extractor.extract(pdf_path)
        statement = self._parse_document(document)
        statement.source_file = str(pdf_path)
        self._validate(statement)

        return statement

    def parse_from_text(self, text: str) -> NPSStatement:
        """Parse NPS statement from raw text."""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return self._parse_lines(lines)

    def _parse_document(self, document: ExtractedDocument) -> NPSStatement:
        """Parse extracted document content."""
        all_lines = document.get_all_lines()
        full_text = document.get_all_text()
        return self._parse_lines(all_lines, full_text)

    def _parse_lines(self, lines: List[str], full_text: str = None) -> NPSStatement:
        """Parse NPS statement from lines of text."""
        if full_text is None:
            full_text = "\n".join(lines)

        # Extract subscriber info
        subscriber = self._extract_subscriber(lines, full_text)

        # Create statement
        statement = NPSStatement(subscriber=subscriber)

        # Extract statement period
        self._extract_statement_period(lines, full_text, statement)

        # Extract Investment Details section (Total Contribution, Withdrawal, Current Valuation, Gain/Loss)
        self._extract_investment_details(lines, full_text, statement)

        # Extract scheme holdings from the scheme table
        statement.schemes = self._extract_scheme_table(lines, full_text)

        # If no schemes found but we have total_value, the extraction failed
        # Don't create fake schemes - just log warning
        if not statement.schemes and statement.total_value > Decimal('0'):
            logger.warning(f"Could not extract scheme details, but found total value: {statement.total_value}")

        # Extract transactions from Contribution/Redemption Details
        statement.transactions = self._extract_transactions(lines, full_text, subscriber.pran)

        return statement

    def _extract_subscriber(self, lines: List[str], full_text: str) -> NPSSubscriber:
        """Extract subscriber information."""
        pran = ""
        name = ""
        pan = None
        dob = None

        # Extract PRAN - look for 12 digit number after PRAN keyword
        pran_patterns = [
            r'PRAN\s*[:\-]?\s*(\d{12})',
            r'Permanent\s+Retirement\s+Account\s+(?:Number|No\.?)\s*[:\-]?\s*(\d{12})',
            r'(?:Account|A/C)\s*(?:Number|No\.?)\s*[:\-]?\s*(\d{12})',
        ]
        for pattern in pran_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                pran = match.group(1)
                break

        # Extract name - look for line immediately after PRAN line
        # Format: "PRAN 110165790788\nARGHA MUKHERJEE\nStatement Date..."
        for i, line in enumerate(lines):
            if 'PRAN' in line.upper() and re.search(r'\d{12}', line):
                # Next line should be the name
                if i + 1 < len(lines):
                    potential_name = lines[i + 1].strip()
                    # Name should be all caps, contain spaces, not contain numbers or special keywords
                    if (potential_name.isupper() and
                        ' ' in potential_name and
                        not re.search(r'\d', potential_name) and
                        'STATEMENT' not in potential_name and
                        'DATE' not in potential_name and
                        'ROAD' not in potential_name and
                        len(potential_name) > 3 and
                        len(potential_name) < 50):
                        name = potential_name
                        break

        # Fallback name extraction
        if not name:
            name_patterns = [
                r'(?:Subscriber|Account\s+Holder)?\s*Name\s*[:\-]?\s*([A-Z][A-Z\s\.]+?)(?:\n|PAN|PRAN|Date|$)',
                r'Dear\s+([A-Z][A-Z\s\.]+)',
            ]
            for pattern in name_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    name = match.group(1).strip()
                    name = re.sub(r'\s+', ' ', name).strip()
                    if len(name) > 3:
                        break

        # Extract PAN
        pan_match = re.search(r'PAN\s*[:\-]?\s*([A-Z]{5}[0-9]{4}[A-Z])', full_text, re.IGNORECASE)
        if pan_match:
            pan = pan_match.group(1).upper()

        # Extract DOB
        dob_match = re.search(r'(?:Date\s+of\s+Birth|DOB|D\.O\.B)\s*[:\-]?\s*(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})', full_text, re.IGNORECASE)
        if dob_match:
            dob = parse_date(dob_match.group(1))

        return NPSSubscriber(
            pran=pran,
            name=name,
            pan=pan,
            dob=dob
        )

    def _extract_statement_period(self, lines: List[str], full_text: str, statement: NPSStatement):
        """Extract statement period dates."""
        # Statement period pattern
        period_match = re.search(
            r'(?:Statement\s+)?Period\s*[:\-]?\s*(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})\s*(?:to|[-])\s*(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})',
            full_text, re.IGNORECASE
        )
        if period_match:
            statement.statement_from_date = parse_date(period_match.group(1))
            statement.statement_to_date = parse_date(period_match.group(2))
            statement.statement_date = statement.statement_to_date

        # Look for "as on" date
        as_on_match = re.search(
            r'(?:Investment\s+Details\s+)?[Aa]s\s+[Oo]n\s*[:\-]?\s*(\d{1,2}[-/\.]\w{3,9}[-/\.]\d{4}|\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})',
            full_text
        )
        if as_on_match:
            statement.statement_date = parse_date(as_on_match.group(1))

    def _extract_investment_details(self, lines: List[str], full_text: str, statement: NPSStatement):
        """
        Extract from "Investment Details as on" section.

        Table format:
        No of Contributions | Total Contribution (₹) | Total Withdrawal (₹) | Deductions | Current Valuation (₹) | Notional Gain/Loss (₹)
        60                  | 673300.76              | 0.00                 | 700.91     | 801713.95             | 128413.19

        The data row has pattern: count contribution withdrawal charges valuation gain_loss
        Where count is small int, others are decimal numbers.
        """
        # Method 1: Look for data row pattern - 6 numbers where first is small int
        # Pattern: small_int big_num small_num small_num big_num medium_num
        for line in lines:
            numbers = re.findall(r'([\d,]+\.?\d*)', line)
            if len(numbers) >= 5:
                parsed = []
                for n in numbers:
                    val = parse_decimal(n)
                    if val is not None:
                        parsed.append(val)

                if len(parsed) >= 5:
                    # Check if first number is a count (small integer, typically < 500)
                    first = parsed[0]
                    if first < Decimal('500') and first == int(first):
                        # This looks like: count, contribution, withdrawal, charges, valuation, gain_loss
                        if len(parsed) >= 6:
                            contribution = parsed[1]
                            valuation = parsed[4]
                            gain_loss = parsed[5]

                            # Validate: contribution + gain_loss ≈ valuation (approximately)
                            if contribution > Decimal('1000') and valuation > Decimal('1000'):
                                calculated_gain = valuation - contribution
                                # Allow some tolerance for charges
                                if abs(calculated_gain - gain_loss) < valuation * Decimal('0.05'):
                                    statement.total_contribution = contribution
                                    statement.total_value = valuation
                                    logger.info(f"Found Investment Details - Contribution: {contribution}, Valuation: {valuation}, Gain/Loss: {gain_loss}")
                                    return

                    # Alternative: look for pattern without count at start
                    # Just 5 numbers: contribution, withdrawal, charges, valuation, gain_loss
                    if len(parsed) >= 5:
                        # Find two largest numbers - likely contribution and valuation
                        sorted_vals = sorted(parsed, reverse=True)
                        if len(sorted_vals) >= 2 and sorted_vals[0] > Decimal('10000') and sorted_vals[1] > Decimal('10000'):
                            # The larger one is typically valuation
                            valuation = sorted_vals[0]
                            contribution = sorted_vals[1]

                            # Validate order: contribution should come before valuation in original list
                            val_idx = parsed.index(valuation)
                            contrib_idx = parsed.index(contribution)

                            if contrib_idx < val_idx:
                                statement.total_contribution = contribution
                                statement.total_value = valuation
                                logger.info(f"Found Investment Details (pattern 2) - Contribution: {contribution}, Valuation: {valuation}")
                                return

        # Method 2: Look for header keywords and find nearby numbers
        found_contribution = False
        found_valuation = False

        for i, line in enumerate(lines):
            line_upper = line.upper()

            if 'TOTAL CONTRIBUTION' in line_upper or 'CONTRIBUTION' in line_upper:
                found_contribution = True

            if 'CURRENT VALUATION' in line_upper or 'VALUATION' in line_upper:
                found_valuation = True

        # Method 3: Look for "Total" row at end of scheme table
        for line in lines:
            line_stripped = line.strip()
            if line_stripped.upper().startswith('TOTAL') and 'CONTRIBUTION' not in line.upper():
                numbers = re.findall(r'([\d,]+\.?\d+)', line)
                if numbers:
                    val = parse_decimal(numbers[-1])
                    if val and val > Decimal('10000'):
                        statement.total_value = val
                        logger.info(f"Found Total value from Total row: {val}")
                        # Don't return - continue looking for contribution

    def _extract_scheme_table(self, lines: List[str], full_text: str) -> List[NPSScheme]:
        """
        Extract scheme holdings from table.

        Actual format from PDF:
        HDFC PENSION FUND MANAGEMENT LIMITED 6422.7537 55.2747 355015.78
        SCHEME E - TIER I
        HDFC PENSION FUND MANAGEMENT LIMITED 7199.7668 29.8930 215222.62
        8.63%
        SCHEME C - TIER I
        ...

        So PFM name + numbers are on same line, scheme type is on next line.
        """
        schemes = []
        found_scheme_types = set()

        # Look for lines with PFM name followed by 3 numbers (units, nav, value)
        for i, line in enumerate(lines):
            # Check if line contains a PFM name and numbers
            pfm = detect_pfm(line)
            if not pfm:
                continue

            # Extract numbers from this line
            numbers = re.findall(r'([\d,]+\.\d{2,4})', line)
            if len(numbers) >= 3:
                units = parse_decimal(numbers[0])
                nav = parse_decimal(numbers[1])
                value = parse_decimal(numbers[2])

                if units and nav and value and value > Decimal('100'):
                    # Validate: units * nav should approximately equal value
                    calculated = units * nav
                    if abs(calculated - value) < value * Decimal('0.02'):  # 2% tolerance
                        # Look for scheme type in next few lines
                        scheme_type = NPSSchemeType.UNKNOWN
                        for j in range(i + 1, min(i + 4, len(lines))):
                            detected = detect_scheme_type(lines[j])
                            if detected != NPSSchemeType.UNKNOWN:
                                scheme_type = detected
                                break

                        # Skip if we already have this scheme type
                        if scheme_type.value in found_scheme_types:
                            continue

                        scheme = NPSScheme(
                            scheme_name=f"{pfm} Scheme {scheme_type.value} - Tier I",
                            pfm_name=pfm,
                            scheme_type=scheme_type,
                            units=units,
                            nav=nav,
                            nav_date=date.today(),
                            current_value=value,
                            tier="I"
                        )
                        schemes.append(scheme)
                        found_scheme_types.add(scheme_type.value)
                        logger.info(f"Found scheme {scheme_type.value}: Units={units}, NAV={nav}, Value={value}")

        return schemes

    def _extract_schemes_line_by_line(self, lines: List[str], full_text: str, pfm: str) -> List[NPSScheme]:
        """Extract schemes by analyzing lines around scheme type keywords."""
        schemes = []
        found_types = set()

        for i, line in enumerate(lines):
            line_upper = line.upper()

            # Check if this line contains a scheme type indicator
            scheme_type = None
            if 'SCHEME E' in line_upper or ('E' in line_upper and 'TIER' in line_upper and 'EQUITY' not in line_upper):
                scheme_type = NPSSchemeType.SCHEME_E
            elif 'SCHEME C' in line_upper or 'CORPORATE' in line_upper:
                scheme_type = NPSSchemeType.SCHEME_C
            elif 'SCHEME G' in line_upper or 'GOVERNMENT' in line_upper or 'GILT' in line_upper:
                scheme_type = NPSSchemeType.SCHEME_G
            elif 'SCHEME A' in line_upper or 'ALTERNATE' in line_upper:
                scheme_type = NPSSchemeType.SCHEME_A

            if scheme_type and scheme_type.value not in found_types:
                # Look for numbers in this line and nearby lines
                search_text = " ".join(lines[max(0, i-1):min(len(lines), i+3)])

                # Find all decimal numbers
                numbers = re.findall(r'([\d,]+\.\d{2,4})', search_text)

                if len(numbers) >= 3:
                    # Parse the numbers
                    parsed = []
                    for n in numbers:
                        val = parse_decimal(n)
                        if val and val > Decimal('0'):
                            parsed.append(val)

                    if len(parsed) >= 3:
                        # Sort to identify: typically NAV is smallest, value is largest
                        # Units usually have 4 decimal places, NAV has 4, value has 2

                        # Find the value (largest number, likely > 1000)
                        value = max(p for p in parsed if p > Decimal('100'))

                        # Find NAV (typically 10-200 range)
                        nav_candidates = [p for p in parsed if Decimal('5') < p < Decimal('200') and p != value]
                        nav = nav_candidates[0] if nav_candidates else parsed[0]

                        # Units is what's left
                        units_candidates = [p for p in parsed if p != value and p != nav]
                        units = units_candidates[0] if units_candidates else Decimal('0')

                        if units > Decimal('0') and nav > Decimal('0') and value > Decimal('100'):
                            scheme = NPSScheme(
                                scheme_name=f"NPS {pfm} Scheme {scheme_type.value}",
                                pfm_name=pfm,
                                scheme_type=scheme_type,
                                units=units,
                                nav=nav,
                                nav_date=date.today(),
                                current_value=value,
                                tier="I"
                            )
                            schemes.append(scheme)
                            found_types.add(scheme_type.value)
                            logger.info(f"Found scheme (line-by-line): {scheme_type.value} - Units: {units}, NAV: {nav}, Value: {value}")

        return schemes

    def _extract_transactions(self, lines: List[str], full_text: str, pran: str) -> List[NPSTransaction]:
        """
        Extract ALL transactions from PDF for record keeping.

        Two sections:
        1. Contribution / Redemption Details - summary transactions
        2. Transaction Details - detailed with units/NAV per scheme
        """
        transactions = []

        # Date pattern: DD-Mon-YY or DD-Mon-YYYY
        date_pattern = re.compile(r'^(\d{1,2}-[A-Za-z]{3}-\d{2,4})')

        current_section = None
        current_tx_type = ""

        for i, line in enumerate(lines):
            line_stripped = line.strip()
            line_upper = line_stripped.upper()

            # Detect section
            if 'CONTRIBUTION' in line_upper and 'REDEMPTION' in line_upper and 'DETAILS' in line_upper:
                current_section = 'contribution_summary'
                continue
            if 'TRANSACTION DETAILS' in line_upper or ('TRANSACTION' in line_upper and 'DETAILS' in line_upper):
                current_section = 'transaction_details'
                continue

            # Skip header rows
            if 'DATE' in line_upper and ('PARTICULARS' in line_upper or 'TRANSACTION' in line_upper):
                continue

            # Check for transaction row (starts with date like 01-Apr-25)
            date_match = date_pattern.match(line_stripped)
            if date_match:
                tx_date = parse_date(date_match.group(1))
                if not tx_date:
                    continue

                remaining = line_stripped[len(date_match.group(0)):].strip()

                # Detect transaction type from the line
                tx_type = "unknown"
                contrib_type = ContributionType.UNKNOWN

                if 'OPENING BALANCE' in line_upper:
                    tx_type = "opening_balance"
                elif 'BILLING' in line_upper:
                    tx_type = "billing"
                elif 'CONTRIBUTION' in line_upper:
                    tx_type = "contribution"
                    if 'EMPLOYER' in line_upper:
                        contrib_type = ContributionType.EMPLOYER
                    else:
                        contrib_type = ContributionType.EMPLOYEE
                elif 'ARREAR' in line_upper:
                    tx_type = "arrear"
                    contrib_type = ContributionType.EMPLOYER
                elif 'SCHEME' in line_upper and 'CHANGE' in line_upper:
                    tx_type = "scheme_change"
                elif 'WITHDRAWAL' in line_upper:
                    tx_type = "withdrawal"
                elif 'REGULAR' in line_upper:
                    tx_type = "regular_contribution"
                    contrib_type = ContributionType.EMPLOYER

                # Detect scheme type
                scheme_type = detect_scheme_type(line)

                # Detect PFM
                pfm = detect_pfm(line) or ""

                # Extract all numbers (including negative in parentheses)
                # Handle format: (0.5571) 48.7856 (27.18)
                numbers_raw = re.findall(r'\(?([\d,]+\.?\d*)\)?', remaining)

                # Parse numbers, handling negatives in parentheses
                amounts = []
                for match in re.finditer(r'\((\d[\d,]*\.?\d*)\)|(\d[\d,]*\.?\d+)', remaining):
                    if match.group(1):  # Negative (in parentheses)
                        val = parse_decimal(match.group(1))
                        if val:
                            amounts.append(-val)
                    elif match.group(2):  # Positive
                        val = parse_decimal(match.group(2))
                        if val:
                            amounts.append(val)

                # Identify units, nav, amount based on position and value
                units = Decimal('0')
                nav = Decimal('0')
                amount = Decimal('0')

                if len(amounts) >= 3:
                    # Transaction Details format: units, nav, amount
                    # Units typically have 4 decimals, NAV is 10-100 range, amount is larger
                    for j, val in enumerate(amounts):
                        abs_val = abs(val)
                        if Decimal('10') < abs_val < Decimal('100') and nav == Decimal('0'):
                            nav = abs_val
                        elif abs_val > Decimal('100') and amount == Decimal('0'):
                            amount = val  # Keep sign
                        elif units == Decimal('0'):
                            units = val  # Keep sign
                elif len(amounts) >= 1:
                    # Simple format - just amount
                    amount = amounts[-1]

                # Create transaction record
                if amount != Decimal('0') or units != Decimal('0'):
                    tx = NPSTransaction(
                        date=tx_date,
                        contribution_type=contrib_type if contrib_type != ContributionType.UNKNOWN else ContributionType.EMPLOYEE,
                        scheme_type=scheme_type,
                        pfm_name=pfm,
                        amount=abs(amount),
                        units=units,
                        nav=nav,
                        description=line_stripped[:200],  # Store original line for reference
                        tier="I"
                    )
                    transactions.append(tx)

        logger.info(f"Extracted {len(transactions)} transactions")
        return transactions

    def _validate(self, statement: NPSStatement):
        """Validate the parsed statement."""
        validation = statement.validation

        # Check PRAN
        if not statement.subscriber.pran:
            validation.add_error("PRAN not found in statement")
        elif len(statement.subscriber.pran) != 12:
            validation.add_error(f"Invalid PRAN length: {len(statement.subscriber.pran)}")

        # Check subscriber name
        if not statement.subscriber.name:
            validation.add_warning("Subscriber name not found")

        # Check total value
        if statement.total_value == Decimal('0'):
            validation.add_warning("Current Valuation not found in Investment Details section")

        # Check schemes
        if not statement.schemes:
            validation.add_warning("No scheme holdings found in scheme table")
        else:
            # Validate scheme values match total
            total_from_schemes = sum(s.current_value for s in statement.schemes)
            if statement.total_value > Decimal('0'):
                diff = abs(total_from_schemes - statement.total_value)
                if diff > statement.total_value * Decimal('0.05'):  # 5% tolerance
                    validation.add_warning(
                        f"Scheme totals ({total_from_schemes}) don't match Current Valuation ({statement.total_value})"
                    )


def parse_nps_statement(
    pdf_path: Union[str, Path],
    password: Optional[str] = None
) -> NPSStatement:
    """
    Convenience function to parse an NPS statement PDF.
    """
    parser = NPSParser(password=password)
    return parser.parse(pdf_path)
