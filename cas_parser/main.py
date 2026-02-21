"""
Main entry point for CDSL CAS Parser.

This module provides the CLI interface and orchestrates the parsing
process from PDF extraction through validation and JSON export.
"""

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from cas_parser.extractor import PDFExtractor, extract_text_from_pdf
from cas_parser.holdings_parser import parse_holdings
from cas_parser.models import CASStatement, Investor, ValidationResult
from cas_parser.section_detector import (
    SectionState,
    detect_sections,
    get_all_sections_by_type,
    get_section_by_type,
)
from cas_parser.transactions_parser import parse_transactions
from cas_parser.unified_parser import parse_cas_unified
from cas_parser.validator import validate_cas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CASParser:
    """
    Main parser class for CDSL Consolidated Account Statements.

    This class orchestrates the complete parsing pipeline:
    1. Extract text from PDF
    2. Detect sections using FSM
    3. Parse investor information
    4. Parse holdings
    5. Parse transactions
    6. Validate results
    """

    # Regex patterns for investor info
    PAN_PATTERN = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b")
    EMAIL_PATTERN = re.compile(r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b")
    MOBILE_PATTERN = re.compile(r"\b(\+91[\s-]?)?([6-9]\d{9})\b")
    NAME_PATTERNS = [
        re.compile(r"(?i)(?:name|investor)\s*:?\s*(.+?)(?:\s*(?:PAN|email|mobile|address)|$)", re.IGNORECASE),
        re.compile(r"(?i)^(?:mr\.?|ms\.?|mrs\.?|dr\.?|shri\.?|smt\.?)\s*(.+)", re.IGNORECASE),
    ]
    DATE_PATTERN = re.compile(
        r"(?i)(?:statement\s+(?:for|as\s+on|period).*?)(\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4})"
    )
    DP_ID_PATTERN = re.compile(r"(?i)DP\s*ID\s*:?\s*([A-Z0-9]+)")
    CLIENT_ID_PATTERN = re.compile(r"(?i)(?:Client|BO)\s*ID\s*:?\s*([A-Z0-9]+)")

    def __init__(self, password: Optional[str] = None):
        """
        Initialize the CAS parser.

        Args:
            password: Optional password for encrypted PDFs.
        """
        self.password = password
        self.extractor = PDFExtractor(password=password)

    def parse(self, pdf_path: str) -> CASStatement:
        """
        Parse a CAS PDF file.

        Args:
            pdf_path: Path to the CAS PDF file.

        Returns:
            CASStatement with all parsed data.

        Raises:
            FileNotFoundError: If PDF file doesn't exist.
            ValueError: If PDF cannot be parsed.
        """
        logger.info(f"Starting CAS parsing: {pdf_path}")

        # Step 1: Extract text from PDF
        document = self.extractor.extract(pdf_path)
        all_lines = document.get_all_lines()

        logger.info(f"Extracted {len(all_lines)} lines from {document.total_pages} pages")

        # Step 2: Detect sections
        sections = detect_sections(all_lines)
        logger.info(f"Detected {len(sections)} sections")

        # Step 3: Extract statement date
        statement_date = self._extract_statement_date(all_lines[:30])

        # Step 4: Try unified parser first (handles interleaved format)
        # This is the common CAMS/KFintech format
        investor, holdings, transactions, quarantine_items = parse_cas_unified(all_lines)

        # If unified parser found data, use it
        if holdings or transactions:
            logger.info(f"Unified parser found {len(holdings)} holdings, {len(transactions)} transactions")
            if quarantine_items:
                logger.warning(f"Quarantined {len(quarantine_items)} items with broken ISINs")
        else:
            # Fallback to section-based parsing
            logger.info("Falling back to section-based parsing")

            # Parse investor information
            investor_section = get_section_by_type(sections, SectionState.INVESTOR_INFO)
            investor_lines = investor_section.lines if investor_section else all_lines[:50]
            investor = self._parse_investor(investor_lines)

            # Parse holdings
            holdings_sections = get_all_sections_by_type(sections, SectionState.HOLDINGS_SUMMARY)
            holdings = []
            for section in holdings_sections:
                section_holdings = parse_holdings(section.lines, nav_date=statement_date)
                holdings.extend(section_holdings)
            logger.info(f"Parsed {len(holdings)} holdings")

            # Parse transactions
            transaction_sections = get_all_sections_by_type(
                sections, SectionState.TRANSACTION_DETAILS
            )
            transactions = []
            for section in transaction_sections:
                section_transactions = parse_transactions(section.lines)
                transactions.extend(section_transactions)
            logger.info(f"Parsed {len(transactions)} transactions")

        # Step 7: Create statement
        # quarantine_items only exists from unified parser
        quarantine = quarantine_items if 'quarantine_items' in locals() else []
        statement = CASStatement(
            investor=investor,
            holdings=holdings,
            transactions=transactions,
            statement_date=statement_date,
            source_file=pdf_path,
            quarantine_items=quarantine,
        )

        # Step 8: Validate
        statement.validation = validate_cas(statement)

        logger.info(
            f"Parsing complete: {len(holdings)} holdings, "
            f"{len(transactions)} transactions, "
            f"valid={statement.validation.is_valid}"
        )

        return statement

    def _parse_investor(self, lines: list) -> Investor:
        """
        Parse investor information from section lines.

        Args:
            lines: Lines from investor info section.

        Returns:
            Parsed Investor object.
        """
        text = " ".join(lines)

        # Extract PAN
        pan = ""
        pan_match = self.PAN_PATTERN.search(text)
        if pan_match:
            pan = pan_match.group(1)

        # Extract email
        email = None
        email_match = self.EMAIL_PATTERN.search(text)
        if email_match:
            email = email_match.group(0)

        # Extract mobile
        mobile = None
        mobile_match = self.MOBILE_PATTERN.search(text)
        if mobile_match:
            mobile = mobile_match.group(2)

        # Extract name
        name = ""
        for pattern in self.NAME_PATTERNS:
            for line in lines:
                name_match = pattern.search(line)
                if name_match:
                    candidate = name_match.group(1).strip()
                    # Clean up the name
                    candidate = re.sub(r"\s+", " ", candidate)
                    candidate = re.sub(r"[,;].*$", "", candidate)
                    if len(candidate) > len(name) and len(candidate) < 100:
                        name = candidate

        # If no name found, try the first non-empty line that looks like a name
        if not name:
            for line in lines:
                cleaned = line.strip()
                if (
                    cleaned
                    and not self.PAN_PATTERN.search(cleaned)
                    and not self.EMAIL_PATTERN.search(cleaned)
                    and not re.search(r"(?i)(statement|consolidated|cas|period)", cleaned)
                    and len(cleaned) > 3
                    and len(cleaned) < 60
                ):
                    name = cleaned
                    break

        # Extract DP ID and Client ID (for demat holdings)
        dp_id = None
        client_id = None
        dp_match = self.DP_ID_PATTERN.search(text)
        if dp_match:
            dp_id = dp_match.group(1)
        client_match = self.CLIENT_ID_PATTERN.search(text)
        if client_match:
            client_id = client_match.group(1)

        return Investor(
            name=name,
            pan=pan,
            email=email,
            mobile=mobile,
            dp_id=dp_id,
            client_id=client_id,
        )

    def _extract_statement_date(self, lines: list) -> Optional[date]:
        """
        Extract the statement date from header lines.

        Args:
            lines: Header lines to search.

        Returns:
            Statement date or None.
        """
        text = " ".join(lines)

        # Try explicit statement date pattern
        date_match = self.DATE_PATTERN.search(text)
        if date_match:
            try:
                return datetime.strptime(date_match.group(1), "%d-%b-%Y").date()
            except ValueError:
                pass

        # Try any date in the header
        generic_date = re.search(
            r"(\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4})",
            text,
        )
        if generic_date:
            try:
                return datetime.strptime(generic_date.group(1), "%d-%b-%Y").date()
            except ValueError:
                pass

        return None


def parse_cas_pdf(pdf_path: str, password: Optional[str] = None) -> CASStatement:
    """
    Parse a CDSL CAS PDF file.

    This is the main entry point for programmatic use.

    Args:
        pdf_path: Path to the CAS PDF file.
        password: Optional password for encrypted PDFs.

    Returns:
        CASStatement with all parsed data.
    """
    parser = CASParser(password=password)
    return parser.parse(pdf_path)


def export_to_json(statement: CASStatement, output_path: Optional[str] = None) -> str:
    """
    Export a CAS statement to JSON.

    Args:
        statement: Parsed CAS statement.
        output_path: Optional path to write JSON file.

    Returns:
        JSON string representation.
    """
    json_data = statement.to_dict()
    json_str = json.dumps(json_data, indent=2, ensure_ascii=False)

    if output_path:
        Path(output_path).write_text(json_str, encoding="utf-8")
        logger.info(f"Exported JSON to: {output_path}")

    return json_str


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Parse CDSL Consolidated Account Statement (CAS) PDFs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s statement.pdf
  %(prog)s statement.pdf -o output.json
  %(prog)s statement.pdf --password mypass -v
        """,
    )
    parser.add_argument(
        "pdf_file",
        help="Path to the CAS PDF file",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file path (default: stdout)",
    )
    parser.add_argument(
        "-p", "--password",
        help="Password for encrypted PDF",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress all output except errors",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate, don't output full JSON",
    )

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)

    try:
        # Parse the PDF
        statement = parse_cas_pdf(args.pdf_file, password=args.password)

        if args.validate_only:
            # Output validation results only
            validation = statement.validation
            print(f"Validation: {'PASSED' if validation.is_valid else 'FAILED'}")
            if validation.errors:
                print("\nErrors:")
                for error in validation.errors:
                    print(f"  - {error}")
            if validation.warnings:
                print("\nWarnings:")
                for warning in validation.warnings:
                    print(f"  - {warning}")
            sys.exit(0 if validation.is_valid else 1)

        # Export to JSON
        json_output = export_to_json(statement, args.output)

        if not args.output:
            print(json_output)

        # Print summary to stderr
        if not args.quiet:
            print(
                f"\nParsed: {len(statement.holdings)} holdings, "
                f"{len(statement.transactions)} transactions",
                file=sys.stderr,
            )
            if not statement.validation.is_valid:
                print(
                    f"Validation errors: {len(statement.validation.errors)}",
                    file=sys.stderr,
                )
            if statement.validation.warnings:
                print(
                    f"Validation warnings: {len(statement.validation.warnings)}",
                    file=sys.stderr,
                )

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.exception("Failed to parse CAS PDF")
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
