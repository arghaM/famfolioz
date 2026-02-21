"""
Unified parser for CDSL CAS statements with interleaved format.

This module handles CAS PDFs where holdings and transactions are
interleaved per scheme, rather than in separate sections.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple

from cas_parser.models import Holding, Investor, Transaction, TransactionType
from cas_parser.isin_resolver import get_isin_resolver

logger = logging.getLogger(__name__)


@dataclass
class SchemeContext:
    """Context for current scheme being parsed."""
    amc: Optional[str] = None
    folio: Optional[str] = None
    pan: Optional[str] = None
    holder_name: Optional[str] = None
    scheme_name: Optional[str] = None
    isin: Optional[str] = None
    registrar: Optional[str] = None
    advisor: Optional[str] = None


class UnifiedCASParser:
    """
    Parser for CAS PDFs with interleaved holdings/transactions format.

    This handles the common CAMS/KFintech format where each scheme
    has its own section with transactions followed by closing balance.
    """

    # Debug mode - set to True to dump extraction details
    DEBUG_EXTRACTION = True

    # Patterns
    AMC_PATTERN = re.compile(r"^([A-Za-z\s]+(?:Mutual Fund|MF))\s*$", re.IGNORECASE)
    FOLIO_PATTERN = re.compile(
        r"Folio\s*No\s*:\s*([A-Z0-9/\s]+?)(?:\s+(?:KYC|PAN)|$)", re.IGNORECASE
    )
    PAN_EXTRACT_PATTERN = re.compile(r"PAN\s*:\s*([A-Z]{5}[0-9]{4}[A-Z])", re.IGNORECASE)
    ISIN_PATTERN = re.compile(r"ISIN\s*:\s*(INF[A-Z0-9]{9})")
    SCHEME_ISIN_PATTERN = re.compile(r"^([A-Z0-9]+)-(.+?)\s*-\s*ISIN\s*:\s*(INF[A-Z0-9]{9})")
    REGISTRAR_PATTERN = re.compile(r"Registrar\s*:\s*(\w+)", re.IGNORECASE)

    # Transaction line pattern: Date Transaction Amount Units Price Balance
    DATE_PATTERN = re.compile(r"^(\d{2}-[A-Za-z]{3}-\d{4})")
    CLOSING_PATTERN = re.compile(
        r"Closing\s*Unit\s*Balance\s*:\s*([\d,]+\.\d+)\s*"
        r"NAV\s*on\s*(\d{2}-[A-Za-z]{3}-\d{4})\s*:\s*INR\s*([\d,]+\.\d+)\s*"
        r".*?(?:Cost\s*Value|Total\s*Cost)\s*:\s*([\d,]+\.\d+)\s*"
        r"Market\s*Value.*?:\s*INR\s*([\d,]+\.\d+)",
        re.IGNORECASE
    )

    # Investor patterns
    EMAIL_PATTERN = re.compile(r"Email\s*(?:Id)?\s*:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", re.IGNORECASE)
    MOBILE_PATTERN = re.compile(r"Mobile\s*:\s*\+?(\d+)", re.IGNORECASE)
    STATEMENT_PERIOD_PATTERN = re.compile(r"(\d{2}-[A-Za-z]{3}-\d{4})\s*To\s*(\d{2}-[A-Za-z]{3}-\d{4})")

    def __init__(self):
        """Initialize the unified parser."""
        self.context = SchemeContext()
        self.holdings: List[Holding] = []
        self.transactions: List[Transaction] = []
        self.investor: Optional[Investor] = None
        self.quarantine_items: List[dict] = []  # Items with broken ISINs

    def parse(self, lines: List[str]) -> Tuple[Investor, List[Holding], List[Transaction]]:
        """
        Parse all data from CAS lines.

        Args:
            lines: All text lines from the PDF.

        Returns:
            Tuple of (Investor, holdings list, transactions list).
        """
        self.holdings = []
        self.transactions = []
        self.context = SchemeContext()

        # Clear debug log at start of parse
        if self.DEBUG_EXTRACTION:
            try:
                with open("/tmp/cas_parser_debug.log", "w") as f:
                    f.write(f"=== CAS Parser Debug Log ===\n")
                    f.write(f"Total lines to parse: {len(lines)}\n\n")
            except:
                pass

        # First pass: extract investor info
        self.investor = self._parse_investor(lines[:50])

        # Second pass: parse scheme data
        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Check for AMC header
            amc_match = self.AMC_PATTERN.match(line)
            if amc_match:
                self.context.amc = amc_match.group(1).strip()
                logger.debug(f"Found AMC: {self.context.amc}")
                i += 1
                continue

            # Check for Folio line
            folio_match = self.FOLIO_PATTERN.search(line)
            if folio_match:
                new_folio = folio_match.group(1).strip().replace(" ", "")
                # If folio changes, reset scheme context (new scheme section)
                if self.context.folio and self.context.folio != new_folio:
                    logger.debug(f"Folio changed from {self.context.folio} to {new_folio}, resetting scheme context")
                    self.context.scheme_name = None
                    self.context.isin = None
                    self.context.registrar = None
                self.context.folio = new_folio
                # Extract PAN from same line - update investor PAN if found
                pan_match = self.PAN_EXTRACT_PATTERN.search(line)
                if pan_match:
                    self.context.pan = pan_match.group(1)
                    # Update investor PAN if not set
                    if self.investor and not self.investor.pan:
                        self.investor.pan = pan_match.group(1)
                logger.debug(f"Found Folio: {self.context.folio}")
                i += 1
                continue

            # Check for scheme line with ISIN
            if "ISIN:" in line or "ISIN :" in line:
                # First, try to extract a valid ISIN from this line
                new_isin_match = self.ISIN_PATTERN.search(line)

                # If no valid ISIN found, try to recover from truncated ISIN
                if not new_isin_match:
                    # Look for partial ISIN (INF followed by some chars but not full 12)
                    partial_match = re.search(r"ISIN\s*:\s*(INF[A-Z0-9]{1,8})(?:\s|$|\()", line)
                    if partial_match:
                        partial_isin = partial_match.group(1)
                        logger.debug(f"Found partial ISIN: {partial_isin}, searching ahead for full ISIN")

                        # Search ahead up to 20 lines for full ISIN starting with same prefix
                        for lookahead in range(1, min(21, len(lines) - i)):
                            future_line = lines[i + lookahead].strip()
                            # Look for standalone full ISIN that starts with our partial
                            full_isin_match = re.search(r"\b(" + re.escape(partial_isin) + r"[A-Z0-9]{" + str(12 - len(partial_isin)) + r"})\b", future_line)
                            if full_isin_match:
                                logger.info(f"Found full ISIN {full_isin_match.group(1)} ahead at line {i + lookahead}")
                                new_isin_match = full_isin_match
                                break
                            # Also check for any ISIN pattern (might be the one we're looking for)
                            any_isin_match = re.search(r"\b(INF[A-Z0-9]{9})\b", future_line)
                            if any_isin_match:
                                # Found a different ISIN - stop searching, we've hit next scheme
                                break
                            # Stop at next scheme marker
                            if "Closing Unit Balance" in future_line or "Folio No" in future_line:
                                break

                if new_isin_match:
                    new_isin = new_isin_match.group(1)
                    # CRITICAL: Reset scheme context when we encounter a NEW ISIN
                    # This prevents scheme_name from previous scheme carrying over
                    if self.context.isin and self.context.isin != new_isin:
                        logger.debug(f"New ISIN detected ({new_isin}), resetting scheme context from {self.context.isin}")
                        self.context.scheme_name = None
                        self.context.registrar = None

                    self._parse_scheme_line(line, lines, i)
                    # Check next line for registrar
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        reg_match = self.REGISTRAR_PATTERN.search(next_line)
                        if reg_match:
                            self.context.registrar = reg_match.group(1)
                else:
                    # CRITICAL: Line has "ISIN:" but no valid ISIN extracted even after lookahead
                    logger.warning(
                        f"Line with truncated ISIN - checking manual mappings: '{line[:80]}...'"
                    )

                    # Extract partial ISIN if any
                    partial_isin = ""
                    partial_match = re.search(r"ISIN\s*:\s*(INF[A-Z0-9]*)", line)
                    if partial_match:
                        partial_isin = partial_match.group(1)

                    # Extract scheme name from the line
                    isin_pos = line.find("ISIN")
                    temp_scheme = ""
                    if isin_pos > 0:
                        temp_scheme = line[:isin_pos].strip()
                        # Remove scheme code prefix
                        code_match = re.match(r"^([A-Z0-9]{2,10})-(.+)$", temp_scheme)
                        if code_match:
                            temp_scheme = code_match.group(2).strip()

                    # Check ONLY manual mappings (not AMFI fuzzy matching)
                    # Manual mappings are created when user resolves quarantine
                    resolved_isin = None
                    try:
                        resolver = get_isin_resolver()
                        resolved_isin = resolver._check_manual_mappings(partial_isin, temp_scheme)
                        if resolved_isin:
                            logger.info(f"Found manual mapping for '{temp_scheme[:40]}': {resolved_isin}")
                            if self.DEBUG_EXTRACTION:
                                print(f"MANUAL MAPPING FOUND: {resolved_isin} for '{temp_scheme[:50]}'")
                    except Exception as e:
                        logger.debug(f"Could not check manual mappings: {e}")

                    if resolved_isin:
                        # Use the manually mapped ISIN
                        if self.context.isin and self.context.isin != resolved_isin:
                            self.context.scheme_name = None
                            self.context.registrar = None
                        self.context.isin = resolved_isin
                        self._parse_scheme_line(line, lines, i)
                    else:
                        # No manual mapping - quarantine this
                        if self.DEBUG_EXTRACTION:
                            print(f"QUARANTINE: No manual mapping for [{i}]: '{line[:80]}...'")
                            print(f"  Partial ISIN: {partial_isin}, Scheme: {temp_scheme[:50]}")

                        self.context.scheme_name = temp_scheme if temp_scheme else None
                        self.context.isin = f"UNKNOWN_{partial_isin}" if partial_isin else "UNKNOWN_"
                        self._parse_scheme_line(line, lines, i)

                i += 1
                continue

            # Also check for standalone ISIN pattern (INF followed by 9 alphanumeric)
            # This catches cases where ISIN appears without "ISIN:" prefix
            isin_standalone = re.search(r"\b(INF[A-Z0-9]{9})\b", line)
            if isin_standalone and not self.context.isin:
                # Only update if we don't have an ISIN yet for this scheme
                potential_isin = isin_standalone.group(1)
                # Validate it looks like a real ISIN (not part of other text)
                if line.strip().endswith(potential_isin) or "ISIN" in line.upper():
                    self.context.isin = potential_isin
                    logger.debug(f"Found standalone ISIN: {self.context.isin}")

            # Check for transaction line (starts with date)
            date_match = self.DATE_PATTERN.match(line)
            if date_match:
                tx = self._parse_transaction_line(line)
                if tx:
                    # Check if ISIN is broken/quarantined
                    if self._is_broken_isin(tx.isin):
                        # Add to quarantine instead of main list
                        self._add_to_quarantine('transaction', tx)
                        logger.info(
                            f"Quarantined transaction: {tx.scheme_name[:30]}... "
                            f"(folio={tx.folio}, date={tx.date}, partial_isin={tx.isin})"
                        )
                    else:
                        self.transactions.append(tx)
                i += 1
                continue

            # Check for closing balance line (holding info)
            closing_match = self.CLOSING_PATTERN.search(line)
            if closing_match:
                holding = self._parse_closing_line(closing_match)
                if holding:
                    # Check if ISIN is broken/quarantined
                    if self._is_broken_isin(holding.isin):
                        # Add to quarantine instead of main list
                        self._add_to_quarantine('holding', holding)
                        logger.info(
                            f"Quarantined holding: {holding.scheme_name[:50]}... "
                            f"(folio={holding.folio}, partial_isin={holding.isin})"
                        )
                    else:
                        self.holdings.append(holding)
                i += 1
                continue

            i += 1

        logger.info(f"Parsed {len(self.holdings)} holdings, {len(self.transactions)} transactions")
        if self.quarantine_items:
            logger.warning(f"Quarantined {len(self.quarantine_items)} items with broken ISINs")

        # Log all unique scheme-ISIN pairs for verification
        seen_isins = {}
        for h in self.holdings:
            if h.isin and h.isin not in seen_isins:
                seen_isins[h.isin] = h.scheme_name
                logger.info(f"PARSED: ISIN={h.isin} -> Scheme='{h.scheme_name[:60]}...' (folio={h.folio})")
            elif h.isin and seen_isins.get(h.isin) != h.scheme_name:
                logger.warning(
                    f"ISIN CONFLICT: {h.isin} has multiple scheme names! "
                    f"'{seen_isins[h.isin][:40]}' vs '{h.scheme_name[:40]}'"
                )

        return self.investor, self.holdings, self.transactions

    def _is_broken_isin(self, isin: str) -> bool:
        """Check if ISIN is broken/truncated (not a valid 12-char INF ISIN)."""
        if not isin:
            return True
        if isin.startswith("UNKNOWN_"):
            return True
        if not isin.startswith("INF"):
            return True
        if len(isin) != 12:
            return True
        return False

    def _add_to_quarantine(self, data_type: str, item) -> None:
        """Add a holding or transaction to quarantine."""
        # Extract partial ISIN
        isin = getattr(item, 'isin', None) or ''
        partial_isin = isin.replace('UNKNOWN_', '') if isin.startswith('UNKNOWN_') else isin

        # Convert item to dict
        if hasattr(item, 'to_dict'):
            data = item.to_dict()
        else:
            data = {
                'scheme_name': getattr(item, 'scheme_name', ''),
                'folio': getattr(item, 'folio', ''),
                'isin': isin,
            }
            if data_type == 'holding':
                data.update({
                    'units': str(getattr(item, 'units', 0)),
                    'nav': str(getattr(item, 'nav', 0)),
                    'nav_date': str(getattr(item, 'nav_date', '')),
                    'current_value': str(getattr(item, 'current_value', 0)),
                    'registrar': getattr(item, 'registrar', ''),
                })
            elif data_type == 'transaction':
                data.update({
                    'date': str(getattr(item, 'date', '')),
                    'description': getattr(item, 'description', ''),
                    'transaction_type': getattr(item, 'transaction_type', '').value if hasattr(getattr(item, 'transaction_type', ''), 'value') else str(getattr(item, 'transaction_type', '')),
                    'amount': str(getattr(item, 'amount', 0)),
                    'units': str(getattr(item, 'units', 0)),
                    'nav': str(getattr(item, 'nav', 0)),
                    'balance_units': str(getattr(item, 'balance_units', 0)),
                })

        self.quarantine_items.append({
            'partial_isin': partial_isin,
            'scheme_name': getattr(item, 'scheme_name', ''),
            'amc': self.context.amc or '',
            'folio_number': getattr(item, 'folio', ''),
            'data_type': data_type,
            'data': data,
        })

    def get_quarantine_items(self) -> List[dict]:
        """Get all quarantined items."""
        return self.quarantine_items

    def _parse_investor(self, lines: List[str]) -> Investor:
        """Parse investor information from header lines."""
        text = " ".join(lines)

        # Extract email
        email = None
        email_match = self.EMAIL_PATTERN.search(text)
        if email_match:
            email = email_match.group(1).lower()

        # Extract mobile
        mobile = None
        mobile_match = self.MOBILE_PATTERN.search(text)
        if mobile_match:
            mobile = mobile_match.group(1)

        # Extract PAN (first occurrence)
        pan = ""
        pan_match = self.PAN_EXTRACT_PATTERN.search(text)
        if pan_match:
            pan = pan_match.group(1)

        # Extract name - look for uppercase name pattern in text
        name = ""

        # First try: Look for an ALL CAPS name pattern (common in CAS)
        name_match = re.search(r"\b([A-Z]{2,}(?:\s+[A-Z]{2,})+)\b", text)
        if name_match:
            candidate = name_match.group(1)
            # Verify it's not a header or abbreviation
            skip_patterns = ["PORTFOLIO SUMMARY", "MUTUAL FUND", "CONSOLIDATED ACCOUNT",
                           "COST VALUE", "MARKET VALUE", "PAN", "KYC", "ISIN", "NAV",
                           "DIRECT PLAN", "GROWTH", "INR", "STT", "SIP", "DEMAT"]
            if not any(skip in candidate for skip in skip_patterns):
                if len(candidate) >= 5 and len(candidate) <= 50:
                    name = candidate

        # Fallback: Look for name after email line
        if not name:
            found_email_line = False
            skip_words = ["west bengal", "india", "maharashtra", "karnataka", "delhi",
                          "tamil nadu", "gujarat", "kerala", "road", "street", "lane",
                          "nagar", "colony", "sector", "block", "floor", "flat",
                          "cost value", "market value", "portfolio", "summary"]

            for i, line in enumerate(lines):
                line_clean = line.strip()
                line_lower = line_clean.lower()

                # Mark when we pass the email line
                if "email" in line_lower:
                    found_email_line = True
                    continue

                # After email line, look for name
                if found_email_line and not name:
                    # Skip header lines
                    if any(x in line_lower for x in ["consolidated", "statement", "portfolio", "mutual fund", "investor"]):
                        continue
                    # Skip date lines
                    if self.STATEMENT_PERIOD_PATTERN.search(line_clean):
                        continue
                    # Skip lines with too many numbers (likely address)
                    if re.search(r"\d{3,}", line_clean):
                        continue
                    # Skip common address words
                    if any(x in line_lower for x in skip_words):
                        continue
                    # Skip if it has @
                    if "@" in line_clean:
                        continue
                    # This might be the name
                    if len(line_clean) > 3 and len(line_clean) < 50:
                        # Check if it looks like a name (mostly letters and spaces)
                        if re.match(r"^[A-Za-z\s]+$", line_clean):
                            name = line_clean
                            break

        return Investor(
            name=name,
            pan=pan,
            email=email,
            mobile=mobile,
        )

    def _parse_scheme_line(self, line: str, all_lines: List[str] = None, current_idx: int = 0) -> None:
        """
        Parse scheme name and ISIN from scheme line.

        In some CAS formats, scheme name is on a separate line before ISIN.
        This method looks back at previous lines if scheme name is not found
        on the current line.
        """
        # Extract ISIN first
        isin_match = self.ISIN_PATTERN.search(line)
        if isin_match:
            self.context.isin = isin_match.group(1)
            logger.debug(f"Extracting scheme for ISIN: {self.context.isin}")

        # Extract scheme name (everything before ISIN)
        scheme_part = ""
        isin_pos = line.find("ISIN")
        if isin_pos > 0:
            raw_scheme = line[:isin_pos].strip()
            logger.debug(f"Raw scheme text before ISIN: '{raw_scheme}'")

            scheme_part = raw_scheme
            # Remove scheme code prefix if present (e.g., "HINSPT-")
            # The code is typically uppercase letters/numbers, followed by dash
            code_match = re.match(r"^([A-Z0-9]{2,10})-(.+)$", scheme_part)
            if code_match:
                scheme_part = code_match.group(2).strip()
                logger.debug(f"Removed scheme code prefix, result: '{scheme_part}'")

            # Clean up trailing dashes and whitespace
            scheme_part = re.sub(r"\s+", " ", scheme_part)
            scheme_part = re.sub(r"[\s\-]+$", "", scheme_part)  # Remove trailing dashes/spaces
            scheme_part = scheme_part.strip()

        # If scheme name not found on this line, look at previous lines
        # But be VERY careful - we must not pick up scheme names from previous sections
        if not scheme_part and all_lines and current_idx > 0:
            # Look back up to 3 lines for scheme name
            for lookback in range(1, min(4, current_idx + 1)):
                prev_line = all_lines[current_idx - lookback].strip()

                # Skip empty lines
                if not prev_line:
                    continue

                # STOP conditions - these indicate we've gone past the current scheme section
                # into a previous scheme's territory
                stop_patterns = [
                    r"Closing\s*Unit\s*Balance",     # End of previous scheme
                    r"Market\s*Value",               # End of previous scheme summary
                    r"ISIN\s*:\s*INF",               # Another ISIN = different scheme
                    r"\bINF[A-Z0-9]{9}\b",           # Standalone ISIN
                ]
                should_stop = False
                for pattern in stop_patterns:
                    if re.search(pattern, prev_line, re.IGNORECASE):
                        logger.debug(f"Lookback stopped at line (boundary marker): {prev_line[:50]}...")
                        should_stop = True
                        break

                if should_stop:
                    break  # Stop looking back entirely

                # Skip lines that are clearly not scheme names but don't indicate section end
                skip_patterns = [
                    r"^\s*Folio\s*No",  # Folio line
                    r"^\s*PAN\s*:",      # PAN line
                    r"^\s*KYC\s*:",      # KYC line
                    r"^\s*Registrar",    # Registrar line
                    r"^\s*\d{2}-\w{3}-\d{4}",  # Date line (transaction)
                    r"Mutual\s*Fund$",         # AMC header
                    r"^\s*Advisor\s*:",        # Advisor line
                ]
                is_skip = False
                for pattern in skip_patterns:
                    if re.search(pattern, prev_line, re.IGNORECASE):
                        is_skip = True
                        break

                if is_skip:
                    continue

                # Check if this line contains scheme-like text
                # Remove any ISIN/Registrar references that might be on the line
                candidate = prev_line
                candidate = re.sub(r"ISIN\s*:\s*INF[A-Z0-9]{9}", "", candidate)
                candidate = re.sub(r"Registrar\s*:\s*\w+", "", candidate, flags=re.IGNORECASE)
                candidate = candidate.strip()

                # A good scheme name should:
                # - Have some text (at least 10 chars for meaningful name)
                # - Not be all numbers
                # - Likely contain "Plan", "Growth", "Dividend", "Direct", "Regular", "Fund"
                if len(candidate) >= 10 and not candidate.isdigit():
                    scheme_keywords = ["Plan", "Growth", "Dividend", "Direct", "Regular", "Fund", "Option", "Index", "ETF"]
                    has_keyword = any(kw.lower() in candidate.lower() for kw in scheme_keywords)

                    # Also accept if it looks like a scheme name pattern
                    looks_like_scheme = has_keyword or re.search(r"[A-Z].*[a-z]", candidate)

                    if looks_like_scheme:
                        # Remove scheme code prefix if present
                        if "-" in candidate:
                            parts = candidate.split("-", 1)
                            if len(parts[0]) <= 10 and parts[0].isupper():
                                candidate = parts[1].strip()

                        scheme_part = re.sub(r"\s+", " ", candidate)
                        scheme_part = scheme_part.rstrip(" -")
                        logger.debug(f"Found scheme name on previous line: {scheme_part}")
                        break

        if scheme_part:
            self.context.scheme_name = scheme_part

        # DEBUG: Log extraction details for troubleshooting
        if self.DEBUG_EXTRACTION:
            debug_lines = [
                f"\n{'='*60}",
                f"ISIN EXTRACTION DEBUG for: {self.context.isin}",
                f"{'='*60}",
                f"Current line [{current_idx}]: '{line}'",
                f"Extracted scheme_name: '{self.context.scheme_name}'",
                f"Context folio: {self.context.folio}",
            ]
            if all_lines and current_idx > 0:
                debug_lines.append(f"Previous 5 lines:")
                for j in range(max(0, current_idx - 5), current_idx):
                    debug_lines.append(f"  [{j}]: '{all_lines[j].strip()}'")
            debug_lines.append(f"{'='*60}\n")

            debug_text = "\n".join(debug_lines)
            print(debug_text)

            # Also write to a debug file
            try:
                with open("/tmp/cas_parser_debug.log", "a") as f:
                    f.write(debug_text + "\n")
            except:
                pass

        if scheme_part:
            logger.debug(f"Found scheme: {self.context.scheme_name} ({self.context.isin})")
        else:
            # Log warning if we have ISIN but no scheme name - this needs investigation
            logger.warning(
                f"ISIN found without scheme name: {self.context.isin}. "
                f"Line was: '{line[:80]}...' "
                f"Context folio: {self.context.folio}"
            )

    def _validate_and_fix_transaction_values(
        self, amount: Decimal, units: Decimal, nav: Decimal
    ) -> Tuple[Decimal, Decimal, Decimal]:
        """
        Cross-validate amount, units, and NAV using the identity:
            amount = |units| × nav

        Detects and fixes a single corrupt value when the other two are consistent.
        """
        abs_units = abs(units)
        abs_amount = abs(amount)

        # Step 1: NAV range check — NAV should be positive and reasonable
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

        # Step 2: Cross-validate amount vs units × nav (when nav is valid)
        if nav > 0 and abs_units > 0:
            expected = abs_units * nav
            if expected > 0:
                ratio = abs_amount / expected
                if ratio >= Decimal("100"):
                    # Amount is wildly too large — recompute from units × nav
                    corrected_amount = expected
                    if amount < 0:
                        corrected_amount = -corrected_amount
                    logger.warning(
                        f"Correcting amount from {amount} to {corrected_amount} "
                        f"(units={units}, nav={nav}, ratio={ratio})"
                    )
                    amount = corrected_amount
                elif ratio <= Decimal("0.01"):
                    # Units are garbled — recompute from amount / nav
                    corrected_units = abs_amount / nav
                    if units < 0:
                        corrected_units = -corrected_units
                    logger.warning(
                        f"Correcting units from {units} to {corrected_units} "
                        f"(amount={amount}, nav={nav}, ratio={ratio})"
                    )
                    units = corrected_units

        return amount, units, nav

    def _parse_transaction_line(self, line: str) -> Optional[Transaction]:
        """Parse a transaction line."""
        # Pattern: Date TransactionType Amount Units Price Balance
        # Example: "01-Jan-2026 Purchase 9,999.50 198.442 50.3900 198.442"
        # Or: "01-Jan-2026 *** Stamp Duty *** 0.50"

        date_match = self.DATE_PATTERN.match(line)
        if not date_match:
            return None

        try:
            tx_date = datetime.strptime(date_match.group(1), "%d-%b-%Y").date()
        except ValueError:
            return None

        rest = line[date_match.end():].strip()

        # Check for special entries (STT, Stamp Duty, etc.)
        if "***" in rest:
            # Extract type and amount
            special_match = re.search(r"\*\*\*\s*(.+?)\s*\*\*\*\s*([\d,.]+)?", rest)
            if special_match:
                description = special_match.group(1).strip()
                amount_str = special_match.group(2) if special_match.group(2) else "0"
                amount = self._parse_decimal(amount_str)

                tx_type = self._detect_special_type(description)

                return Transaction(
                    date=tx_date,
                    description=description,
                    transaction_type=tx_type,
                    units=Decimal("0"),
                    balance_units=Decimal("0"),
                    folio=self.context.folio or "",
                    scheme_name=self.context.scheme_name or "",
                    isin=self.context.isin or "",
                    amount=amount,
                    nav=None,
                )
            return None

        # Parse regular transaction
        # Try to extract: Description Amount Units Price Balance
        parts = rest.split()
        if len(parts) < 4:
            return None

        # Find where numbers start
        description_parts = []
        number_parts = []

        for part in parts:
            # Check if this looks like a financial number
            # Must have a decimal point — pure integers (e.g. transaction ref
            # numbers like '949239426') are not financial values in CAS
            clean = part.replace(",", "").replace("(", "-").replace(")", "")
            try:
                float(clean)
                if "." not in clean:
                    # No decimal point — treat as description text
                    if not number_parts:
                        description_parts.append(part)
                    else:
                        description_parts.append(part)
                    continue
                number_parts.append(part)
            except ValueError:
                if not number_parts:  # Still in description
                    description_parts.append(part)
                else:
                    description_parts.append(part)

        description = " ".join(description_parts)

        if len(number_parts) < 3:
            return None

        # Parse numbers: Amount, Units, Price, Balance
        try:
            amount = self._parse_decimal(number_parts[0])
            units = self._parse_decimal(number_parts[1])
            nav = self._parse_decimal(number_parts[2])
            balance = self._parse_decimal(number_parts[3]) if len(number_parts) > 3 else Decimal("0")
        except (InvalidOperation, IndexError):
            return None

        # Cross-validate and fix corrupt values
        amount, units, nav = self._validate_and_fix_transaction_values(amount, units, nav)

        # Detect transaction type
        tx_type = self._detect_transaction_type(description, units)

        return Transaction(
            date=tx_date,
            description=description,
            transaction_type=tx_type,
            units=units,
            balance_units=balance,
            folio=self.context.folio or "",
            scheme_name=self.context.scheme_name or "",
            isin=self.context.isin or "",
            amount=amount,
            nav=nav,
        )

    def _parse_closing_line(self, match: re.Match) -> Optional[Holding]:
        """Parse closing balance line to create a Holding."""
        try:
            units = self._parse_decimal(match.group(1))
            nav_date = datetime.strptime(match.group(2), "%d-%b-%Y").date()
            nav = self._parse_decimal(match.group(3))
            cost_value = self._parse_decimal(match.group(4))
            market_value = self._parse_decimal(match.group(5))
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Failed to parse closing line: {e}")
            return None

        return Holding(
            scheme_name=self.context.scheme_name or "",
            isin=self.context.isin or "",
            folio=self.context.folio or "",
            units=units,
            nav=nav,
            nav_date=nav_date,
            current_value=market_value,
            registrar=self.context.registrar,
            amc=self.context.amc,
        )

    def _parse_decimal(self, value: str) -> Decimal:
        """Parse a decimal value from string."""
        # Handle parentheses for negative numbers
        clean = value.replace(",", "").strip()
        if clean.startswith("(") and clean.endswith(")"):
            clean = "-" + clean[1:-1]
        return Decimal(clean)

    def _detect_transaction_type(self, description: str, units: Decimal) -> TransactionType:
        """Detect transaction type from description."""
        desc_lower = description.lower()

        if "sip" in desc_lower or "systematic investment" in desc_lower:
            return TransactionType.SIP
        if "switch" in desc_lower and "in" in desc_lower:
            return TransactionType.SWITCH_IN
        if "switch" in desc_lower and "out" in desc_lower:
            return TransactionType.SWITCH_OUT
        if "stp" in desc_lower and "in" in desc_lower:
            return TransactionType.STP_IN
        if "stp" in desc_lower and "out" in desc_lower:
            return TransactionType.STP_OUT
        if "dividend" in desc_lower and "reinvest" in desc_lower:
            return TransactionType.DIVIDEND_REINVESTMENT
        if "dividend" in desc_lower:
            return TransactionType.DIVIDEND_PAYOUT
        if "redemption" in desc_lower or "redeem" in desc_lower:
            return TransactionType.REDEMPTION
        if "purchase" in desc_lower:
            return TransactionType.PURCHASE

        # Fallback based on units
        if units < 0:
            return TransactionType.REDEMPTION
        elif units > 0:
            return TransactionType.PURCHASE

        return TransactionType.UNKNOWN

    def _detect_special_type(self, description: str) -> TransactionType:
        """Detect type for special entries (STT, Stamp Duty, etc.)."""
        desc_lower = description.lower()

        if "stt" in desc_lower:
            return TransactionType.STT
        if "stamp" in desc_lower:
            return TransactionType.STAMP_DUTY
        if "load" in desc_lower or "charge" in desc_lower:
            return TransactionType.CHARGES

        return TransactionType.CHARGES


def parse_cas_unified(lines: List[str]) -> Tuple[Investor, List[Holding], List[Transaction], List[dict]]:
    """
    Parse CAS data using the unified parser.

    Args:
        lines: All text lines from the PDF.

    Returns:
        Tuple of (Investor, holdings list, transactions list, quarantine_items list).
    """
    parser = UnifiedCASParser()
    investor, holdings, transactions = parser.parse(lines)
    return investor, holdings, transactions, parser.get_quarantine_items()
