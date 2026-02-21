"""
Data models for CDSL CAS Parser.

This module defines the core data structures using dataclasses for:
- Investor information
- Mutual fund holdings
- Transaction records
- Validation results
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, List


class TransactionType(Enum):
    """
    Enumeration of all possible transaction types in a CAS statement.

    These types cover standard mutual fund operations including purchases,
    redemptions, systematic investments, switches, dividends, and charges.
    """
    PURCHASE = "purchase"
    REDEMPTION = "redemption"
    SIP = "sip"
    STP_IN = "stp_in"
    STP_OUT = "stp_out"
    SWITCH_IN = "switch_in"
    SWITCH_OUT = "switch_out"
    DIVIDEND_PAYOUT = "dividend_payout"
    DIVIDEND_REINVESTMENT = "dividend_reinvestment"
    STT = "stt"
    STAMP_DUTY = "stamp_duty"
    CHARGES = "charges"
    SEGREGATED_PORTFOLIO = "segregated_portfolio"
    BONUS = "bonus"
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    UNKNOWN = "unknown"


@dataclass
class Investor:
    """
    Represents investor personal information from the CAS statement.

    Attributes:
        name: Full name of the investor
        pan: Permanent Account Number (10-character alphanumeric)
        email: Email address (optional)
        mobile: Mobile phone number (optional)
        address: Registered address (optional)
        dp_id: Depository Participant ID for demat holdings (optional)
        client_id: Client ID with the DP for demat holdings (optional)
    """
    name: str
    pan: str
    email: Optional[str] = None
    mobile: Optional[str] = None
    address: Optional[str] = None
    dp_id: Optional[str] = None
    client_id: Optional[str] = None

    def __post_init__(self):
        """Normalize and validate investor data."""
        self.name = self.name.strip() if self.name else ""
        self.pan = self.pan.strip().upper() if self.pan else ""
        if self.email:
            self.email = self.email.strip().lower()


@dataclass
class Holding:
    """
    Represents a mutual fund holding from the CAS statement.

    Attributes:
        scheme_name: Full name of the mutual fund scheme
        isin: International Securities Identification Number (12 characters)
        folio: Folio number for the investment
        units: Number of units held (using Decimal for precision)
        nav: Net Asset Value per unit on the statement date
        nav_date: Date of the NAV
        current_value: Total current value (units × NAV)
        registrar: Registrar and Transfer Agent (e.g., CAMS, KFintech)
        amc: Asset Management Company name
        is_segregated: Whether this is a segregated portfolio holding
    """
    scheme_name: str
    isin: str
    folio: str
    units: Decimal
    nav: Decimal
    nav_date: date
    current_value: Decimal
    registrar: Optional[str] = None
    amc: Optional[str] = None
    is_segregated: bool = False

    def __post_init__(self):
        """Normalize holding data."""
        self.scheme_name = " ".join(self.scheme_name.split()) if self.scheme_name else ""
        self.isin = self.isin.strip().upper() if self.isin else ""
        self.folio = self.folio.strip() if self.folio else ""

        # Ensure Decimal types
        if not isinstance(self.units, Decimal):
            self.units = Decimal(str(self.units))
        if not isinstance(self.nav, Decimal):
            self.nav = Decimal(str(self.nav))
        if not isinstance(self.current_value, Decimal):
            self.current_value = Decimal(str(self.current_value))


@dataclass
class Transaction:
    """
    Represents a single transaction in the CAS statement.

    Attributes:
        date: Date of the transaction
        description: Original transaction description from the statement
        transaction_type: Categorized type of transaction
        amount: Transaction amount in INR (optional for some transaction types)
        units: Number of units transacted (positive for buy, negative for sell)
        nav: NAV at which the transaction was executed (optional)
        balance_units: Running balance of units after this transaction
        folio: Folio number for this transaction
        scheme_name: Name of the scheme
        isin: ISIN of the scheme
    """
    date: date
    description: str
    transaction_type: TransactionType
    units: Decimal
    balance_units: Decimal
    folio: str
    scheme_name: str
    isin: str
    amount: Optional[Decimal] = None
    nav: Optional[Decimal] = None

    def __post_init__(self):
        """Normalize transaction data."""
        self.description = " ".join(self.description.split()) if self.description else ""
        self.folio = self.folio.strip() if self.folio else ""
        self.scheme_name = " ".join(self.scheme_name.split()) if self.scheme_name else ""
        self.isin = self.isin.strip().upper() if self.isin else ""

        # Ensure Decimal types
        if not isinstance(self.units, Decimal):
            self.units = Decimal(str(self.units))
        if not isinstance(self.balance_units, Decimal):
            self.balance_units = Decimal(str(self.balance_units))
        if self.amount is not None and not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))
        if self.nav is not None and not isinstance(self.nav, Decimal):
            self.nav = Decimal(str(self.nav))


@dataclass
class ValidationResult:
    """
    Result of validation checks on parsed CAS data.

    Attributes:
        is_valid: True if all critical validations pass
        errors: List of critical errors that indicate parsing failures
        warnings: List of non-critical issues that should be reviewed
    """
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        """Add a critical error and mark result as invalid."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """Add a non-critical warning."""
        self.warnings.append(message)

    def merge(self, other: "ValidationResult") -> None:
        """Merge another validation result into this one."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        if not other.is_valid:
            self.is_valid = False


@dataclass
class CASStatement:
    """
    Complete parsed CDSL CAS statement.

    This is the top-level container for all parsed data from a CAS PDF,
    including investor information, holdings, transactions, and validation results.

    Attributes:
        investor: Investor personal information
        holdings: List of mutual fund holdings
        transactions: List of all transactions
        statement_date: Date of the CAS statement
        validation: Results of data validation
        source_file: Path to the source PDF file (optional)
    """
    investor: Investor
    holdings: List[Holding] = field(default_factory=list)
    transactions: List[Transaction] = field(default_factory=list)
    statement_date: Optional[date] = None
    validation: ValidationResult = field(default_factory=ValidationResult)
    source_file: Optional[str] = None
    quarantine_items: List[dict] = field(default_factory=list)  # Items with broken ISINs

    def get_holdings_for_folio(self, folio: str) -> List[Holding]:
        """Get all holdings for a specific folio number."""
        return [h for h in self.holdings if h.folio == folio]

    def get_transactions_for_folio(self, folio: str) -> List[Transaction]:
        """Get all transactions for a specific folio number."""
        return [t for t in self.transactions if t.folio == folio]

    def get_transactions_for_isin(self, isin: str) -> List[Transaction]:
        """Get all transactions for a specific ISIN."""
        return [t for t in self.transactions if t.isin == isin]

    def to_dict(self) -> dict:
        """
        Convert the CAS statement to a dictionary for JSON serialization.

        Returns:
            Dictionary representation of the complete CAS statement.
        """
        return {
            "investor": {
                "name": self.investor.name,
                "pan": self.investor.pan,
                "email": self.investor.email,
                "mobile": self.investor.mobile,
                "address": self.investor.address,
                "dp_id": self.investor.dp_id,
                "client_id": self.investor.client_id,
            },
            "statement_date": self.statement_date.isoformat() if self.statement_date else None,
            "holdings": [
                {
                    "scheme_name": h.scheme_name,
                    "isin": h.isin,
                    "folio": h.folio,
                    "units": str(h.units),
                    "nav": str(h.nav),
                    "nav_date": h.nav_date.isoformat(),
                    "current_value": str(h.current_value),
                    "registrar": h.registrar,
                    "amc": h.amc,
                    "is_segregated": h.is_segregated,
                }
                for h in self.holdings
            ],
            "transactions": [
                {
                    "date": t.date.isoformat(),
                    "description": t.description,
                    "type": t.transaction_type.value,
                    "amount": str(t.amount) if t.amount is not None else None,
                    "units": str(t.units),
                    "nav": str(t.nav) if t.nav is not None else None,
                    "balance_units": str(t.balance_units),
                    "folio": t.folio,
                    "scheme_name": t.scheme_name,
                    "isin": t.isin,
                }
                for t in self.transactions
            ],
            "validation": {
                "is_valid": self.validation.is_valid,
                "errors": self.validation.errors,
                "warnings": self.validation.warnings,
            },
            "source_file": self.source_file,
            "quarantine": self.quarantine_items,
        }
