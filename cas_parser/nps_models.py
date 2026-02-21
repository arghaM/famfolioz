"""
Data models for NPS (National Pension System) Statement Parser.

This module defines the core data structures for:
- NPS Subscriber information
- NPS Schemes and holdings
- NPS Transactions (contributions)
- Validation results
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, List


class ContributionType(Enum):
    """
    Enumeration of NPS contribution types.
    """
    EMPLOYEE = "employee"
    EMPLOYER = "employer"
    VOLUNTARY = "voluntary"
    TIER_II = "tier_ii"
    UNKNOWN = "unknown"


class NPSSchemeType(Enum):
    """
    NPS Scheme types based on asset class.
    """
    SCHEME_E = "E"  # Equity (up to 75%)
    SCHEME_C = "C"  # Corporate Bonds
    SCHEME_G = "G"  # Government Securities
    SCHEME_A = "A"  # Alternate Assets
    UNKNOWN = "unknown"


@dataclass
class NPSSubscriber:
    """
    Represents NPS subscriber information.

    Attributes:
        pran: Permanent Retirement Account Number (12 digits)
        name: Full name of the subscriber
        dob: Date of birth (optional)
        pan: PAN number (optional)
        email: Email address (optional)
        mobile: Mobile number (optional)
        employer_name: Name of the employer for corporate subscribers (optional)
    """
    pran: str
    name: str
    dob: Optional[date] = None
    pan: Optional[str] = None
    email: Optional[str] = None
    mobile: Optional[str] = None
    employer_name: Optional[str] = None

    def __post_init__(self):
        """Normalize and validate subscriber data."""
        self.pran = self.pran.strip().replace(" ", "") if self.pran else ""
        self.name = self.name.strip() if self.name else ""
        if self.pan:
            self.pan = self.pan.strip().upper()
        if self.email:
            self.email = self.email.strip().lower()


@dataclass
class NPSScheme:
    """
    Represents an NPS scheme/fund holding.

    Attributes:
        scheme_name: Full name of the pension fund scheme
        pfm_name: Pension Fund Manager name (e.g., SBI, LIC, HDFC)
        scheme_type: Type of scheme (E, C, G, A)
        units: Number of units held
        nav: Current NAV per unit
        nav_date: Date of the NAV
        current_value: Total current value (units * NAV)
        tier: Tier I or Tier II
    """
    scheme_name: str
    pfm_name: str
    scheme_type: NPSSchemeType
    units: Decimal
    nav: Decimal
    nav_date: date
    current_value: Decimal
    tier: str = "I"

    def __post_init__(self):
        """Normalize scheme data."""
        self.scheme_name = " ".join(self.scheme_name.split()) if self.scheme_name else ""
        self.pfm_name = self.pfm_name.strip() if self.pfm_name else ""
        self.tier = self.tier.strip().upper() if self.tier else "I"

        # Ensure Decimal types
        if not isinstance(self.units, Decimal):
            self.units = Decimal(str(self.units))
        if not isinstance(self.nav, Decimal):
            self.nav = Decimal(str(self.nav))
        if not isinstance(self.current_value, Decimal):
            self.current_value = Decimal(str(self.current_value))


@dataclass
class NPSTransaction:
    """
    Represents a single NPS transaction/contribution.

    Attributes:
        date: Date of the transaction
        contribution_type: Type of contribution (Employee/Employer/Voluntary)
        scheme_type: Target scheme type (E, C, G, A)
        pfm_name: Pension Fund Manager
        amount: Contribution amount in INR
        units: Units allotted
        nav: NAV at which units were allotted
        description: Transaction description/narration
        tier: Tier I or Tier II
    """
    date: date
    contribution_type: ContributionType
    scheme_type: NPSSchemeType
    pfm_name: str
    amount: Decimal
    units: Decimal
    nav: Decimal
    description: str = ""
    tier: str = "I"

    def __post_init__(self):
        """Normalize transaction data."""
        self.pfm_name = self.pfm_name.strip() if self.pfm_name else ""
        self.description = " ".join(self.description.split()) if self.description else ""
        self.tier = self.tier.strip().upper() if self.tier else "I"

        # Ensure Decimal types
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))
        if not isinstance(self.units, Decimal):
            self.units = Decimal(str(self.units))
        if not isinstance(self.nav, Decimal):
            self.nav = Decimal(str(self.nav))


@dataclass
class NPSValidationResult:
    """
    Result of validation checks on parsed NPS data.

    Attributes:
        is_valid: True if all critical validations pass
        errors: List of critical errors
        warnings: List of non-critical issues
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


@dataclass
class NPSStatement:
    """
    Complete parsed NPS statement.

    This is the top-level container for all parsed data from an NPS PDF.

    Attributes:
        subscriber: Subscriber information
        schemes: List of scheme holdings
        transactions: List of all transactions
        statement_date: Date of the statement
        statement_from_date: Statement period start date
        statement_to_date: Statement period end date
        total_contribution: Total contributions made
        total_value: Current portfolio value
        validation: Results of data validation
        source_file: Path to the source PDF file
    """
    subscriber: NPSSubscriber
    schemes: List[NPSScheme] = field(default_factory=list)
    transactions: List[NPSTransaction] = field(default_factory=list)
    statement_date: Optional[date] = None
    statement_from_date: Optional[date] = None
    statement_to_date: Optional[date] = None
    total_contribution: Decimal = Decimal("0")
    total_value: Decimal = Decimal("0")
    validation: NPSValidationResult = field(default_factory=NPSValidationResult)
    source_file: Optional[str] = None

    def get_transactions_by_scheme(self, scheme_type: NPSSchemeType) -> List[NPSTransaction]:
        """Get all transactions for a specific scheme type."""
        return [t for t in self.transactions if t.scheme_type == scheme_type]

    def get_transactions_by_contribution(self, contrib_type: ContributionType) -> List[NPSTransaction]:
        """Get all transactions for a specific contribution type."""
        return [t for t in self.transactions if t.contribution_type == contrib_type]

    def to_dict(self) -> dict:
        """Convert the NPS statement to a dictionary for JSON serialization."""
        return {
            "subscriber": {
                "pran": self.subscriber.pran,
                "name": self.subscriber.name,
                "dob": self.subscriber.dob.isoformat() if self.subscriber.dob else None,
                "pan": self.subscriber.pan,
                "email": self.subscriber.email,
                "mobile": self.subscriber.mobile,
                "employer_name": self.subscriber.employer_name,
            },
            "statement_date": self.statement_date.isoformat() if self.statement_date else None,
            "statement_from_date": self.statement_from_date.isoformat() if self.statement_from_date else None,
            "statement_to_date": self.statement_to_date.isoformat() if self.statement_to_date else None,
            "total_contribution": str(self.total_contribution),
            "total_value": str(self.total_value),
            "schemes": [
                {
                    "scheme_name": s.scheme_name,
                    "pfm_name": s.pfm_name,
                    "scheme_type": s.scheme_type.value,
                    "units": str(s.units),
                    "nav": str(s.nav),
                    "nav_date": s.nav_date.isoformat(),
                    "current_value": str(s.current_value),
                    "tier": s.tier,
                }
                for s in self.schemes
            ],
            "transactions": [
                {
                    "date": t.date.isoformat(),
                    "contribution_type": t.contribution_type.value,
                    "scheme_type": t.scheme_type.value,
                    "pfm_name": t.pfm_name,
                    "amount": str(t.amount),
                    "units": str(t.units),
                    "nav": str(t.nav),
                    "description": t.description,
                    "tier": t.tier,
                }
                for t in self.transactions
            ],
            "validation": {
                "is_valid": self.validation.is_valid,
                "errors": self.validation.errors,
                "warnings": self.validation.warnings,
            },
            "source_file": self.source_file,
        }
