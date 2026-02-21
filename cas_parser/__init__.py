"""
CDSL Consolidated Account Statement (CAS) PDF Parser.

A production-grade parser for extracting investor details, mutual fund holdings,
and transaction history from CDSL CAS PDF documents.
"""

from cas_parser.models import (
    Investor,
    Holding,
    Transaction,
    TransactionType,
    CASStatement,
    ValidationResult,
)
from cas_parser.main import parse_cas_pdf

__version__ = "1.0.0"
__all__ = [
    "Investor",
    "Holding",
    "Transaction",
    "TransactionType",
    "CASStatement",
    "ValidationResult",
    "parse_cas_pdf",
]
