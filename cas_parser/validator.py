"""
Validation module for CDSL CAS Parser.

This module implements validation rules to verify the integrity
and consistency of parsed CAS data.
"""

import logging
import re
from decimal import Decimal
from typing import Dict, List, Optional, Set

from cas_parser.models import (
    CASStatement,
    Holding,
    Investor,
    Transaction,
    TransactionType,
    ValidationResult,
)

logger = logging.getLogger(__name__)

# Validation constants
VALUE_TOLERANCE = Decimal("0.01")  # 1% tolerance for value calculations
UNITS_TOLERANCE = Decimal("0.001")  # Tolerance for unit comparisons
ISIN_PATTERN = re.compile(r"^INF[A-Z0-9]{9}$")
PAN_PATTERN = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")


class CASValidator:
    """
    Validator for CDSL CAS parsed data.

    Implements multiple validation rules including:
    - Value calculations (units × NAV ≈ current_value)
    - Unit balance consistency (sum of transactions ≈ holding units)
    - Format validation (ISIN, PAN patterns)
    - Anomaly detection
    """

    def __init__(
        self,
        value_tolerance: Decimal = VALUE_TOLERANCE,
        units_tolerance: Decimal = UNITS_TOLERANCE,
    ):
        """
        Initialize the validator.

        Args:
            value_tolerance: Tolerance for value calculations (as decimal, e.g., 0.01 = 1%).
            units_tolerance: Tolerance for unit comparisons.
        """
        self.value_tolerance = value_tolerance
        self.units_tolerance = units_tolerance

    def validate(self, statement: CASStatement) -> ValidationResult:
        """
        Perform complete validation of a CAS statement.

        Args:
            statement: Parsed CAS statement to validate.

        Returns:
            ValidationResult with errors and warnings.
        """
        result = ValidationResult()

        logger.info("Starting CAS validation")

        # Validate investor
        investor_result = self.validate_investor(statement.investor)
        result.merge(investor_result)

        # Validate each holding
        for holding in statement.holdings:
            holding_result = self.validate_holding(holding)
            result.merge(holding_result)

        # Validate transactions
        for transaction in statement.transactions:
            tx_result = self.validate_transaction(transaction)
            result.merge(tx_result)

        # Validate holding-transaction consistency
        consistency_result = self.validate_holdings_transactions_consistency(
            statement.holdings, statement.transactions
        )
        result.merge(consistency_result)

        # Check for orphaned data
        orphan_result = self._check_orphaned_data(statement)
        result.merge(orphan_result)

        logger.info(
            f"Validation complete: valid={result.is_valid}, "
            f"errors={len(result.errors)}, warnings={len(result.warnings)}"
        )

        return result

    def validate_investor(self, investor: Investor) -> ValidationResult:
        """
        Validate investor information.

        Args:
            investor: Investor data to validate.

        Returns:
            ValidationResult for investor validation.
        """
        result = ValidationResult()

        # Validate PAN
        if not investor.pan:
            result.add_error("Investor PAN is missing")
        elif not PAN_PATTERN.match(investor.pan):
            result.add_error(f"Invalid PAN format: {investor.pan}")

        # Validate name
        if not investor.name:
            result.add_error("Investor name is missing")
        elif len(investor.name) < 2:
            result.add_warning(f"Investor name seems too short: {investor.name}")

        # Validate email if present
        if investor.email and "@" not in investor.email:
            result.add_warning(f"Invalid email format: {investor.email}")

        return result

    def validate_holding(self, holding: Holding) -> ValidationResult:
        """
        Validate a single holding.

        Args:
            holding: Holding data to validate.

        Returns:
            ValidationResult for holding validation.
        """
        result = ValidationResult()

        # Validate ISIN
        if not holding.isin:
            result.add_error(f"Missing ISIN for holding: {holding.scheme_name[:50]}")
        elif not ISIN_PATTERN.match(holding.isin):
            result.add_error(f"Invalid ISIN format: {holding.isin}")

        # Validate folio
        if not holding.folio:
            result.add_warning(
                f"Missing folio for holding: {holding.scheme_name[:50]}"
            )

        # Validate units
        if holding.units < 0 and not holding.is_segregated:
            result.add_warning(
                f"Negative units for non-segregated holding: "
                f"{holding.scheme_name[:30]} ({holding.units})"
            )

        # Validate NAV
        if holding.nav <= 0:
            result.add_error(
                f"Invalid NAV (<=0) for holding: {holding.scheme_name[:30]}"
            )

        # Validate value calculation: units × NAV ≈ current_value
        if holding.units > 0 and holding.nav > 0:
            calculated_value = holding.units * holding.nav
            if holding.current_value > 0:
                diff_ratio = abs(calculated_value - holding.current_value) / holding.current_value
                if diff_ratio > self.value_tolerance:
                    result.add_warning(
                        f"Value mismatch for {holding.scheme_name[:30]}: "
                        f"calculated={calculated_value:.2f}, "
                        f"stated={holding.current_value:.2f}, "
                        f"diff={diff_ratio*100:.2f}%"
                    )

        return result

    def validate_transaction(self, transaction: Transaction) -> ValidationResult:
        """
        Validate a single transaction.

        Args:
            transaction: Transaction data to validate.

        Returns:
            ValidationResult for transaction validation.
        """
        result = ValidationResult()

        # Validate ISIN
        if not transaction.isin:
            result.add_warning(
                f"Missing ISIN for transaction on {transaction.date}"
            )
        elif not ISIN_PATTERN.match(transaction.isin):
            result.add_warning(f"Invalid ISIN format in transaction: {transaction.isin}")

        # Validate folio
        if not transaction.folio:
            result.add_warning(
                f"Missing folio for transaction on {transaction.date}"
            )

        # Validate units sign based on transaction type
        expected_negative_types = {
            TransactionType.REDEMPTION,
            TransactionType.SWITCH_OUT,
            TransactionType.STP_OUT,
            TransactionType.TRANSFER_OUT,
        }
        expected_positive_types = {
            TransactionType.PURCHASE,
            TransactionType.SIP,
            TransactionType.SWITCH_IN,
            TransactionType.STP_IN,
            TransactionType.DIVIDEND_REINVESTMENT,
            TransactionType.BONUS,
            TransactionType.TRANSFER_IN,
        }

        if transaction.transaction_type in expected_negative_types and transaction.units > 0:
            result.add_warning(
                f"Expected negative units for {transaction.transaction_type.value} "
                f"on {transaction.date}, got {transaction.units}"
            )
        elif transaction.transaction_type in expected_positive_types and transaction.units < 0:
            result.add_warning(
                f"Expected positive units for {transaction.transaction_type.value} "
                f"on {transaction.date}, got {transaction.units}"
            )

        # Validate NAV for buy/sell transactions
        buy_sell_types = expected_negative_types | expected_positive_types
        if transaction.transaction_type in buy_sell_types:
            if transaction.nav is not None and transaction.nav <= 0:
                result.add_warning(
                    f"Invalid NAV (<=0) for transaction on {transaction.date}"
                )

        # STT and charges should not have large unit changes
        if transaction.transaction_type in (TransactionType.STT, TransactionType.STAMP_DUTY, TransactionType.CHARGES):
            if abs(transaction.units) > Decimal("1"):
                result.add_warning(
                    f"Unexpected large unit change for {transaction.transaction_type.value}: "
                    f"{transaction.units}"
                )

        return result

    def validate_holdings_transactions_consistency(
        self, holdings: List[Holding], transactions: List[Transaction]
    ) -> ValidationResult:
        """
        Validate consistency between holdings and their transactions.

        This checks that the sum of transaction units approximately
        equals the holding units for each scheme/folio combination.

        Args:
            holdings: List of holdings.
            transactions: List of transactions.

        Returns:
            ValidationResult for consistency validation.
        """
        result = ValidationResult()

        # Group transactions by ISIN and folio
        tx_by_key: Dict[str, List[Transaction]] = {}
        for tx in transactions:
            key = f"{tx.isin}|{tx.folio}"
            if key not in tx_by_key:
                tx_by_key[key] = []
            tx_by_key[key].append(tx)

        # Check each holding
        for holding in holdings:
            key = f"{holding.isin}|{holding.folio}"

            if key not in tx_by_key:
                # No transactions found - might be a partial statement
                result.add_warning(
                    f"No transactions found for holding: {holding.scheme_name[:30]} "
                    f"(folio: {holding.folio})"
                )
                continue

            # Sum transaction units
            tx_list = tx_by_key[key]
            total_units = sum(tx.units for tx in tx_list)

            # The final transaction's balance_units should match holding units
            # Sort by date and compare with last balance
            sorted_txs = sorted(tx_list, key=lambda t: t.date)
            if sorted_txs:
                last_balance = sorted_txs[-1].balance_units
                if last_balance > 0:
                    diff = abs(last_balance - holding.units)
                    if diff > self.units_tolerance:
                        result.add_warning(
                            f"Unit balance mismatch for {holding.scheme_name[:30]}: "
                            f"last_tx_balance={last_balance}, holding_units={holding.units}"
                        )

        return result

    def _check_orphaned_data(self, statement: CASStatement) -> ValidationResult:
        """
        Check for orphaned transactions without corresponding holdings.

        Args:
            statement: Complete CAS statement.

        Returns:
            ValidationResult with orphan warnings.
        """
        result = ValidationResult()

        # Get all ISIN/folio combinations from holdings
        holding_keys: Set[str] = set()
        for h in statement.holdings:
            holding_keys.add(f"{h.isin}|{h.folio}")

        # Check transactions
        orphan_isins: Set[str] = set()
        for tx in statement.transactions:
            key = f"{tx.isin}|{tx.folio}"
            if key not in holding_keys and tx.isin:
                orphan_isins.add(tx.isin)

        for isin in orphan_isins:
            result.add_warning(
                f"Transactions found for ISIN {isin} with no corresponding holding"
            )

        return result


def validate_cas(statement: CASStatement) -> ValidationResult:
    """
    Convenience function to validate a CAS statement.

    Args:
        statement: Parsed CAS statement to validate.

    Returns:
        ValidationResult with errors and warnings.
    """
    validator = CASValidator()
    return validator.validate(statement)


def validate_isin(isin: str) -> bool:
    """
    Validate an ISIN format.

    Args:
        isin: ISIN string to validate.

    Returns:
        True if valid, False otherwise.
    """
    return bool(ISIN_PATTERN.match(isin))


def validate_pan(pan: str) -> bool:
    """
    Validate a PAN format.

    Args:
        pan: PAN string to validate.

    Returns:
        True if valid, False otherwise.
    """
    return bool(PAN_PATTERN.match(pan))


def validate_holding_value(
    holding: Holding, tolerance: Decimal = VALUE_TOLERANCE
) -> bool:
    """
    Validate that holding value matches units × NAV.

    Args:
        holding: Holding to validate.
        tolerance: Acceptable tolerance (as decimal ratio).

    Returns:
        True if value is within tolerance, False otherwise.
    """
    if holding.units <= 0 or holding.nav <= 0 or holding.current_value <= 0:
        return True  # Can't validate without positive values

    calculated = holding.units * holding.nav
    diff_ratio = abs(calculated - holding.current_value) / holding.current_value
    return diff_ratio <= tolerance
