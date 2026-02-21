"""Tests for CAS validator."""

from datetime import date
from decimal import Decimal

import pytest

from cas_parser.models import (
    CASStatement,
    Holding,
    Investor,
    Transaction,
    TransactionType,
    ValidationResult,
)
from cas_parser.validator import (
    CASValidator,
    validate_cas,
    validate_holding_value,
    validate_isin,
    validate_pan,
)


class TestValidateISIN:
    """Tests for ISIN validation."""

    def test_valid_isin(self):
        """Test valid ISIN formats."""
        assert validate_isin("INF179K01234") is True
        assert validate_isin("INF090I01BC5") is True
        assert validate_isin("INF200K01RJ1") is True

    def test_invalid_isin(self):
        """Test invalid ISIN formats."""
        assert validate_isin("") is False
        assert validate_isin("INF179") is False  # Too short
        assert validate_isin("INF179K012345") is False  # Too long
        assert validate_isin("XYZ179K01234") is False  # Wrong prefix
        assert validate_isin("inf179k01234") is False  # Lowercase


class TestValidatePAN:
    """Tests for PAN validation."""

    def test_valid_pan(self):
        """Test valid PAN formats."""
        assert validate_pan("ABCDE1234F") is True
        assert validate_pan("ZZZZZ9999Z") is True

    def test_invalid_pan(self):
        """Test invalid PAN formats."""
        assert validate_pan("") is False
        assert validate_pan("ABCDE123F") is False  # Too short
        assert validate_pan("ABCDE12345F") is False  # Too long
        assert validate_pan("12345ABCDE") is False  # Wrong format
        assert validate_pan("abcde1234f") is False  # Lowercase


class TestValidateHoldingValue:
    """Tests for holding value validation."""

    def test_valid_holding_value(self):
        """Test holding where value matches units × NAV."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000.00"),
        )

        assert validate_holding_value(holding) is True

    def test_holding_value_within_tolerance(self):
        """Test holding where value is within tolerance."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5005.00"),  # 0.1% difference
        )

        assert validate_holding_value(holding, tolerance=Decimal("0.01")) is True

    def test_holding_value_outside_tolerance(self):
        """Test holding where value exceeds tolerance."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("6000.00"),  # 20% difference
        )

        assert validate_holding_value(holding, tolerance=Decimal("0.01")) is False


class TestCASValidator:
    """Tests for CASValidator class."""

    def test_validate_valid_investor(self):
        """Test validating a valid investor."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")

        validator = CASValidator()
        result = validator.validate_investor(investor)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_investor_missing_pan(self):
        """Test validating investor with missing PAN."""
        investor = Investor(name="John Doe", pan="")

        validator = CASValidator()
        result = validator.validate_investor(investor)

        assert result.is_valid is False
        assert any("PAN" in e for e in result.errors)

    def test_validate_investor_invalid_pan(self):
        """Test validating investor with invalid PAN."""
        investor = Investor(name="John Doe", pan="INVALID")

        validator = CASValidator()
        result = validator.validate_investor(investor)

        assert result.is_valid is False
        assert any("Invalid PAN" in e for e in result.errors)

    def test_validate_valid_holding(self):
        """Test validating a valid holding."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000.00"),
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        assert result.is_valid is True

    def test_validate_holding_missing_isin(self):
        """Test validating holding with missing ISIN."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="",
            folio="12345",
            units=Decimal("100"),
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000"),
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        assert result.is_valid is False
        assert any("ISIN" in e for e in result.errors)

    def test_validate_holding_invalid_isin(self):
        """Test validating holding with invalid ISIN."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INVALID123",
            folio="12345",
            units=Decimal("100"),
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000"),
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        assert result.is_valid is False
        assert any("Invalid ISIN" in e for e in result.errors)

    def test_validate_holding_value_mismatch(self):
        """Test validating holding with value mismatch."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("10000.00"),  # Should be 5000
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        # Should have a warning about value mismatch
        assert len(result.warnings) > 0 or not result.is_valid

    def test_validate_transaction_units_sign(self):
        """Test validating transaction units sign."""
        # Redemption with positive units (wrong)
        tx = Transaction(
            date=date(2024, 1, 15),
            description="Redemption",
            transaction_type=TransactionType.REDEMPTION,
            units=Decimal("100"),  # Should be negative
            balance_units=Decimal("0"),
            folio="12345",
            scheme_name="Test Fund",
            isin="INF179K01234",
        )

        validator = CASValidator()
        result = validator.validate_transaction(tx)

        assert len(result.warnings) > 0

    def test_validate_full_statement(self):
        """Test validating a complete CAS statement."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.000"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000.00"),
        )
        transaction = Transaction(
            date=date(2024, 1, 15),
            description="Purchase",
            transaction_type=TransactionType.PURCHASE,
            units=Decimal("100.000"),
            balance_units=Decimal("100.000"),
            folio="12345",
            scheme_name="Test Fund",
            isin="INF179K01234",
        )

        statement = CASStatement(
            investor=investor,
            holdings=[holding],
            transactions=[transaction],
        )

        result = validate_cas(statement)

        assert result.is_valid is True

    def test_validate_statement_with_orphaned_transactions(self):
        """Test detecting transactions without corresponding holdings."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")
        holding = Holding(
            scheme_name="Fund A",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100"),
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000"),
        )
        transaction = Transaction(
            date=date(2024, 1, 15),
            description="Purchase",
            transaction_type=TransactionType.PURCHASE,
            units=Decimal("100"),
            balance_units=Decimal("100"),
            folio="99999",  # Different folio
            scheme_name="Fund B",
            isin="INF179K09999",  # Different ISIN
        )

        statement = CASStatement(
            investor=investor,
            holdings=[holding],
            transactions=[transaction],
        )

        validator = CASValidator()
        result = validator.validate(statement)

        # Should have warning about orphaned transaction
        assert len(result.warnings) > 0


class TestEdgeCases:
    """Test edge cases in validation."""

    def test_validate_negative_units_non_segregated(self):
        """Test validating negative units for non-segregated holding."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("-100"),  # Negative
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("-5000"),
            is_segregated=False,
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        # Should have warning about negative units
        assert len(result.warnings) > 0

    def test_validate_stt_with_large_units(self):
        """Test validating STT transaction with unexpectedly large units."""
        tx = Transaction(
            date=date(2024, 1, 15),
            description="STT",
            transaction_type=TransactionType.STT,
            units=Decimal("100"),  # STT shouldn't have large unit changes
            balance_units=Decimal("1000"),
            folio="12345",
            scheme_name="Test Fund",
            isin="INF179K01234",
        )

        validator = CASValidator()
        result = validator.validate_transaction(tx)

        # Should have warning about large units for STT
        assert len(result.warnings) > 0

    def test_validate_zero_nav(self):
        """Test validating holding with zero NAV."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100"),
            nav=Decimal("0"),  # Zero NAV
            nav_date=date(2024, 1, 15),
            current_value=Decimal("0"),
        )

        validator = CASValidator()
        result = validator.validate_holding(holding)

        # Should have error about invalid NAV
        assert not result.is_valid or len(result.errors) > 0
