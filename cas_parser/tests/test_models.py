"""Tests for CAS Parser data models."""

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


class TestInvestor:
    """Tests for Investor dataclass."""

    def test_create_investor_minimal(self):
        """Test creating investor with minimal required fields."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")

        assert investor.name == "John Doe"
        assert investor.pan == "ABCDE1234F"
        assert investor.email is None
        assert investor.mobile is None

    def test_create_investor_full(self):
        """Test creating investor with all fields."""
        investor = Investor(
            name="John Doe",
            pan="ABCDE1234F",
            email="john@example.com",
            mobile="9876543210",
            address="123 Main St",
            dp_id="IN301234",
            client_id="12345678",
        )

        assert investor.name == "John Doe"
        assert investor.pan == "ABCDE1234F"
        assert investor.email == "john@example.com"
        assert investor.mobile == "9876543210"
        assert investor.dp_id == "IN301234"

    def test_investor_normalization(self):
        """Test that investor data is normalized."""
        investor = Investor(
            name="  John Doe  ",
            pan="  abcde1234f  ",
            email="  JOHN@EXAMPLE.COM  ",
        )

        assert investor.name == "John Doe"
        assert investor.pan == "ABCDE1234F"
        assert investor.email == "john@example.com"


class TestHolding:
    """Tests for Holding dataclass."""

    def test_create_holding(self):
        """Test creating a holding with all fields."""
        holding = Holding(
            scheme_name="HDFC Equity Fund - Growth",
            isin="INF179K01234",
            folio="12345678/90",
            units=Decimal("1000.5678"),
            nav=Decimal("45.67"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("45692.45"),
            registrar="CAMS",
            amc="HDFC Mutual Fund",
        )

        assert holding.scheme_name == "HDFC Equity Fund - Growth"
        assert holding.isin == "INF179K01234"
        assert holding.units == Decimal("1000.5678")
        assert holding.nav == Decimal("45.67")

    def test_holding_decimal_conversion(self):
        """Test that numeric values are converted to Decimal."""
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=100.5,  # float
            nav=45.67,  # float
            nav_date=date(2024, 1, 15),
            current_value=4590.12,  # float
        )

        assert isinstance(holding.units, Decimal)
        assert isinstance(holding.nav, Decimal)
        assert isinstance(holding.current_value, Decimal)

    def test_holding_normalization(self):
        """Test that holding data is normalized."""
        holding = Holding(
            scheme_name="  HDFC   Equity  Fund  ",
            isin="  inf179k01234  ",
            folio="  12345  ",
            units=Decimal("100"),
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000"),
        )

        assert holding.scheme_name == "HDFC Equity Fund"
        assert holding.isin == "INF179K01234"
        assert holding.folio == "12345"


class TestTransaction:
    """Tests for Transaction dataclass."""

    def test_create_transaction(self):
        """Test creating a transaction."""
        tx = Transaction(
            date=date(2024, 1, 15),
            description="Purchase",
            transaction_type=TransactionType.PURCHASE,
            units=Decimal("100.5678"),
            balance_units=Decimal("1000.5678"),
            folio="12345678",
            scheme_name="HDFC Equity Fund",
            isin="INF179K01234",
            amount=Decimal("10000.00"),
            nav=Decimal("45.67"),
        )

        assert tx.date == date(2024, 1, 15)
        assert tx.transaction_type == TransactionType.PURCHASE
        assert tx.units == Decimal("100.5678")

    def test_transaction_types(self):
        """Test all transaction type values."""
        assert TransactionType.PURCHASE.value == "purchase"
        assert TransactionType.REDEMPTION.value == "redemption"
        assert TransactionType.SIP.value == "sip"
        assert TransactionType.SWITCH_IN.value == "switch_in"
        assert TransactionType.SWITCH_OUT.value == "switch_out"
        assert TransactionType.DIVIDEND_PAYOUT.value == "dividend_payout"
        assert TransactionType.DIVIDEND_REINVESTMENT.value == "dividend_reinvestment"
        assert TransactionType.STT.value == "stt"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_empty_result_is_valid(self):
        """Test that empty result defaults to valid."""
        result = ValidationResult()

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_add_error(self):
        """Test adding an error marks result invalid."""
        result = ValidationResult()
        result.add_error("Test error")

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert "Test error" in result.errors

    def test_add_warning(self):
        """Test adding a warning doesn't affect validity."""
        result = ValidationResult()
        result.add_warning("Test warning")

        assert result.is_valid is True
        assert len(result.warnings) == 1

    def test_merge_results(self):
        """Test merging two validation results."""
        result1 = ValidationResult()
        result1.add_error("Error 1")

        result2 = ValidationResult()
        result2.add_warning("Warning 1")

        result1.merge(result2)

        assert result1.is_valid is False
        assert len(result1.errors) == 1
        assert len(result1.warnings) == 1


class TestCASStatement:
    """Tests for CASStatement dataclass."""

    def test_create_statement(self):
        """Test creating a CAS statement."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")
        statement = CASStatement(investor=investor)

        assert statement.investor.name == "John Doe"
        assert len(statement.holdings) == 0
        assert len(statement.transactions) == 0

    def test_to_dict(self):
        """Test converting statement to dictionary."""
        investor = Investor(name="John Doe", pan="ABCDE1234F")
        holding = Holding(
            scheme_name="Test Fund",
            isin="INF179K01234",
            folio="12345",
            units=Decimal("100.567"),
            nav=Decimal("50.00"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5028.35"),
        )
        statement = CASStatement(
            investor=investor,
            holdings=[holding],
            statement_date=date(2024, 1, 31),
        )

        result = statement.to_dict()

        assert result["investor"]["name"] == "John Doe"
        assert result["investor"]["pan"] == "ABCDE1234F"
        assert len(result["holdings"]) == 1
        assert result["holdings"][0]["units"] == "100.567"
        assert result["statement_date"] == "2024-01-31"

    def test_get_holdings_for_folio(self):
        """Test filtering holdings by folio."""
        investor = Investor(name="John", pan="ABCDE1234F")
        holdings = [
            Holding(
                scheme_name="Fund A",
                isin="INF179K01234",
                folio="12345",
                units=Decimal("100"),
                nav=Decimal("50"),
                nav_date=date(2024, 1, 1),
                current_value=Decimal("5000"),
            ),
            Holding(
                scheme_name="Fund B",
                isin="INF179K05678",
                folio="12345",
                units=Decimal("200"),
                nav=Decimal("25"),
                nav_date=date(2024, 1, 1),
                current_value=Decimal("5000"),
            ),
            Holding(
                scheme_name="Fund C",
                isin="INF179K09999",
                folio="99999",
                units=Decimal("50"),
                nav=Decimal("100"),
                nav_date=date(2024, 1, 1),
                current_value=Decimal("5000"),
            ),
        ]
        statement = CASStatement(investor=investor, holdings=holdings)

        folio_holdings = statement.get_holdings_for_folio("12345")

        assert len(folio_holdings) == 2
        assert all(h.folio == "12345" for h in folio_holdings)
