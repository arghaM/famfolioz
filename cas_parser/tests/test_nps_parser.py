"""
Tests for NPS Parser module.
"""

import pytest
from datetime import date
from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nps_models import (
    NPSSubscriber, NPSScheme, NPSTransaction, NPSStatement,
    NPSValidationResult, ContributionType, NPSSchemeType
)
from nps_parser import (
    NPSParser, parse_date, parse_decimal, detect_scheme_type,
    detect_contribution_type, detect_pfm, generate_nps_tx_hash
)


class TestNPSModels:
    """Test NPS data models."""

    def test_subscriber_creation(self):
        """Test NPSSubscriber dataclass."""
        subscriber = NPSSubscriber(
            pran="123456789012",
            name="John Doe",
            pan="ABCDE1234F"
        )
        assert subscriber.pran == "123456789012"
        assert subscriber.name == "John Doe"
        assert subscriber.pan == "ABCDE1234F"

    def test_subscriber_normalization(self):
        """Test subscriber data normalization."""
        subscriber = NPSSubscriber(
            pran="  1234 5678 9012  ",
            name="  John  Doe  ",
            pan="  abcde1234f  ",
            email="  TEST@EMAIL.COM  "
        )
        assert subscriber.pran == "123456789012"
        assert subscriber.name == "John  Doe"
        assert subscriber.pan == "ABCDE1234F"
        assert subscriber.email == "test@email.com"

    def test_scheme_creation(self):
        """Test NPSScheme dataclass."""
        scheme = NPSScheme(
            scheme_name="SBI Pension Fund Scheme E",
            pfm_name="SBI",
            scheme_type=NPSSchemeType.SCHEME_E,
            units=Decimal("1000.5432"),
            nav=Decimal("45.6789"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("45710.12"),
            tier="I"
        )
        assert scheme.scheme_name == "SBI Pension Fund Scheme E"
        assert scheme.pfm_name == "SBI"
        assert scheme.scheme_type == NPSSchemeType.SCHEME_E
        assert scheme.units == Decimal("1000.5432")
        assert scheme.tier == "I"

    def test_transaction_creation(self):
        """Test NPSTransaction dataclass."""
        tx = NPSTransaction(
            date=date(2024, 1, 15),
            contribution_type=ContributionType.EMPLOYEE,
            scheme_type=NPSSchemeType.SCHEME_E,
            pfm_name="SBI",
            amount=Decimal("5000.00"),
            units=Decimal("109.5432"),
            nav=Decimal("45.6789"),
            description="Employee Contribution",
            tier="I"
        )
        assert tx.contribution_type == ContributionType.EMPLOYEE
        assert tx.amount == Decimal("5000.00")

    def test_validation_result(self):
        """Test NPSValidationResult."""
        validation = NPSValidationResult()
        assert validation.is_valid is True
        assert len(validation.errors) == 0

        validation.add_warning("Test warning")
        assert validation.is_valid is True
        assert len(validation.warnings) == 1

        validation.add_error("Test error")
        assert validation.is_valid is False
        assert len(validation.errors) == 1


class TestParserHelpers:
    """Test parser helper functions."""

    def test_parse_date_dmy(self):
        """Test date parsing with DD-MM-YYYY format."""
        assert parse_date("15-01-2024") == date(2024, 1, 15)
        assert parse_date("01/12/2023") == date(2023, 12, 1)

    def test_parse_date_text(self):
        """Test date parsing with text month."""
        assert parse_date("15-Jan-2024") == date(2024, 1, 15)
        assert parse_date("01 Dec 2023") == date(2023, 12, 1)

    def test_parse_date_invalid(self):
        """Test date parsing with invalid input."""
        assert parse_date("invalid") is None
        assert parse_date("") is None
        assert parse_date(None) is None

    def test_parse_decimal(self):
        """Test decimal parsing."""
        assert parse_decimal("1000.50") == Decimal("1000.50")
        assert parse_decimal("1,00,000.50") == Decimal("100000.50")
        assert parse_decimal("Rs. 5000") == Decimal("5000")
        assert parse_decimal("INR 10,000.00") == Decimal("10000.00")

    def test_parse_decimal_invalid(self):
        """Test decimal parsing with invalid input."""
        assert parse_decimal("") is None
        assert parse_decimal(None) is None

    def test_detect_scheme_type(self):
        """Test scheme type detection."""
        assert detect_scheme_type("Scheme E - Equity") == NPSSchemeType.SCHEME_E
        assert detect_scheme_type("SCHEME-C Corporate Bonds") == NPSSchemeType.SCHEME_C
        assert detect_scheme_type("Government Securities (G)") == NPSSchemeType.SCHEME_G
        assert detect_scheme_type("Alternate Assets") == NPSSchemeType.SCHEME_A
        assert detect_scheme_type("Unknown scheme") == NPSSchemeType.UNKNOWN

    def test_detect_contribution_type(self):
        """Test contribution type detection."""
        assert detect_contribution_type("Employee Contribution") == ContributionType.EMPLOYEE
        assert detect_contribution_type("EMPLOYER CONT") == ContributionType.EMPLOYER
        assert detect_contribution_type("Voluntary contribution") == ContributionType.VOLUNTARY
        assert detect_contribution_type("Tier II") == ContributionType.TIER_II
        assert detect_contribution_type("Unknown") == ContributionType.UNKNOWN

    def test_detect_pfm(self):
        """Test PFM detection."""
        assert detect_pfm("SBI Pension Fund") == "SBI"
        assert detect_pfm("LIC Pension Fund Limited") == "LIC"
        assert detect_pfm("HDFC Pension Management") == "HDFC"
        assert detect_pfm("ICICI Prudential") == "ICICI"
        assert detect_pfm("Unknown Fund") == ""

    def test_generate_tx_hash(self):
        """Test transaction hash generation."""
        hash1 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        hash2 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        hash3 = generate_nps_tx_hash("123456789012", "2024-01-16", "E", 5000.00, 109.5432)

        assert hash1 == hash2  # Same inputs, same hash
        assert hash1 != hash3  # Different date, different hash
        assert len(hash1) == 32  # MD5 hash length


class TestNPSParser:
    """Test NPS parser."""

    def test_parser_initialization(self):
        """Test parser initialization."""
        parser = NPSParser()
        assert parser.password is None

        parser_with_pwd = NPSParser(password="test123")
        assert parser_with_pwd.password == "test123"

    def test_parse_from_text_basic(self):
        """Test parsing from raw text."""
        sample_text = """
        NPS Statement
        PRAN: 123456789012
        Subscriber Name: JOHN DOE
        PAN: ABCDE1234F

        Statement Period: 01-04-2023 to 31-03-2024

        Portfolio Summary
        Scheme E - Equity: Units 500.1234, NAV 45.67, Value 22840.54
        Scheme C - Corporate Bonds: Units 300.5678, NAV 32.10, Value 9648.23
        Scheme G - Government Securities: Units 200.9012, NAV 28.50, Value 5725.68

        Transaction History
        15-01-2024 Employee Contribution Scheme E 5000.00 109.5432 45.67
        15-01-2024 Employer Contribution Scheme E 5000.00 109.5432 45.67
        """

        parser = NPSParser()
        statement = parser.parse_from_text(sample_text)

        assert statement.subscriber.pran == "123456789012"
        assert statement.subscriber.name == "JOHN DOE"
        assert statement.subscriber.pan == "ABCDE1234F"

    def test_statement_to_dict(self):
        """Test statement serialization to dict."""
        subscriber = NPSSubscriber(
            pran="123456789012",
            name="John Doe"
        )
        statement = NPSStatement(subscriber=subscriber)
        statement.schemes.append(NPSScheme(
            scheme_name="Test Scheme",
            pfm_name="SBI",
            scheme_type=NPSSchemeType.SCHEME_E,
            units=Decimal("100"),
            nav=Decimal("50"),
            nav_date=date(2024, 1, 15),
            current_value=Decimal("5000"),
            tier="I"
        ))

        result = statement.to_dict()

        assert result['subscriber']['pran'] == "123456789012"
        assert result['subscriber']['name'] == "John Doe"
        assert len(result['schemes']) == 1
        assert result['schemes'][0]['scheme_type'] == "E"


class TestIdempotency:
    """Test idempotency behavior."""

    def test_same_hash_for_same_transaction(self):
        """Verify same transaction generates same hash."""
        hash1 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        hash2 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        assert hash1 == hash2

    def test_different_hash_for_different_amount(self):
        """Verify different amounts generate different hashes."""
        hash1 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        hash2 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5001.00, 109.5432)
        assert hash1 != hash2

    def test_different_hash_for_different_scheme(self):
        """Verify different schemes generate different hashes."""
        hash1 = generate_nps_tx_hash("123456789012", "2024-01-15", "E", 5000.00, 109.5432)
        hash2 = generate_nps_tx_hash("123456789012", "2024-01-15", "C", 5000.00, 109.5432)
        assert hash1 != hash2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
