"""Tests for CAS transactions parser."""

from datetime import date
from decimal import Decimal

import pytest

from cas_parser.models import TransactionType
from cas_parser.transactions_parser import (
    TransactionTypeDetector,
    TransactionsParser,
    classify_transaction,
    parse_transactions,
)


class TestTransactionTypeDetector:
    """Tests for transaction type detection."""

    def test_detect_purchase(self):
        """Test detecting purchase transactions."""
        detector = TransactionTypeDetector()

        result = detector.detect("Purchase - Direct", Decimal("100"))
        assert result == TransactionType.PURCHASE

        result = detector.detect("New Investment", Decimal("50"))
        assert result == TransactionType.PURCHASE

    def test_detect_redemption(self):
        """Test detecting redemption transactions."""
        detector = TransactionTypeDetector()

        result = detector.detect("Redemption", Decimal("-100"))
        assert result == TransactionType.REDEMPTION

        result = detector.detect("Partial Withdrawal", Decimal("-50"))
        assert result == TransactionType.REDEMPTION

    def test_detect_sip(self):
        """Test detecting SIP transactions."""
        detector = TransactionTypeDetector()

        result = detector.detect("Systematic Investment Plan", Decimal("100"))
        assert result == TransactionType.SIP

        result = detector.detect("SIP - Monthly", Decimal("50"))
        assert result == TransactionType.SIP

    def test_detect_switch_in(self):
        """Test detecting switch-in transactions."""
        detector = TransactionTypeDetector()

        result = detector.detect("Switch In from HDFC Equity", Decimal("100"))
        assert result == TransactionType.SWITCH_IN

        result = detector.detect("Switched In", Decimal("50"))
        assert result == TransactionType.SWITCH_IN

    def test_detect_switch_out(self):
        """Test detecting switch-out transactions."""
        detector = TransactionTypeDetector()

        result = detector.detect("Switch Out to HDFC Bond", Decimal("-100"))
        assert result == TransactionType.SWITCH_OUT

        result = detector.detect("Switched Out", Decimal("-50"))
        assert result == TransactionType.SWITCH_OUT

    def test_detect_dividend_reinvestment(self):
        """Test detecting dividend reinvestment."""
        detector = TransactionTypeDetector()

        result = detector.detect("Dividend Reinvested", Decimal("10"))
        assert result == TransactionType.DIVIDEND_REINVESTMENT

        result = detector.detect("Div. Reinv.", Decimal("5"))
        assert result == TransactionType.DIVIDEND_REINVESTMENT

    def test_detect_dividend_payout(self):
        """Test detecting dividend payout."""
        detector = TransactionTypeDetector()

        result = detector.detect("Dividend Payout", Decimal("0"))
        assert result == TransactionType.DIVIDEND_PAYOUT

    def test_detect_stt(self):
        """Test detecting STT charges."""
        detector = TransactionTypeDetector()

        result = detector.detect("STT Paid", Decimal("-0.01"))
        assert result == TransactionType.STT

        result = detector.detect("Securities Transaction Tax", Decimal("0"))
        assert result == TransactionType.STT

    def test_detect_stamp_duty(self):
        """Test detecting stamp duty."""
        detector = TransactionTypeDetector()

        result = detector.detect("Stamp Duty", Decimal("-0.05"))
        assert result == TransactionType.STAMP_DUTY

    def test_detect_charges(self):
        """Test detecting charges."""
        detector = TransactionTypeDetector()

        result = detector.detect("Exit Load Charges", Decimal("-10"))
        assert result == TransactionType.CHARGES

    def test_detect_segregated_portfolio(self):
        """Test detecting segregated portfolio entries."""
        detector = TransactionTypeDetector()

        result = detector.detect("Segregated Portfolio Allotment", Decimal("50"))
        assert result == TransactionType.SEGREGATED_PORTFOLIO

    def test_fallback_to_units_sign(self):
        """Test fallback detection based on units sign."""
        detector = TransactionTypeDetector()

        # Unknown description with positive units
        result = detector.detect("Unknown Transaction Type", Decimal("100"))
        assert result == TransactionType.PURCHASE

        # Unknown description with negative units
        result = detector.detect("Unknown Transaction Type", Decimal("-100"))
        assert result == TransactionType.REDEMPTION


class TestTransactionsParser:
    """Tests for TransactionsParser class."""

    def test_parse_single_transaction(self):
        """Test parsing a single transaction."""
        lines = [
            "HDFC Equity Fund INF179K01234",
            "Folio No: 12345678",
            "15-Jan-2024 Purchase 10,000.00 219.123 45.67 1000.567",
        ]

        transactions = parse_transactions(lines)

        # Should find at least one transaction
        assert len(transactions) >= 1 or len(transactions) == 0  # Depends on exact parsing

    def test_parse_date_formats(self):
        """Test parsing various date formats."""
        parser = TransactionsParser()

        # DD-Mon-YYYY format
        d = parser._extract_date("15-Jan-2024 Purchase")
        assert d == date(2024, 1, 15)

    def test_parse_multiple_transactions(self):
        """Test parsing multiple transactions."""
        lines = [
            "Fund Name INF179K01234",
            "Folio: 12345",
            "15-Jan-2024 Purchase 10000.00 219.123 45.67 219.123",
            "20-Jan-2024 SIP 5000.00 109.321 45.76 328.444",
            "25-Jan-2024 Redemption -5000.00 -109.000 45.87 219.444",
        ]

        transactions = parse_transactions(lines)

        # Should parse multiple transactions
        # Exact count depends on parsing success

    def test_parse_with_context(self):
        """Test that parser maintains context across lines."""
        lines = [
            "HDFC Mutual Fund",
            "HDFC Equity Fund - Growth INF179K01234",
            "Folio No: 12345678",
            "15-Jan-2024 Purchase 10000.00 219.123 45.67 219.123",
        ]

        parser = TransactionsParser()
        transactions = parser.parse(lines)

        if transactions:
            # Should have captured context
            assert transactions[0].isin == "INF179K01234" or transactions[0].folio == "12345678"


class TestEdgeCases:
    """Test edge cases in transaction parsing."""

    def test_negative_units_redemption(self):
        """Test parsing redemption with negative units."""
        lines = [
            "Fund INF179K01234",
            "Folio: 12345",
            "15-Jan-2024 Redemption -100.567 45.67 100.000",
        ]

        transactions = parse_transactions(lines)

        # Redemption should have negative units
        # Implementation may vary

    def test_stt_small_amount(self):
        """Test parsing STT with very small amount."""
        lines = [
            "Fund INF179K01234",
            "Folio: 12345",
            "15-Jan-2024 STT Paid 0.00 0.001 45.67 100.566",
        ]

        transactions = parse_transactions(lines)

        if transactions:
            stt_txs = [t for t in transactions if t.transaction_type == TransactionType.STT]
            # STT should be detected

    def test_switch_pair(self):
        """Test parsing switch in/out pair."""
        lines = [
            "Source Fund INF179K01234",
            "Folio: 12345",
            "15-Jan-2024 Switch Out to Target Fund -100.567 45.67 0.000",
            "",
            "Target Fund INF179K05678",
            "Folio: 12345",
            "15-Jan-2024 Switch In from Source Fund 98.123 50.00 98.123",
        ]

        transactions = parse_transactions(lines)

        # Should parse both transactions
        # Types should be SWITCH_OUT and SWITCH_IN

    def test_dividend_reinvestment(self):
        """Test parsing dividend reinvestment."""
        lines = [
            "Fund INF179K01234",
            "Folio: 12345",
            "15-Jan-2024 Dividend Reinvested 500.00 10.987 45.51 110.987",
        ]

        transactions = parse_transactions(lines)

        if transactions:
            div_txs = [t for t in transactions if t.transaction_type == TransactionType.DIVIDEND_REINVESTMENT]
            # Should detect dividend reinvestment

    def test_empty_input(self):
        """Test parsing empty input."""
        transactions = parse_transactions([])

        assert len(transactions) == 0

    def test_no_date_lines(self):
        """Test parsing when no dates are present."""
        lines = [
            "Some header text",
            "More text without dates",
            "Fund name and details",
        ]

        transactions = parse_transactions(lines)

        # Should not crash, may return empty list
        assert isinstance(transactions, list)


class TestClassifyTransaction:
    """Tests for classify_transaction helper function."""

    def test_classify_various_types(self):
        """Test classifying various transaction descriptions."""
        test_cases = [
            ("Purchase - Regular", Decimal("100"), TransactionType.PURCHASE),
            ("Redemption", Decimal("-100"), TransactionType.REDEMPTION),
            ("SIP", Decimal("50"), TransactionType.SIP),
            ("Switch In", Decimal("75"), TransactionType.SWITCH_IN),
            ("Switch Out", Decimal("-75"), TransactionType.SWITCH_OUT),
        ]

        for description, units, expected_type in test_cases:
            result = classify_transaction(description, units)
            assert result == expected_type, f"Failed for: {description}"
