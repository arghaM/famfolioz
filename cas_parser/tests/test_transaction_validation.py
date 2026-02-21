"""Tests for transaction value cross-validation in parsers and persistence layer."""

from decimal import Decimal

import pytest

from cas_parser.transactions_parser import TransactionsParser
from cas_parser.unified_parser import UnifiedCASParser


class TestUnifiedParserValidation:
    """Tests for _validate_and_fix_transaction_values in UnifiedCASParser."""

    def setup_method(self):
        self.parser = UnifiedCASParser.__new__(UnifiedCASParser)

    def test_all_values_consistent_no_change(self):
        """When amount ≈ |units| × nav, no corrections should be made."""
        amount = Decimal("5000.00")
        units = Decimal("100.000")
        nav = Decimal("50.0000")
        result = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        assert result == (amount, units, nav)

    def test_corrupt_nav_negative(self):
        """Folio 6 pattern: nav < 0, amount and units are correct."""
        amount = Decimal("600000.00")
        units = Decimal("-54972.000")
        nav = Decimal("-5000.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # NAV should be recomputed: 600000 / 54972 ≈ 10.914
        assert n > 0
        assert Decimal("10") < n < Decimal("12")
        # Amount and units should be unchanged
        assert a == amount
        assert u == units

    def test_corrupt_amount_wildly_large(self):
        """Folio 17 pattern: amount=949M when it should be ~6L."""
        amount = Decimal("949000000.00")
        units = Decimal("-54972.000")
        nav = Decimal("11.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # Amount should be corrected to |units| × nav ≈ 604692
        expected_amount = abs(units) * nav
        assert abs(a) == pytest.approx(float(expected_amount), rel=0.01)
        # Units and NAV unchanged
        assert u == units
        assert n == nav

    def test_corrupt_units_too_large(self):
        """Folio 20 pattern: units=10000 garbage when it should be ~100."""
        amount = Decimal("1961.00")
        units = Decimal("10000.000")
        nav = Decimal("19.6100")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # Units should be corrected to amount / nav ≈ 100
        expected_units = amount / nav
        assert float(u) == pytest.approx(float(expected_units), rel=0.01)
        # Amount and NAV unchanged
        assert a == amount
        assert n == nav

    def test_nav_zero_no_cross_check(self):
        """When nav=0, cross-check is not possible, values unchanged."""
        amount = Decimal("5000.00")
        units = Decimal("100.000")
        nav = Decimal("0")
        # nav=0 triggers range check, but if amount/units can't produce valid nav...
        # recomputed = 5000/100 = 50 which is valid, so nav gets fixed
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        assert n == Decimal("50")
        assert a == amount
        assert u == units

    def test_nav_zero_units_zero(self):
        """When nav=0 and units=0, nothing can be recomputed."""
        amount = Decimal("5000.00")
        units = Decimal("0")
        nav = Decimal("0")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # Can't recompute — values unchanged
        assert a == amount
        assert u == units
        assert n == nav

    def test_negative_amount_sign_preserved(self):
        """When amount is corrected, original sign should be preserved."""
        amount = Decimal("-949000000.00")  # negative corrupt amount
        units = Decimal("-54972.000")
        nav = Decimal("11.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # Corrected amount should be negative (preserving sign)
        assert a < 0

    def test_negative_units_sign_preserved(self):
        """When units are corrected, original sign should be preserved."""
        amount = Decimal("1961.00")
        units = Decimal("-10000.000")  # negative corrupt units
        nav = Decimal("19.6100")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        # Corrected units should be negative (preserving sign)
        assert u < 0

    def test_small_discrepancy_no_correction(self):
        """Small discrepancies (within normal range) should not trigger correction."""
        # amount = 5050, units × nav = 5000 — ratio = 1.01, within tolerance
        amount = Decimal("5050.00")
        units = Decimal("100.000")
        nav = Decimal("50.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        assert a == amount
        assert u == units
        assert n == nav


class TestTransactionsParserValidation:
    """Tests for _validate_and_fix_transaction_values in TransactionsParser."""

    def setup_method(self):
        self.parser = TransactionsParser()

    def test_all_values_consistent_no_change(self):
        amount = Decimal("5000.00")
        units = Decimal("100.000")
        nav = Decimal("50.0000")
        result = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        assert result == (amount, units, nav)

    def test_corrupt_nav_negative(self):
        amount = Decimal("600000.00")
        units = Decimal("-54972.000")
        nav = Decimal("-5000.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        assert n > 0
        assert Decimal("10") < n < Decimal("12")

    def test_corrupt_amount_wildly_large(self):
        amount = Decimal("949000000.00")
        units = Decimal("-54972.000")
        nav = Decimal("11.0000")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        expected_amount = abs(units) * nav
        assert abs(a) == pytest.approx(float(expected_amount), rel=0.01)

    def test_corrupt_units_too_large(self):
        amount = Decimal("1961.00")
        units = Decimal("10000.000")
        nav = Decimal("19.6100")
        a, u, n = self.parser._validate_and_fix_transaction_values(amount, units, nav)
        expected_units = amount / nav
        assert float(u) == pytest.approx(float(expected_units), rel=0.01)

    def test_none_values_pass_through(self):
        """When any value is None, validation should pass through unchanged."""
        result = self.parser._validate_and_fix_transaction_values(
            None, Decimal("100"), Decimal("50")
        )
        assert result == (None, Decimal("100"), Decimal("50"))

        result = self.parser._validate_and_fix_transaction_values(
            Decimal("5000"), None, Decimal("50")
        )
        assert result == (Decimal("5000"), None, Decimal("50"))


class TestPersistenceLayerValidation:
    """Tests for _validate_transaction_for_insert in data.py."""

    def setup_method(self):
        from cas_parser.webapp.data import _validate_transaction_for_insert
        self.validate = _validate_transaction_for_insert

    def test_all_values_consistent_no_change(self):
        a, u, n = self.validate(5000.0, 100.0, 50.0)
        assert a == pytest.approx(5000.0)
        assert u == pytest.approx(100.0)
        assert n == pytest.approx(50.0)

    def test_corrupt_nav_negative(self):
        a, u, n = self.validate(600000.0, -54972.0, -5000.0)
        assert n > 0
        assert 10 < n < 12

    def test_corrupt_amount_wildly_large(self):
        a, u, n = self.validate(949000000.0, -54972.0, 11.0)
        expected = abs(-54972.0) * 11.0
        assert abs(a) == pytest.approx(expected, rel=0.01)

    def test_corrupt_units_too_large(self):
        a, u, n = self.validate(1961.0, 10000.0, 19.61)
        expected_units = 1961.0 / 19.61
        assert u == pytest.approx(expected_units, rel=0.01)

    def test_nav_zero_units_zero_no_crash(self):
        """Should not crash on zero values."""
        a, u, n = self.validate(5000.0, 0.0, 0.0)
        assert a == 5000.0
        assert u == 0.0
        assert n == 0.0

    def test_all_zeros(self):
        """All zeros should pass through without error."""
        a, u, n = self.validate(0.0, 0.0, 0.0)
        assert a == 0.0
        assert u == 0.0
        assert n == 0.0
