"""Tests for XIRR calculation engine."""

from datetime import date

import pytest

from cas_parser.webapp.xirr import _validate_amount, build_cashflows_for_folio, xirr


class TestXirr:
    """Tests for the xirr() function."""

    def test_simple_positive_return(self):
        """Invest 10000, get back 11000 after 1 year => ~10% return."""
        cashflows = [
            (date(2023, 1, 1), -10000),
            (date(2024, 1, 1), 11000),
        ]
        result = xirr(cashflows)
        assert result is not None
        assert abs(result - 0.10) < 0.01

    def test_simple_negative_return(self):
        """Invest 10000, get back 9000 after 1 year => ~-10% return."""
        cashflows = [
            (date(2023, 1, 1), -10000),
            (date(2024, 1, 1), 9000),
        ]
        result = xirr(cashflows)
        assert result is not None
        assert abs(result - (-0.10)) < 0.01

    def test_sip_pattern(self):
        """Monthly SIP of 1000 for 12 months, terminal value 13000."""
        cashflows = []
        for month in range(1, 13):
            cashflows.append((date(2023, month, 1), -1000))
        cashflows.append((date(2024, 1, 1), 13000))
        result = xirr(cashflows)
        assert result is not None
        # SIP of 12000 growing to 13000 in ~1 year should give positive return
        assert result > 0

    def test_doubled_in_two_years(self):
        """Invest 10000, doubles in 2 years => ~41.4% annualized."""
        cashflows = [
            (date(2022, 1, 1), -10000),
            (date(2024, 1, 1), 20000),
        ]
        result = xirr(cashflows)
        assert result is not None
        assert abs(result - 0.414) < 0.02

    def test_empty_cashflows(self):
        """Empty list returns None."""
        assert xirr([]) is None

    def test_single_cashflow(self):
        """Single cashflow returns None."""
        assert xirr([(date(2023, 1, 1), -10000)]) is None

    def test_same_sign_cashflows(self):
        """All negative or all positive returns None."""
        assert xirr([(date(2023, 1, 1), -1000), (date(2024, 1, 1), -2000)]) is None
        assert xirr([(date(2023, 1, 1), 1000), (date(2024, 1, 1), 2000)]) is None

    def test_zero_terminal_value(self):
        """Near-total loss: XIRR is None (NPV never crosses zero) or very negative."""
        cashflows = [
            (date(2023, 1, 1), -10000),
            (date(2024, 1, 1), 0.01),  # Near-zero terminal
        ]
        result = xirr(cashflows)
        # With near-zero terminal value, NPV ~ -10000 for all rates, no solution
        assert result is None or result < -0.9

    def test_significant_loss(self):
        """50% loss in 1 year => ~-50% return."""
        cashflows = [
            (date(2023, 1, 1), -10000),
            (date(2024, 1, 1), 5000),
        ]
        result = xirr(cashflows)
        assert result is not None
        assert abs(result - (-0.50)) < 0.01

    def test_high_return(self):
        """Very high return should still converge."""
        cashflows = [
            (date(2023, 1, 1), -1000),
            (date(2024, 1, 1), 5000),
        ]
        result = xirr(cashflows)
        assert result is not None
        assert abs(result - 4.0) < 0.1  # ~400% return

    def test_multiple_investments_and_redemptions(self):
        """Mix of investments and partial redemptions."""
        cashflows = [
            (date(2023, 1, 1), -10000),
            (date(2023, 6, 1), -5000),
            (date(2023, 9, 1), 3000),
            (date(2024, 1, 1), 14000),
        ]
        result = xirr(cashflows)
        assert result is not None
        # Net invested ~12000, got back 17000
        assert result > 0


class TestBuildCashflows:
    """Tests for build_cashflows_for_folio()."""

    def test_purchase_becomes_outflow(self):
        """Purchase with positive DB amount becomes negative cashflow."""
        txns = [{'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000}]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2
        assert cfs[0] == (date(2023, 1, 1), -10000)
        assert cfs[1] == (date(2024, 1, 1), 11000)

    def test_sip_becomes_outflow(self):
        """SIP with positive DB amount becomes negative cashflow."""
        txns = [{'tx_date': '2023-01-01', 'tx_type': 'sip', 'amount': 5000}]
        cfs = build_cashflows_for_folio(txns, 5500, as_of_date=date(2024, 1, 1))
        assert cfs[0][1] == -5000

    def test_switch_in_becomes_outflow(self):
        """Switch-in treated as investment outflow."""
        txns = [{'tx_date': '2023-01-01', 'tx_type': 'switch_in', 'amount': 5000}]
        cfs = build_cashflows_for_folio(txns, 5500, as_of_date=date(2024, 1, 1))
        assert cfs[0][1] == -5000

    def test_redemption_becomes_inflow(self):
        """Redemption with negative DB amount becomes positive cashflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'redemption', 'amount': -3000},
        ]
        cfs = build_cashflows_for_folio(txns, 8000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 3
        assert cfs[0] == (date(2023, 1, 1), -10000)
        assert cfs[1] == (date(2023, 6, 1), 3000)  # positive inflow
        assert cfs[2] == (date(2024, 1, 1), 8000)

    def test_switch_out_becomes_inflow(self):
        """Switch-out treated as redemption inflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'switch_out', 'amount': -5000},
        ]
        cfs = build_cashflows_for_folio(txns, 0, as_of_date=date(2024, 1, 1))
        # No terminal value since current_value=0
        assert len(cfs) == 2
        assert cfs[1][1] == 5000  # positive inflow

    def test_dividend_payout_becomes_inflow(self):
        """Dividend payout is an inflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'dividend_payout', 'amount': 500},
        ]
        cfs = build_cashflows_for_folio(txns, 10000, as_of_date=date(2024, 1, 1))
        assert cfs[1] == (date(2023, 6, 1), 500)

    def test_dividend_reinvestment_skipped(self):
        """Dividend reinvestment is not an external cashflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'dividend_reinvestment', 'amount': 500},
        ]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2  # Only purchase + terminal

    def test_stamp_duty_skipped(self):
        """Stamp duty is not an external cashflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-01-01', 'tx_type': 'stamp_duty', 'amount': -1.5},
        ]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2

    def test_stt_skipped(self):
        """STT is not an external cashflow."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-01-01', 'tx_type': 'stt', 'amount': -10},
        ]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2

    def test_zero_amount_skipped(self):
        """Transactions with zero amount are skipped."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'purchase', 'amount': 0},
        ]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2

    def test_none_amount_skipped(self):
        """Transactions with None amount are skipped."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'purchase', 'amount': None},
        ]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2

    def test_no_terminal_value_when_zero(self):
        """No terminal cashflow when current_value is 0."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'redemption', 'amount': -10500},
        ]
        cfs = build_cashflows_for_folio(txns, 0, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2  # No terminal value appended

    def test_date_string_parsing(self):
        """Handles YYYY-MM-DD string dates."""
        txns = [{'tx_date': '2023-01-15', 'tx_type': 'purchase', 'amount': 10000}]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert cfs[0][0] == date(2023, 1, 15)

    def test_date_object_passthrough(self):
        """Handles date objects directly."""
        txns = [{'tx_date': date(2023, 1, 15), 'tx_type': 'purchase', 'amount': 10000}]
        cfs = build_cashflows_for_folio(txns, 11000, as_of_date=date(2024, 1, 1))
        assert cfs[0][0] == date(2023, 1, 15)

    def test_empty_transactions(self):
        """Empty transactions with terminal value gives single cashflow."""
        cfs = build_cashflows_for_folio([], 10000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 1
        assert cfs[0] == (date(2024, 1, 1), 10000)

    def test_full_sip_scenario(self):
        """Full SIP scenario computes meaningful XIRR."""
        txns = []
        for month in range(1, 13):
            txns.append({
                'tx_date': f'2023-{month:02d}-01',
                'tx_type': 'sip',
                'amount': 5000,
            })
        cfs = build_cashflows_for_folio(txns, 65000, as_of_date=date(2024, 1, 1))
        result = xirr(cfs)
        assert result is not None
        assert result > 0  # 60000 invested, 65000 terminal


class TestValidateAmount:
    """Tests for Tier 1: per-transaction cross-validation (_validate_amount)."""

    def test_corrupt_amount_overridden_by_units_nav(self):
        """When amount is >100x expected (units*nav), use expected instead."""
        tx = {'amount': 949_000_000, 'units': 54972, 'nav': 11.0}
        # expected = 54972 * 11 = 604692
        # ratio = 949M / 604692 >> 100
        result = _validate_amount(tx)
        assert abs(result - 604692) < 1

    def test_garbled_units_keeps_amount(self):
        """When units/nav are garbled (ratio < 0.01), keep original amount."""
        tx = {'amount': 1961, 'units': 10000, 'nav': 50.0}
        # expected = 10000 * 50 = 500000
        # ratio = 1961 / 500000 = 0.0039 < 0.01
        result = _validate_amount(tx)
        assert result == 1961

    def test_negative_nav_keeps_amount(self):
        """When NAV is negative, skip cross-check, keep amount."""
        tx = {'amount': 5000, 'units': 100, 'nav': -5000}
        result = _validate_amount(tx)
        assert result == 5000

    def test_zero_units_keeps_amount(self):
        """When units are zero, skip cross-check, keep amount."""
        tx = {'amount': 5000, 'units': 0, 'nav': 50.0}
        result = _validate_amount(tx)
        assert result == 5000

    def test_no_nav_keeps_amount(self):
        """When NAV is missing, skip cross-check, keep amount."""
        tx = {'amount': 5000, 'units': 100}
        result = _validate_amount(tx)
        assert result == 5000

    def test_agreeing_amount_kept(self):
        """When amount and units*nav agree (ratio ~1), keep amount."""
        tx = {'amount': 5000, 'units': 100, 'nav': 50.0}
        # expected = 5000, ratio = 1.0
        result = _validate_amount(tx)
        assert result == 5000

    def test_returns_positive_magnitude(self):
        """Always returns positive magnitude regardless of sign."""
        tx = {'amount': -5000, 'units': -100, 'nav': 50.0}
        result = _validate_amount(tx)
        assert result == 5000


class TestTier2OutlierRemoval:
    """Tests for Tier 2: portfolio-context outlier detection."""

    def test_outlier_cashflow_removed(self):
        """Cashflow >500x current_value is removed when >= 3 cashflows."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'sip', 'amount': 5000},
            {'tx_date': '2023-02-01', 'tx_type': 'sip', 'amount': 5000},
            {'tx_date': '2023-03-01', 'tx_type': 'redemption', 'amount': -949_000_000,
             'nav': -5000, 'units': -54972},  # corrupt: nav<0, so Tier 1 can't help
        ]
        # current_value = 800_000 → threshold = 400M
        # The 949M cashflow exceeds threshold → removed
        cfs = build_cashflows_for_folio(txns, 800_000, as_of_date=date(2024, 1, 1))
        # Should have: 2 SIPs + terminal value = 3 cashflows (outlier removed)
        amounts = [cf[1] for cf in cfs]
        assert all(abs(a) < 1_000_000 for a in amounts)

    def test_legitimate_large_transaction_kept(self):
        """Lumpsum investment that's large but <500x current_value is kept."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 1_000_000},
            {'tx_date': '2023-06-01', 'tx_type': 'sip', 'amount': 5000},
            {'tx_date': '2023-09-01', 'tx_type': 'sip', 'amount': 5000},
        ]
        # current_value = 1_500_000 → threshold = 750M
        # 1M lumpsum is well under → kept
        cfs = build_cashflows_for_folio(txns, 1_500_000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 4  # 3 transactions + terminal
        assert cfs[0][1] == -1_000_000

    def test_tier2_skipped_when_fewer_than_3_cashflows(self):
        """Outlier removal skipped when < 3 cashflows (could be legitimate)."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 500_000_000},
        ]
        # current_value = 1000 → threshold = 500_000
        # Single cashflow, so Tier 2 doesn't apply
        cfs = build_cashflows_for_folio(txns, 1000, as_of_date=date(2024, 1, 1))
        assert len(cfs) == 2
        assert cfs[0][1] == -500_000_000

    def test_tier2_skipped_when_no_current_value(self):
        """Outlier removal skipped when current_value is 0 (fully redeemed)."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'purchase', 'amount': 10000},
            {'tx_date': '2023-06-01', 'tx_type': 'sip', 'amount': 5000},
            {'tx_date': '2023-09-01', 'tx_type': 'redemption', 'amount': -999_999_999},
        ]
        cfs = build_cashflows_for_folio(txns, 0, as_of_date=date(2024, 1, 1))
        # No terminal value, no Tier 2 → all 3 kept
        assert len(cfs) == 3

    def test_tier1_and_tier2_combined(self):
        """Tier 1 fixes amount, Tier 2 not triggered for reasonable result."""
        txns = [
            {'tx_date': '2023-01-01', 'tx_type': 'sip', 'amount': 5000,
             'units': 100, 'nav': 50.0},
            {'tx_date': '2023-02-01', 'tx_type': 'sip', 'amount': 5000,
             'units': 100, 'nav': 50.0},
            # Corrupt amount but units*nav is correct → Tier 1 fixes it
            {'tx_date': '2023-03-01', 'tx_type': 'sip', 'amount': 50_000_000,
             'units': 100, 'nav': 50.0},
        ]
        cfs = build_cashflows_for_folio(txns, 16000, as_of_date=date(2024, 1, 1))
        # Tier 1 should fix the 50M to 5000
        amounts = [cf[1] for cf in cfs]
        assert all(abs(a) <= 16000 for a in amounts)
