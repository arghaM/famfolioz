"""
Pure Python XIRR (Extended Internal Rate of Return) calculator.

Uses Newton-Raphson method with fallbacks for robust convergence.
No external dependencies (no scipy/numpy required).
"""

from datetime import date, datetime
from typing import List, Optional, Tuple

# Transaction types that represent external cash movements
OUTFLOW_TYPES = {'purchase', 'sip', 'switch_in'}
INFLOW_TYPES = {'redemption', 'switch_out', 'dividend_payout'}
SKIP_TYPES = {'dividend_reinvestment', 'stamp_duty', 'stt'}


def xirr(cashflows: List[Tuple[date, float]], tolerance: float = 1e-7,
         max_iterations: int = 300) -> Optional[float]:
    """
    Calculate XIRR using Newton-Raphson with fallbacks.

    Args:
        cashflows: List of (date, amount) tuples.
                   Negative = money out (investment), Positive = money in (redemption/terminal value).
        tolerance: Convergence tolerance.
        max_iterations: Max Newton-Raphson iterations per guess.

    Returns:
        Annualized return as float (e.g., 0.12 for 12%), or None if no solution found.
    """
    if len(cashflows) < 2:
        return None

    amounts = [cf[1] for cf in cashflows]
    has_positive = any(a > 0 for a in amounts)
    has_negative = any(a < 0 for a in amounts)
    if not (has_positive and has_negative):
        return None

    # Sort by date
    cashflows = sorted(cashflows, key=lambda x: x[0])
    d0 = cashflows[0][0]

    # Pre-compute year fractions
    year_fracs = []
    for d, amount in cashflows:
        delta = (d - d0).days / 365.0
        year_fracs.append((amount, delta))

    def npv(rate):
        """Net present value at given rate."""
        return sum(amt / (1.0 + rate) ** yf for amt, yf in year_fracs)

    def dnpv(rate):
        """Derivative of NPV with respect to rate."""
        return sum(-yf * amt / (1.0 + rate) ** (yf + 1.0) for amt, yf in year_fracs)

    # NPV tolerance: relative to total cashflow magnitude
    total_abs = sum(abs(a) for a, _ in year_fracs)
    npv_tol = max(total_abs * 1e-6, 1.0)

    def newton_raphson(guess):
        """Run Newton-Raphson from initial guess."""
        rate = guess
        for _ in range(max_iterations):
            val = npv(rate)
            deriv = dnpv(rate)
            if abs(deriv) < 1e-14:
                break
            new_rate = rate - val / deriv
            # Clamp to valid range
            new_rate = max(-0.999, min(10.0, new_rate))
            if abs(new_rate - rate) < tolerance:
                # Verify NPV is actually near zero (not just stuck at clamp boundary)
                if abs(npv(new_rate)) < npv_tol:
                    return new_rate
                break
            rate = new_rate
        # Check if we converged close enough
        if abs(npv(rate)) < npv_tol:
            return rate
        return None

    # Smart initial guess from simple annualized return
    total_out = sum(a for a, _ in year_fracs if a < 0)
    total_in = sum(a for a, _ in year_fracs if a > 0)
    total_years = year_fracs[-1][1]
    if total_years > 0 and total_out < 0:
        simple_return = (-total_in / total_out) - 1.0
        initial_guess = simple_return / max(total_years, 0.5)
        initial_guess = max(-0.99, min(5.0, initial_guess))
    else:
        initial_guess = 0.1

    # Try Newton-Raphson with smart guess first, then fallbacks
    guesses = [initial_guess, 0.0, 0.1, 0.5, -0.5, 1.0, -0.9]
    for guess in guesses:
        result = newton_raphson(guess)
        if result is not None and -0.999 <= result <= 10.0:
            return result

    # Bisection fallback
    return _bisection(npv, -0.999, 10.0, tolerance, max_iterations)


def _bisection(f, lo, hi, tolerance, max_iterations):
    """Bisection method as last-resort fallback."""
    f_lo = f(lo)
    f_hi = f(hi)
    if f_lo * f_hi > 0:
        return None

    for _ in range(max_iterations):
        mid = (lo + hi) / 2.0
        f_mid = f(mid)
        if abs(f_mid) < tolerance or (hi - lo) < tolerance:
            return mid
        if f_lo * f_mid < 0:
            hi = mid
        else:
            lo = mid
            f_lo = f_mid
    return (lo + hi) / 2.0


def _validate_amount(tx: dict) -> float:
    """
    Cross-validate transaction amount against units × NAV.

    When both units and NAV are available and positive, computes
    expected = abs(units) * nav. If the stored amount is wildly off
    (ratio > 100), uses the expected value instead. If units/NAV appear
    garbled (ratio < 0.01), keeps the original amount.

    Returns the validated amount (always positive magnitude).
    """
    amount = abs(tx.get('amount', 0))
    nav = tx.get('nav')
    units = tx.get('units')

    if nav is not None and nav > 0 and units is not None and abs(units) > 0:
        expected = abs(units) * nav
        if expected > 0:
            ratio = amount / expected
            if ratio > 100:
                # Amount is corrupt (e.g. 949M vs expected ~6L), use expected
                return expected
            # ratio < 0.01 means units/NAV garbled, keep amount as-is
            # Otherwise amount and cross-check agree

    return amount


def build_cashflows_for_folio(
    transactions: List[dict],
    current_value: float,
    as_of_date: date = None,
) -> List[Tuple[date, float]]:
    """
    Build cashflow list from transactions for XIRR calculation.

    Convention: negative = money going out (investment), positive = money coming in.

    DB stores: purchase/sip/switch_in as positive amounts,
               redemption/switch_out as negative amounts.

    For XIRR: investments are outflows (negative), redemptions are inflows (positive).

    Includes two tiers of validation against corrupt data:
    - Tier 1: Per-transaction cross-validation using units × NAV
    - Tier 2: Portfolio-context outlier removal using current_value as reference

    Args:
        transactions: List of transaction dicts with tx_date, tx_type, amount,
                      and optionally units, nav.
        current_value: Current market value of the holding.
        as_of_date: Date for terminal value (defaults to today).

    Returns:
        List of (date, amount) tuples suitable for xirr().
    """
    if as_of_date is None:
        as_of_date = date.today()

    cashflows = []

    for tx in transactions:
        amount = tx.get('amount')
        if amount is None or amount == 0:
            continue

        tx_type = (tx.get('tx_type') or '').lower()
        if tx_type in SKIP_TYPES:
            continue

        # Parse date
        tx_date = _parse_date(tx.get('tx_date'))
        if tx_date is None:
            continue

        # Tier 1: Cross-validate amount against units × NAV
        validated = _validate_amount(tx)

        if tx_type in OUTFLOW_TYPES:
            # Normal purchase: amount > 0 → outflow (negative cashflow)
            # Reversal purchase: amount < 0 → inflow (positive cashflow, money returned)
            if amount < 0:
                cashflows.append((tx_date, validated))
            else:
                cashflows.append((tx_date, -validated))
        elif tx_type in INFLOW_TYPES:
            cashflows.append((tx_date, validated))
        # Unknown types are skipped

    # Tier 2: Remove outlier cashflows relative to current_value
    if current_value and current_value > 0 and len(cashflows) >= 3:
        threshold = current_value * 500
        cashflows = [
            cf for cf in cashflows
            if abs(cf[1]) <= threshold
        ]

    # Terminal value: current holding value as inflow
    if current_value and current_value > 0:
        cashflows.append((as_of_date, current_value))

    return cashflows


def _parse_date(val) -> Optional[date]:
    """Parse a date from string or date object."""
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%d-%b-%Y'):
            try:
                return datetime.strptime(val, fmt).date()
            except ValueError:
                continue
    return None
