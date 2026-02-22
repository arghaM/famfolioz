"""
Portfolio benchmarking module.

Reconstructs portfolio value from individual holdings' NAV history
(fetched from MFAPI.in), auto-selects benchmarks by asset class,
and computes financial metrics: absolute return, CAGR, volatility,
max drawdown, alpha.

Uses Time-Weighted Return (TWR) to strip out the effect of cash flows
(SIPs, redemptions) so portfolio returns are comparable to benchmark NAVs.
"""

import json
import logging
import math
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta

from cas_parser.webapp import data as db
from cas_parser.webapp.xirr import xirr as compute_xirr

logger = logging.getLogger(__name__)

# Default benchmarks by asset class (using index fund AMFI codes)
DEFAULT_BENCHMARKS = {
    'equity': {'scheme_code': 120716, 'name': 'UTI Nifty 50 Index Fund (Direct)'},
    'debt': {'scheme_code': 152515, 'name': 'Axis CRISIL IBX SDL Debt Index Fund'},
    'hybrid': {
        'composite': True,
        'components': [
            {'scheme_code': 120716, 'name': 'Nifty 50', 'weight': 0.65},
            {'scheme_code': 152515, 'name': 'Debt Index', 'weight': 0.35},
        ]
    },
    'gold_commodity': {'scheme_code': 106193, 'name': 'Kotak Gold ETF'},
}

# Transaction types that represent external cash flows
_CASHFLOW_IN_KEYWORDS = {'purchase', 'sip', 'systematic investment', 'new purchase'}
_CASHFLOW_OUT_KEYWORDS = {'redemption', 'systematic withdrawal'}
# Switch/STP keywords — skipped in per-transaction classification, but handled
# at portfolio level via _compute_switch_net_flows() which nets switch amounts
# across folios. Matched pairs (both switch_in and switch_out in portfolio)
# cancel out. Orphaned switches (source/dest outside portfolio) produce net
# flows that are treated as real external cash movements.
_SWITCH_IN_KEYWORDS = {'switch in', 'switched in', 'switch_in', 'stp_in', 'stp in'}
_SWITCH_OUT_KEYWORDS = {'switch out', 'switched out', 'switch_out', 'stp_out', 'stp out'}


def _classify_cash_flow(tx_type):
    """Classify a transaction as external cash flow direction.

    Returns: 1 for cash IN, -1 for cash OUT, 0 for not an external cash flow.

    Switch/STP transactions return 0 here because they are handled separately
    at the portfolio level by _compute_switch_net_flows().
    """
    tx_lower = (tx_type or '').lower()

    # Check switch/STP FIRST — handled at portfolio level, not per-transaction
    for kw in _SWITCH_IN_KEYWORDS:
        if kw in tx_lower:
            return 0
    for kw in _SWITCH_OUT_KEYWORDS:
        if kw in tx_lower:
            return 0

    for kw in _CASHFLOW_IN_KEYWORDS:
        if kw in tx_lower:
            return 1

    for kw in _CASHFLOW_OUT_KEYWORDS:
        if kw in tx_lower:
            return -1

    # Dividend payout: money leaves portfolio to investor
    if 'dividend' in tx_lower and 'payout' in tx_lower:
        return -1
    if tx_lower == 'dividend_payout':
        return -1

    # Everything else (dividend reinvest, STT, stamp duty, charges, switches)
    # is NOT an external cash flow
    return 0


def _compute_switch_net_flows(folios):
    """Compute net switch/STP cash flows across all portfolio folios.

    True internal transfers (matched switch_in + switch_out within the
    portfolio) net to ~0 on the same date. Orphaned switches (source/dest
    fund not in portfolio) produce non-zero net flows that represent real
    external cash movements.

    Example:
        - switch_out from Fund A (-500K) + switch_in to Fund B (+500K) = net 0
        - switch_in to Fund B (+500K) with no matching switch_out = net +500K
          (real money entering portfolio from outside)

    Returns: {date_str: net_amount} for dates with material net switch flows.
    """
    switch_flows = defaultdict(float)

    for folio in folios:
        for tx in folio.get('transactions', []):
            tx_type = (tx.get('tx_type') or '').lower()
            is_switch = any(
                kw in tx_type
                for kw in _SWITCH_IN_KEYWORDS | _SWITCH_OUT_KEYWORDS
            )
            if not is_switch:
                continue

            amount = tx.get('amount')
            if amount is None or amount == 0:
                tx_nav = tx.get('nav')
                tx_units = tx.get('units', 0) or 0
                if tx_nav and tx_nav > 0:
                    amount = tx_units * tx_nav
            if amount and abs(amount) > 0.01:
                switch_flows[tx['tx_date']] += amount

    # Return only dates with material net flows (> 1 rupee tolerance
    # to ignore rounding differences between matched switch pairs)
    return {d: round(amt, 2) for d, amt in switch_flows.items()
            if abs(amt) > 1.0}


def _is_hidden_dividend_payout(tx):
    """Detect dividend payouts disguised as 'charges' in CAS data.

    IDCW (Income Distribution cum Capital Withdrawal) funds distribute
    dividends that sometimes appear as 'charges' in CAS PDFs, with
    positive amount and zero units. These are real cash outflows
    (money sent to investor's bank account).

    Returns True if the transaction looks like a dividend payout.
    """
    tx_type = (tx.get('tx_type') or '').lower()
    if tx_type != 'charges':
        return False
    amount = tx.get('amount') or 0
    units = tx.get('units') or 0
    return amount > 0 and abs(units) < 0.001


def fetch_fund_nav(scheme_code):
    """Fetch NAV history from MFAPI and cache in benchmark_data table.

    Returns list of {date: 'YYYY-MM-DD', nav: float} sorted by date ASC.
    """
    # Check cache freshness
    latest = db.get_benchmark_data_latest_date(scheme_code)
    needs_refresh = False
    if not latest:
        needs_refresh = True
    else:
        latest_dt = datetime.strptime(latest, '%Y-%m-%d')
        if (datetime.now() - latest_dt).days > 2:
            needs_refresh = True

    if needs_refresh:
        try:
            url = f'https://api.mfapi.in/mf/{scheme_code}'
            req = urllib.request.Request(url, headers={'User-Agent': 'FamFolioz/1.0'})
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode('utf-8'))

            nav_data = result.get('data', [])
            if nav_data:
                rows = []
                for entry in nav_data:
                    try:
                        dt = datetime.strptime(entry['date'], '%d-%m-%Y')
                        rows.append({
                            'data_date': dt.strftime('%Y-%m-%d'),
                            'nav': float(entry['nav'])
                        })
                    except (ValueError, KeyError):
                        continue
                db.upsert_benchmark_data(scheme_code, rows)
        except Exception as e:
            logger.warning(f"Failed to fetch NAV for scheme {scheme_code}: {e}")

    # Return from cache
    data = db.get_benchmark_data(scheme_code)
    return [{'date': d['data_date'], 'nav': d['nav']} for d in data]


def _build_cumulative_units(transactions):
    """Build cumulative units timeline from transactions.

    Returns sorted list of (date_str, cumulative_units).
    """
    timeline = []
    cumulative = 0.0
    for tx in sorted(transactions, key=lambda t: t['tx_date']):
        units = tx['units'] or 0
        cumulative += units
        timeline.append((tx['tx_date'], cumulative))
    return timeline


def _get_units_on_date(units_timeline, target_date):
    """Get units held on a given date using forward-fill from units timeline."""
    result = 0.0
    for date_str, units in units_timeline:
        if date_str > target_date:
            break
        result = units
    return result


def _nav_lookup(nav_list, target_date):
    """Forward-fill NAV lookup: find latest NAV on or before target_date."""
    result = None
    for entry in nav_list:
        if entry['date'] > target_date:
            break
        result = entry['nav']
    return result


def build_portfolio_timeseries(investor_id, category=None, start_date=None, end_date=None):
    """Build portfolio value timeseries from individual holdings' NAV history.

    For each folio: gets transactions -> builds cumulative units,
    fetches NAV history via MFAPI, multiplies units x NAV for each date.

    Returns (timeseries, cash_flows) where:
        timeseries: [{date, value}]
        cash_flows: {date -> net_cash_flow_amount}
    """
    folios = db.get_folios_with_transactions(investor_id, category)
    if not folios:
        return [], {}

    # For each folio, build units timeline, fetch NAV, collect cash flows
    folio_data = []
    all_dates = set()
    all_cash_flows = defaultdict(float)  # date -> net cash flow

    for folio in folios:
        amfi_code = folio.get('amfi_code')
        if not amfi_code:
            continue

        transactions = folio['transactions']
        units_timeline = _build_cumulative_units(transactions)
        if not units_timeline:
            continue

        nav_history = fetch_fund_nav(int(amfi_code))
        if not nav_history:
            continue

        # Collect dates from NAV history
        for entry in nav_history:
            all_dates.add(entry['date'])

        # Collect cash flows from transactions
        for tx in transactions:
            if _classify_cash_flow(tx.get('tx_type', '')) == 0:
                # Check for hidden dividend payouts (IDCW 'charges')
                if _is_hidden_dividend_payout(tx):
                    # Dividend payout = money OUT of portfolio to investor
                    all_cash_flows[tx['tx_date']] += -(tx.get('amount') or 0)
                continue
            # Use amount directly — DB sign is correct:
            #   positive = money IN (purchase, SIP)
            #   negative = money OUT (redemption) or reversal of purchase
            amount = tx.get('amount')
            if amount is None or amount == 0:
                tx_nav = tx.get('nav')
                tx_units = tx.get('units', 0) or 0
                if tx_nav and tx_nav > 0:
                    amount = tx_units * tx_nav
            if amount and abs(amount) > 0.01:
                all_cash_flows[tx['tx_date']] += amount

        folio_data.append({
            'units_timeline': units_timeline,
            'nav_history': nav_history,
            'first_tx_date': units_timeline[0][0],
        })

    # Add net switch flows: orphaned switches are real external cash flows.
    # Matched pairs (switch_in + switch_out in same portfolio) cancel to ~0.
    switch_net = _compute_switch_net_flows(folios)
    for d, amt in switch_net.items():
        all_cash_flows[d] += amt

    if not folio_data or not all_dates:
        return [], {}

    # Build unified date grid
    sorted_dates = sorted(all_dates)

    # Apply date filters
    if start_date:
        sorted_dates = [d for d in sorted_dates if d >= start_date]
    if end_date:
        sorted_dates = [d for d in sorted_dates if d <= end_date]

    # Find earliest transaction date across all folios
    earliest_tx = min(fd['first_tx_date'] for fd in folio_data)
    sorted_dates = [d for d in sorted_dates if d >= earliest_tx]

    if not sorted_dates:
        return [], {}

    # Ensure transaction dates are included in the grid (important for TWR)
    tx_dates = set(all_cash_flows.keys())
    for d in tx_dates:
        if (not start_date or d >= start_date) and \
           (not end_date or d <= end_date) and d >= earliest_tx:
            all_dates.add(d)
    sorted_dates = sorted(set(sorted_dates) | (tx_dates & set(sorted_dates[0:1] + sorted_dates)))
    sorted_dates = sorted(set(sorted_dates))

    # Sample dates if too many (keep max ~500 points for performance)
    # But always keep transaction dates for accurate TWR
    if len(sorted_dates) > 500:
        step = len(sorted_dates) // 500
        sampled = set(sorted_dates[::step])
        # Always include first, last, and all transaction dates
        sampled.add(sorted_dates[0])
        sampled.add(sorted_dates[-1])
        sampled.update(d for d in tx_dates if d in set(sorted_dates))
        sorted_dates = sorted(sampled)

    # For each date, compute total portfolio value
    timeseries = []
    for date in sorted_dates:
        total_value = 0.0
        has_any_nav = False

        for fd in folio_data:
            # Skip dates before this folio's first transaction
            if date < fd['first_tx_date']:
                continue

            units = _get_units_on_date(fd['units_timeline'], date)
            if units <= 0:
                continue

            nav = _nav_lookup(fd['nav_history'], date)
            if nav is not None:
                total_value += units * nav
                has_any_nav = True

        if has_any_nav and total_value > 0:
            timeseries.append({'date': date, 'value': round(total_value, 2)})

    return timeseries, dict(all_cash_flows)


def _compute_twr_series(value_series, cash_flows):
    """Convert raw portfolio values + cash flows into a Time-Weighted Return
    (TWR) adjusted NAV series.

    This strips out the effect of external cash flows (SIPs, redemptions)
    so the resulting series reflects pure investment return — comparable
    to a benchmark NAV.

    Formula per period:
        r_t = (V_t - CF_t) / V_{t-1} - 1
        NAV_t = NAV_{t-1} * (1 + r_t)

    Where CF_t is the net external cash flow between t-1 and t.
    """
    if not value_series or len(value_series) < 2:
        return value_series or []

    # Sort cash flow dates for interval lookups
    sorted_cf_dates = sorted(cash_flows.keys()) if cash_flows else []

    result = [{'date': value_series[0]['date'], 'value': 100.0}]
    nav = 100.0

    for i in range(1, len(value_series)):
        prev_date = value_series[i - 1]['date']
        curr_date = value_series[i]['date']
        v_curr = value_series[i]['value']
        v_prev = value_series[i - 1]['value']

        # Sum cash flows in interval (prev_date, curr_date]
        interval_cf = 0.0
        for cf_date in sorted_cf_dates:
            if cf_date <= prev_date:
                continue
            if cf_date > curr_date:
                break
            interval_cf += cash_flows.get(cf_date, 0)

        if v_prev > 0:
            # TWR daily return: market movement only, excluding cash flows
            r = (v_curr - interval_cf) / v_prev - 1
            # Clamp extreme returns to avoid numerical issues
            r = max(r, -0.95)
            r = min(r, 5.0)
            nav *= (1 + r)
        elif interval_cf > 0 and v_curr > 0:
            # Portfolio starting from zero with new investment — keep nav as is
            pass

        result.append({'date': curr_date, 'value': round(nav, 4)})

    return result


def build_benchmark_timeseries(category, start_date=None, end_date=None,
                               category_weights=None):
    """Build benchmark timeseries for a given category.

    For 'all' (empty category), builds a weighted composite using
    the investor's actual category allocation.

    Returns {name: str, data: [{date, value}]}.
    """
    if not category:
        # "All" mode: build weighted benchmark from category weights
        return _build_weighted_benchmark(category_weights, start_date, end_date)

    bench = DEFAULT_BENCHMARKS.get(category)
    if not bench:
        return {'name': 'N/A', 'data': []}

    if bench.get('composite'):
        return _build_composite_benchmark(bench, start_date, end_date)

    # Simple single-fund benchmark
    nav_data = fetch_fund_nav(bench['scheme_code'])
    filtered = _filter_date_range(nav_data, start_date, end_date)
    return {
        'name': bench['name'],
        'data': [{'date': d['date'], 'value': d['nav']} for d in filtered]
    }


def _build_composite_benchmark(bench, start_date, end_date):
    """Build composite benchmark from weighted components."""
    components = []
    for comp in bench['components']:
        nav_data = fetch_fund_nav(comp['scheme_code'])
        filtered = _filter_date_range(nav_data, start_date, end_date)
        components.append({
            'weight': comp['weight'],
            'name': comp['name'],
            'data': {d['date']: d['nav'] for d in filtered}
        })

    if not components:
        return {'name': 'Composite', 'data': []}

    # Get all unique dates across components
    all_dates = set()
    for comp in components:
        all_dates.update(comp['data'].keys())
    sorted_dates = sorted(all_dates)

    # Normalize each component to base=1 at start
    base_navs = {}
    for comp in components:
        for d in sorted_dates:
            if d in comp['data']:
                base_navs[id(comp)] = comp['data'][d]
                break

    result = []
    for date in sorted_dates:
        weighted_val = 0.0
        valid = True
        for comp in components:
            nav = comp['data'].get(date)
            base = base_navs.get(id(comp))
            if nav is None or base is None or base == 0:
                valid = False
                break
            weighted_val += comp['weight'] * (nav / base)
        if valid:
            result.append({'date': date, 'value': round(weighted_val * 100, 4)})

    name = ' + '.join(f"{int(c['weight']*100)}% {c['name']}" for c in bench['components'])
    return {'name': name, 'data': result}


def _build_weighted_benchmark(category_weights, start_date, end_date):
    """Build weighted benchmark using investor's actual category allocation."""
    if not category_weights:
        # Fall back to equity benchmark
        return build_benchmark_timeseries('equity', start_date, end_date)

    # Get benchmark for each category with weight
    components = []
    for cat, weight in category_weights.items():
        if weight <= 0:
            continue
        bench = DEFAULT_BENCHMARKS.get(cat)
        if not bench or bench.get('composite'):
            # For composite or unknown, use equity as fallback
            bench = DEFAULT_BENCHMARKS.get('equity', {})
        if not bench.get('scheme_code'):
            continue

        nav_data = fetch_fund_nav(bench['scheme_code'])
        filtered = _filter_date_range(nav_data, start_date, end_date)
        components.append({
            'weight': weight,
            'name': bench.get('name', cat),
            'data': {d['date']: d['nav'] for d in filtered}
        })

    if not components:
        return {'name': 'Weighted Benchmark', 'data': []}

    # Get all unique dates
    all_dates = set()
    for comp in components:
        all_dates.update(comp['data'].keys())
    sorted_dates = sorted(all_dates)

    # Normalize each component to base=1 at start
    base_navs = {}
    for comp in components:
        for d in sorted_dates:
            if d in comp['data']:
                base_navs[id(comp)] = comp['data'][d]
                break

    result = []
    for date in sorted_dates:
        weighted_val = 0.0
        valid = True
        for comp in components:
            nav = comp['data'].get(date)
            base = base_navs.get(id(comp))
            if nav is None or base is None or base == 0:
                valid = False
                break
            weighted_val += comp['weight'] * (nav / base)
        if valid:
            result.append({'date': date, 'value': round(weighted_val * 100, 4)})

    parts = []
    for comp in components:
        pct = int(comp['weight'] * 100)
        if pct > 0:
            parts.append(f"{pct}% {comp['name']}")
    name = 'Weighted: ' + ' + '.join(parts) if parts else 'Weighted Benchmark'

    return {'name': name, 'data': result}


def _filter_date_range(data, start_date, end_date):
    """Filter list of {date, nav} by date range."""
    result = data
    if start_date:
        result = [d for d in result if d['date'] >= start_date]
    if end_date:
        result = [d for d in result if d['date'] <= end_date]
    return result


def calculate_metrics(timeseries):
    """Calculate financial metrics from a TWR-normalized timeseries.

    Returns dict with absolute_return, cagr (from TWR series),
    volatility, max_drawdown, max_drawdown_period.
    XIRR is computed separately and merged in get_performance_data.
    """
    if not timeseries or len(timeseries) < 2:
        return {
            'absolute_return': None,
            'cagr': None,
            'xirr': None,
            'volatility': None,
            'max_drawdown': None,
            'max_drawdown_period': None,
        }

    start_val = timeseries[0]['value']
    end_val = timeseries[-1]['value']

    # Absolute return (from TWR series)
    if start_val and start_val > 0:
        absolute_return = ((end_val - start_val) / start_val) * 100
    else:
        absolute_return = None

    # CAGR (from TWR series — used as fallback if XIRR unavailable)
    start_date = datetime.strptime(timeseries[0]['date'], '%Y-%m-%d')
    end_date = datetime.strptime(timeseries[-1]['date'], '%Y-%m-%d')
    days = (end_date - start_date).days
    years = days / 365.25

    if years > 0.01 and start_val and start_val > 0 and end_val > 0:
        cagr = (math.pow(end_val / start_val, 1 / years) - 1) * 100
    else:
        cagr = None

    # Volatility
    volatility = calculate_volatility(timeseries)

    # Max drawdown
    dd_result = calculate_max_drawdown(timeseries)
    max_drawdown = dd_result[0]
    if dd_result[1] and dd_result[2]:
        max_drawdown_period = f"{dd_result[1]} to {dd_result[2]}"
    else:
        max_drawdown_period = None

    return {
        'absolute_return': round(absolute_return, 2) if absolute_return is not None else None,
        'cagr': round(cagr, 2) if cagr is not None else None,
        'xirr': None,  # filled in by get_performance_data
        'volatility': round(volatility, 2) if volatility is not None else None,
        'max_drawdown': round(max_drawdown, 2) if max_drawdown is not None else None,
        'max_drawdown_period': max_drawdown_period,
    }


def _compute_portfolio_xirr(cash_flows, current_value):
    """Compute XIRR for the portfolio from cash flows + terminal value.

    cash_flows: {date_str: net_amount} where positive = money IN to portfolio.
    current_value: current portfolio market value (terminal value).

    Returns XIRR as percentage (e.g. 12.5) or None.
    """
    if not cash_flows or not current_value or current_value <= 0:
        return None

    xirr_cfs = []
    for date_str, amount in cash_flows.items():
        if abs(amount) < 0.01:
            continue
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue
        # Flip sign: money IN to portfolio = money OUT from investor (negative)
        xirr_cfs.append((d, -amount))

    if not xirr_cfs:
        return None

    # Terminal value: current portfolio value as positive inflow
    xirr_cfs.append((date.today(), current_value))

    result = compute_xirr(xirr_cfs)
    if result is not None:
        return round(result * 100, 2)
    return None


def _compute_benchmark_xirr(cash_flows, category=None, category_weights=None):
    """Compute XIRR as if the same cash flows were invested in the benchmark.

    Uses raw fund NAVs (not normalized timeseries) so we have full date coverage.
    For weighted benchmarks, splits each cash flow proportionally across funds.
    If a fund has no NAV for a date, its weight is redistributed to other funds.

    Returns XIRR as percentage or None.
    """
    if not cash_flows:
        return None

    # Build list of {scheme_code, weight} for the benchmark
    fund_configs = _get_benchmark_fund_configs(category, category_weights)
    if not fund_configs:
        return None

    # Fetch raw NAV data for each fund
    fund_navs = {}
    for fc in fund_configs:
        navs = fetch_fund_nav(fc['scheme_code'])
        if navs:
            fund_navs[fc['scheme_code']] = navs

    if not fund_navs:
        return None

    # Track units held per fund
    fund_units = {sc: 0.0 for sc in fund_navs}
    xirr_cfs = []

    for date_str in sorted(cash_flows.keys()):
        amount = cash_flows[date_str]
        if abs(amount) < 0.01:
            continue

        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue

        # Look up NAV for each fund on this date
        available = []
        for fc in fund_configs:
            sc = fc['scheme_code']
            if sc not in fund_navs:
                continue
            nav = _nav_lookup(fund_navs[sc], date_str)
            if nav and nav > 0:
                available.append({'scheme_code': sc, 'weight': fc['weight'], 'nav': nav})

        if not available:
            continue

        # Redistribute weights to available funds only
        total_weight = sum(a['weight'] for a in available)
        if total_weight <= 0:
            continue

        for a in available:
            adj_weight = a['weight'] / total_weight
            fund_units[a['scheme_code']] += (amount * adj_weight) / a['nav']

        # For XIRR: flip sign (money IN to portfolio = investor outflow = negative)
        xirr_cfs.append((d, -amount))

    if not xirr_cfs:
        return None

    # Terminal value: sum of units × latest NAV for each fund
    terminal_value = 0.0
    for sc, units in fund_units.items():
        if units <= 0:
            continue
        navs = fund_navs[sc]
        latest_nav = navs[-1]['nav'] if navs else 0
        terminal_value += units * latest_nav

    if terminal_value <= 0:
        return None

    xirr_cfs.append((date.today(), terminal_value))

    result = compute_xirr(xirr_cfs)
    if result is not None:
        return round(result * 100, 2)
    return None


def _get_benchmark_fund_configs(category, category_weights=None):
    """Get list of {scheme_code, weight} for benchmark XIRR calculation."""
    if category:
        bench = DEFAULT_BENCHMARKS.get(category)
        if not bench:
            return []
        if bench.get('composite'):
            return [
                {'scheme_code': c['scheme_code'], 'weight': c['weight']}
                for c in bench['components']
            ]
        return [{'scheme_code': bench['scheme_code'], 'weight': 1.0}]

    # "All MF" mode: build from category weights
    if not category_weights:
        # Fallback to pure equity benchmark
        return [{'scheme_code': DEFAULT_BENCHMARKS['equity']['scheme_code'], 'weight': 1.0}]

    configs = []
    for cat, weight in category_weights.items():
        if weight <= 0:
            continue
        bench = DEFAULT_BENCHMARKS.get(cat)
        if not bench:
            bench = DEFAULT_BENCHMARKS.get('equity')
        if bench.get('composite'):
            # Expand composite into components, scaled by category weight
            for c in bench['components']:
                configs.append({
                    'scheme_code': c['scheme_code'],
                    'weight': weight * c['weight']
                })
        else:
            configs.append({'scheme_code': bench['scheme_code'], 'weight': weight})

    # Merge duplicate scheme_codes (e.g. equity fallback used multiple times)
    merged = {}
    for c in configs:
        sc = c['scheme_code']
        merged[sc] = merged.get(sc, 0) + c['weight']

    return [{'scheme_code': sc, 'weight': w} for sc, w in merged.items()]


def calculate_volatility(timeseries):
    """Calculate annualized volatility from timeseries.

    Computes daily returns, then std dev * sqrt(252).
    """
    if not timeseries or len(timeseries) < 3:
        return None

    # Compute daily returns
    daily_returns = []
    for i in range(1, len(timeseries)):
        prev = timeseries[i - 1]['value']
        curr = timeseries[i]['value']
        if prev and prev > 0:
            daily_returns.append((curr - prev) / prev)

    if len(daily_returns) < 2:
        return None

    # Mean
    mean = sum(daily_returns) / len(daily_returns)

    # Variance
    variance = sum((r - mean) ** 2 for r in daily_returns) / (len(daily_returns) - 1)

    # Std dev * sqrt(252) for annualized
    std_dev = math.sqrt(variance)
    annualized = std_dev * math.sqrt(252) * 100  # as percentage

    return annualized


def calculate_max_drawdown(timeseries):
    """Calculate maximum drawdown from timeseries.

    Returns (max_drawdown_pct, peak_date, trough_date).
    """
    if not timeseries or len(timeseries) < 2:
        return (None, None, None)

    peak = timeseries[0]['value']
    peak_date = timeseries[0]['date']
    max_dd = 0.0
    max_dd_peak_date = None
    max_dd_trough_date = None

    for point in timeseries:
        if point['value'] >= peak:
            peak = point['value']
            peak_date = point['date']

        if peak > 0:
            drawdown = (peak - point['value']) / peak * 100
            if drawdown > max_dd:
                max_dd = drawdown
                max_dd_peak_date = peak_date
                max_dd_trough_date = point['date']

    return (
        round(max_dd, 2) if max_dd > 0 else 0.0,
        max_dd_peak_date,
        max_dd_trough_date
    )


def _align_to_common_dates(portfolio_ts, benchmark_ts):
    """Align portfolio and benchmark timeseries to common dates.

    Uses portfolio dates as primary, forward-fills benchmark values.
    """
    if not portfolio_ts:
        return portfolio_ts, benchmark_ts

    portfolio_dates = {p['date'] for p in portfolio_ts}

    # Build sorted benchmark lookup
    bm_sorted = sorted(benchmark_ts, key=lambda x: x['date'])

    aligned_bm = []
    for p_date in sorted(portfolio_dates):
        # Forward-fill: find latest benchmark date <= p_date
        val = None
        for entry in bm_sorted:
            if entry['date'] > p_date:
                break
            val = entry['value']
        if val is not None:
            aligned_bm.append({'date': p_date, 'value': val})

    # Filter portfolio to dates that have benchmark data
    bm_dates = {b['date'] for b in aligned_bm}
    aligned_pf = [p for p in portfolio_ts if p['date'] in bm_dates]

    return aligned_pf, aligned_bm


def _normalize_to_base100(timeseries):
    """Normalize timeseries to base=100 at start."""
    if not timeseries:
        return []

    base = timeseries[0]['value']
    if not base or base == 0:
        return timeseries

    return [
        {'date': p['date'], 'value': round((p['value'] / base) * 100, 4)}
        for p in timeseries
    ]


def get_performance_data(investor_id, category=None, start_date=None,
                         end_date=None, extra_benchmarks=None):
    """Main orchestrator: build portfolio + user-selected benchmark timeseries.

    Uses TWR (Time-Weighted Return) for the portfolio so that returns are
    comparable to benchmark NAVs (stripping out the effect of SIPs/redemptions).

    Benchmarks come exclusively from user-added selections (via the + button).
    No auto-selected composite benchmark.

    Returns complete response with timeseries, metrics, and alpha.
    """
    # 1. Build portfolio timeseries (raw values + cash flows)
    portfolio_ts, cash_flows = build_portfolio_timeseries(
        investor_id, category or None, start_date, end_date
    )

    # 2. Apply TWR to get a return-only series comparable to benchmark NAV
    twr_ts = _compute_twr_series(portfolio_ts, cash_flows)

    # 3. Normalize portfolio TWR series
    norm_pf = _normalize_to_base100(twr_ts)

    # 4. Calculate portfolio metrics
    pf_metrics = calculate_metrics(norm_pf)

    # 5. Compute portfolio XIRR
    current_value = portfolio_ts[-1]['value'] if portfolio_ts else 0

    # Scope cash flows to the selected time period.
    # For sub-periods (1Y, 3Y, etc.), the broker treats the portfolio value
    # at period start as an initial investment, then only counts SIPs/redemptions
    # within the period, plus current value as terminal.
    period_cash_flows = cash_flows
    start_value = 0
    if start_date and portfolio_ts:
        period_cash_flows = {
            d: amt for d, amt in cash_flows.items()
            if d >= start_date and (not end_date or d <= end_date)
        }
        start_value = portfolio_ts[0]['value']
        if start_value > 0:
            period_cash_flows[start_date] = (
                period_cash_flows.get(start_date, 0) + start_value
            )

    pf_xirr = _compute_portfolio_xirr(period_cash_flows, current_value)
    pf_metrics['xirr'] = pf_xirr

    # Category label
    cat_labels = {
        'equity': 'Equity', 'debt': 'Debt',
        'hybrid': 'Hybrid', 'gold_commodity': 'Gold/Commodity'
    }
    cat_label = cat_labels.get(category, 'All MF')

    # 6. Process user-added benchmarks
    #
    # Industry standard (used by M-Profit, brokers):
    #   Portfolio return = XIRR (money-weighted, accounts for SIP timing)
    #   Benchmark return = CAGR (simple time-weighted return of the index/fund)
    #   Alpha = Portfolio XIRR - Benchmark CAGR
    #
    # We compute CAGR directly from the benchmark's raw NAV for the period.
    # We also keep XIRR (simulating same cashflows) for the Excel export.
    benchmarks_list = []
    # For "ALL" period (no start_date), use portfolio start as benchmark start
    bm_start = start_date
    if not bm_start and portfolio_ts:
        bm_start = portfolio_ts[0]['date']

    if extra_benchmarks:
        for eb in extra_benchmarks:
            scheme_code = eb.get('scheme_code')
            if not scheme_code:
                continue

            eb_nav = fetch_fund_nav(int(scheme_code))
            eb_ts = [{'date': d['date'], 'value': d['nav']} for d in eb_nav]
            eb_ts = _filter_date_range_dicts(eb_ts, start_date, end_date)

            # Align to portfolio dates
            _, aligned_eb = _align_to_common_dates(norm_pf, eb_ts)
            norm_eb = _normalize_to_base100(aligned_eb)
            eb_metrics = calculate_metrics(norm_eb)

            # Compute benchmark CAGR directly from raw NAV
            # (matches how M-Profit and brokers display benchmark returns)
            eb_cagr = _compute_benchmark_cagr(eb_nav, bm_start)
            if eb_cagr is not None:
                eb_metrics['cagr'] = eb_cagr

            # Also compute XIRR (simulating same cashflows) for Excel export
            _, eb_xirr = _build_single_benchmark_export(
                int(scheme_code), period_cash_flows, start_date, start_value
            )
            eb_metrics['xirr'] = eb_xirr

            benchmarks_list.append({
                'name': eb.get('scheme_name', f'Fund {scheme_code}'),
                'scheme_code': scheme_code,
                'timeseries': norm_eb,
                'metrics': eb_metrics,
            })

    # 7. Compute alpha: Portfolio XIRR - Benchmark CAGR
    # This is the industry standard comparison (money-weighted portfolio
    # return vs time-weighted benchmark return).
    alpha = None
    if benchmarks_list:
        bm0 = benchmarks_list[0]['metrics']
        pf_return = pf_xirr if pf_xirr is not None else pf_metrics.get('cagr')
        bm_return = bm0.get('cagr')
        if pf_return is not None and bm_return is not None:
            alpha = round(pf_return - bm_return, 2)

    return {
        'portfolio': {
            'name': f'My Portfolio ({cat_label})',
            'timeseries': norm_pf,
            'metrics': pf_metrics,
        },
        'benchmarks': benchmarks_list,
        'alpha': alpha,
    }


def _compute_benchmark_cagr(nav_data, start_date):
    """Compute CAGR of a benchmark from its raw NAV history.

    This is the simple time-weighted return of the fund/index:
        CAGR = (end_nav / start_nav)^(1/years) - 1

    This matches how M-Profit and most brokers display benchmark returns.

    Args:
        nav_data: List of {date, nav} sorted by date ASC.
        start_date: Start date string (YYYY-MM-DD). If None, uses first NAV.

    Returns:
        CAGR as percentage (e.g. 13.35 for 13.35%), or None.
    """
    if not nav_data or len(nav_data) < 2:
        return None

    # Start NAV: forward-fill to start_date
    if start_date:
        start_nav = _nav_lookup(nav_data, start_date)
    else:
        start_nav = nav_data[0]['nav']
        start_date = nav_data[0]['date']

    if not start_nav or start_nav <= 0:
        return None

    # End NAV: latest available
    end_nav = nav_data[-1]['nav']
    end_date = nav_data[-1]['date']
    if not end_nav or end_nav <= 0:
        return None

    # Compute years between dates
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None

    days = (end_dt - start_dt).days
    if days < 7:
        return None

    years = days / 365.25
    cagr = (math.pow(end_nav / start_nav, 1 / years) - 1) * 100
    return round(cagr, 2)


def get_multi_period_returns(investor_id, category=None, extra_benchmarks=None):
    """Compute portfolio XIRR and benchmark CAGR for all standard periods.

    Builds folio data (units timelines + NAV histories) ONCE, then computes
    portfolio values at exact period boundary dates — avoiding the 500-point
    sampling artifact in build_portfolio_timeseries().

    Returns:
        {
            'periods': ['1Y', '2Y', '3Y', '5Y', 'ALL'],
            'portfolio': {'name': str, 'returns': {period: float|None}},
            'benchmarks': [{'name': str, 'scheme_code': int, 'returns': {period: float|None}}],
            'alpha': {period: float|None}
        }
    """
    cat_labels = {
        'equity': 'Equity', 'debt': 'Debt',
        'hybrid': 'Hybrid', 'gold_commodity': 'Gold/Commodity'
    }
    cat_label = cat_labels.get(category, 'All MF')
    empty_result = {
        'periods': ['1Y', '2Y', '3Y', '5Y', 'ALL'],
        'portfolio': {'name': f'My Portfolio ({cat_label})', 'returns': {}},
        'benchmarks': [],
        'alpha': {},
    }

    # 1. Build folio data: units timelines, NAV histories, cash flows
    #    (same logic as build_portfolio_timeseries, but we keep folio-level
    #     data so we can compute portfolio value at any exact date)
    folios = db.get_folios_with_transactions(investor_id, category or None)
    if not folios:
        return empty_result

    folio_data = []
    all_cash_flows = defaultdict(float)

    for folio in folios:
        amfi_code = folio.get('amfi_code')
        if not amfi_code:
            continue

        transactions = folio['transactions']
        units_timeline = _build_cumulative_units(transactions)
        if not units_timeline:
            continue

        nav_history = fetch_fund_nav(int(amfi_code))
        if not nav_history:
            continue

        # Collect cash flows from transactions
        for tx in transactions:
            if _classify_cash_flow(tx.get('tx_type', '')) == 0:
                # Check for hidden dividend payouts (IDCW 'charges')
                if _is_hidden_dividend_payout(tx):
                    all_cash_flows[tx['tx_date']] += -(tx.get('amount') or 0)
                continue
            amount = tx.get('amount')
            if amount is None or amount == 0:
                tx_nav = tx.get('nav')
                tx_units = tx.get('units', 0) or 0
                if tx_nav and tx_nav > 0:
                    amount = tx_units * tx_nav
            if amount and abs(amount) > 0.01:
                all_cash_flows[tx['tx_date']] += amount

        folio_data.append({
            'units_timeline': units_timeline,
            'nav_history': nav_history,
            'first_tx_date': units_timeline[0][0],
        })

    # Add net switch flows (orphaned switches = real external cash flows)
    switch_net = _compute_switch_net_flows(folios)
    for d, amt in switch_net.items():
        all_cash_flows[d] += amt

    if not folio_data:
        return empty_result

    cash_flows = dict(all_cash_flows)
    earliest_tx = min(fd['first_tx_date'] for fd in folio_data)

    def _portfolio_value_at(target_date):
        """Compute exact portfolio value at a date (units x NAV per folio)."""
        total = 0.0
        for fd in folio_data:
            if target_date < fd['first_tx_date']:
                continue
            units = _get_units_on_date(fd['units_timeline'], target_date)
            if units <= 0:
                continue
            nav = _nav_lookup(fd['nav_history'], target_date)
            if nav is not None:
                total += units * nav
        return round(total, 2)

    # Current portfolio value (latest available)
    today_str = date.today().strftime('%Y-%m-%d')
    current_value = _portfolio_value_at(today_str)
    if current_value <= 0:
        return empty_result

    # Pre-fetch benchmark NAV data (each call is cached)
    bm_nav_cache = {}
    if extra_benchmarks:
        for eb in extra_benchmarks:
            sc = eb.get('scheme_code')
            if sc:
                bm_nav_cache[int(sc)] = fetch_fund_nav(int(sc))

    # 2. For each period, compute returns
    periods = ['1Y', '2Y', '3Y', '5Y', 'ALL']
    year_map = {'1Y': 1, '2Y': 2, '3Y': 3, '5Y': 5}

    portfolio_returns = {}
    benchmark_returns = {int(eb['scheme_code']): {} for eb in (extra_benchmarks or []) if eb.get('scheme_code')}
    alpha_values = {}

    for period in periods:
        # Compute start date for this period
        if period == 'ALL':
            start_date = None
        else:
            years = year_map[period]
            start_dt = date.today() - relativedelta(years=years)
            start_date = start_dt.strftime('%Y-%m-%d')

            # Skip if portfolio doesn't go back far enough
            if start_date < earliest_tx:
                portfolio_returns[period] = None
                for sc in benchmark_returns:
                    benchmark_returns[sc][period] = None
                alpha_values[period] = None
                continue

        # Scope cash flows to period
        if start_date:
            period_cfs = {
                d: amt for d, amt in cash_flows.items() if d >= start_date
            }
            # Exact portfolio value at period start (no sampling artifact)
            start_value = _portfolio_value_at(start_date)
            if start_value > 0:
                period_cfs[start_date] = period_cfs.get(start_date, 0) + start_value
        else:
            period_cfs = dict(cash_flows)

        # Portfolio XIRR
        pf_xirr = _compute_portfolio_xirr(period_cfs, current_value)
        portfolio_returns[period] = pf_xirr

        # Benchmark start date: use start_date, or portfolio start for ALL
        bm_start = start_date if start_date else earliest_tx

        # Benchmark CAGR for each user-added benchmark
        for sc, nav_data in bm_nav_cache.items():
            bm_cagr = _compute_benchmark_cagr(nav_data, bm_start)
            benchmark_returns[sc][period] = bm_cagr

        # Alpha vs first benchmark
        if extra_benchmarks and bm_nav_cache:
            first_sc = int(extra_benchmarks[0]['scheme_code'])
            bm_ret = benchmark_returns.get(first_sc, {}).get(period)
            if pf_xirr is not None and bm_ret is not None:
                alpha_values[period] = round(pf_xirr - bm_ret, 2)
            else:
                alpha_values[period] = None
        else:
            alpha_values[period] = None

    # 3. Build response
    benchmarks_list = []
    if extra_benchmarks:
        for eb in extra_benchmarks:
            sc = eb.get('scheme_code')
            if not sc:
                continue
            benchmarks_list.append({
                'name': eb.get('scheme_name', f'Fund {sc}'),
                'scheme_code': int(sc),
                'returns': benchmark_returns.get(int(sc), {}),
            })

    return {
        'periods': periods,
        'portfolio': {
            'name': f'My Portfolio ({cat_label})',
            'returns': portfolio_returns,
        },
        'benchmarks': benchmarks_list,
        'alpha': alpha_values,
    }


def _filter_date_range_dicts(data, start_date, end_date):
    """Filter list of {date, value} dicts by date range."""
    result = data
    if start_date:
        result = [d for d in result if d['date'] >= start_date]
    if end_date:
        result = [d for d in result if d['date'] <= end_date]
    return result


def get_cashflows_for_export(investor_id, category=None, start_date=None,
                              end_date=None, benchmarks=None):
    """Build period-scoped cashflow data for Excel export validation.

    Uses the user-added benchmarks (from the + button) to generate
    benchmark cashflow sheets. Each benchmark gets its own entry.

    Args:
        benchmarks: List of user-added benchmarks, each with
                    scheme_code and scheme_name.

    Returns structured data with portfolio cashflows and one benchmark
    cashflow set per user-added benchmark.
    """
    # 1. Build portfolio timeseries + all cash flows
    portfolio_ts, cash_flows = build_portfolio_timeseries(
        investor_id, category or None, start_date, end_date
    )

    if not portfolio_ts:
        return {
            'portfolio_cashflows': [],
            'benchmarks': [],
            'portfolio_xirr': None,
            'category': category or '',
        }

    current_value = portfolio_ts[-1]['value']

    # 2. Scope cash flows to the selected period
    period_cash_flows = cash_flows
    start_value = 0
    if start_date and portfolio_ts:
        period_cash_flows = {
            d: amt for d, amt in cash_flows.items()
            if d >= start_date and (not end_date or d <= end_date)
        }
        start_value = portfolio_ts[0]['value']
        if start_value > 0:
            period_cash_flows[start_date] = (
                period_cash_flows.get(start_date, 0) + start_value
            )

    # 3. Category label
    cat_labels = {
        'equity': 'Equity', 'debt': 'Debt',
        'hybrid': 'Hybrid', 'gold_commodity': 'Gold/Commodity'
    }
    cat_label = cat_labels.get(category, 'All MF')

    # 4. Build portfolio cashflow rows (XIRR sign convention)
    portfolio_rows = _build_portfolio_export_rows(
        period_cash_flows, cash_flows, current_value, start_date, start_value
    )

    # Compute portfolio XIRR
    pf_xirr = _compute_portfolio_xirr(period_cash_flows, current_value)

    # 5. Build benchmark cashflow rows for each user-added benchmark
    benchmark_results = []
    if benchmarks:
        for bm in benchmarks:
            scheme_code = bm.get('scheme_code')
            scheme_name = bm.get('scheme_name', f'Fund {scheme_code}')
            if not scheme_code:
                continue

            bm_rows, bm_xirr = _build_single_benchmark_export(
                int(scheme_code), period_cash_flows, start_date, start_value
            )
            benchmark_results.append({
                'name': scheme_name,
                'scheme_code': scheme_code,
                'cashflows': bm_rows,
                'xirr': bm_xirr,
            })

    return {
        'portfolio_cashflows': portfolio_rows,
        'benchmarks': benchmark_results,
        'portfolio_xirr': pf_xirr,
        'category': cat_label,
    }


def _build_portfolio_export_rows(period_cash_flows, all_cash_flows,
                                  current_value, start_date, start_value):
    """Build portfolio cashflow rows for Excel export."""
    rows = []
    for date_str in sorted(period_cash_flows.keys()):
        amount = period_cash_flows[date_str]
        if abs(amount) < 0.01:
            continue

        if start_date and date_str == start_date and start_value > 0:
            actual_cf = all_cash_flows.get(date_str, 0)
            if abs(actual_cf) > 0.01:
                rows.append({
                    'date': date_str,
                    'description': 'Initial Portfolio Value',
                    'amount': round(-start_value, 2),
                })
                rows.append({
                    'date': date_str,
                    'description': 'SIP' if actual_cf > 0 else 'Redemption',
                    'amount': round(-actual_cf, 2),
                })
                continue
            else:
                desc = 'Initial Portfolio Value'
        else:
            desc = 'SIP' if amount > 0 else 'Redemption'

        rows.append({
            'date': date_str,
            'description': desc,
            'amount': round(-amount, 2),
        })

    rows.append({
        'date': date.today().strftime('%Y-%m-%d'),
        'description': 'Current Portfolio Value',
        'amount': round(current_value, 2),
    })
    return rows


def _build_single_benchmark_export(scheme_code, period_cash_flows,
                                    start_date, start_value):
    """Build benchmark cashflow rows for a single fund.

    Simulates buying/selling units of this fund with the same cash flows.
    Returns (rows, xirr_pct).
    """
    nav_data = fetch_fund_nav(scheme_code)
    if not nav_data:
        return [], None

    cumulative_units = 0.0
    rows = []
    xirr_cfs = []

    for date_str in sorted(period_cash_flows.keys()):
        amount = period_cash_flows[date_str]
        if abs(amount) < 0.01:
            continue

        nav = _nav_lookup(nav_data, date_str)
        if not nav or nav <= 0:
            continue

        units_bought = amount / nav
        cumulative_units += units_bought

        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue

        # Description
        if start_date and date_str == start_date and start_value > 0:
            desc = 'Initial Value'
        else:
            desc = 'SIP' if amount > 0 else 'Redemption'

        rows.append({
            'date': date_str,
            'description': desc,
            'amount': round(-amount, 2),
            'nav': round(nav, 4),
            'units': round(units_bought, 4),
            'cumulative_units': round(cumulative_units, 4),
        })

        xirr_cfs.append((d, -amount))

    if not rows or cumulative_units <= 0:
        return rows, None

    # Terminal value
    latest_nav = nav_data[-1]['nav']
    terminal_value = cumulative_units * latest_nav

    rows.append({
        'date': date.today().strftime('%Y-%m-%d'),
        'description': 'Terminal Value',
        'amount': round(terminal_value, 2),
        'nav': round(latest_nav, 4),
        'units': 0,
        'cumulative_units': round(cumulative_units, 4),
    })

    xirr_cfs.append((date.today(), terminal_value))
    result = compute_xirr(xirr_cfs)
    bm_xirr = round(result * 100, 2) if result is not None else None

    return rows, bm_xirr
