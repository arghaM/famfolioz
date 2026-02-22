"""Tax harvesting, FIFO lot tracking, realized/unrealized gains, and asset allocation."""

import logging
from datetime import date, datetime
from typing import List, Optional, Tuple
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

BUY_TX_TYPES = {'purchase', 'sip', 'switch_in', 'stp_in', 'transfer_in', 'bonus', 'dividend_reinvestment'}
SELL_TX_TYPES = {'redemption', 'switch_out', 'stp_out', 'transfer_out'}

__all__ = [
    'get_current_fy_dates',
    'get_fund_tax_type',
    '_reverse_from_lots',
    'compute_fifo_lots',
    'compute_unrealized_gains',
    'compute_realized_gains_fy',
    'compute_tax_harvesting',
    'update_investor_tax_slab',
    'update_fund_exit_load',
    'confirm_fund_allocation_review',
    'get_funds_needing_review',
    'get_portfolio_asset_allocation',
]


def get_current_fy_dates() -> Tuple[str, str]:
    """Return (fy_start, fy_end) as YYYY-MM-DD for the current Indian financial year."""
    today = date.today()
    if today.month >= 4:
        fy_start = date(today.year, 4, 1)
        fy_end = date(today.year + 1, 3, 31)
    else:
        fy_start = date(today.year - 1, 4, 1)
        fy_end = date(today.year, 3, 31)
    return fy_start.isoformat(), fy_end.isoformat()


def get_fund_tax_type(isin: str) -> str:
    """Return 'equity' or 'debt' based on fund_category and equity_pct.

    Equity: fund_category='equity', or hybrid with equity_pct >= 65.
    Debt: everything else (debt, gold_commodity, hybrid <65% equity).
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT fund_category, COALESCE(equity_pct, 0) as equity_pct
            FROM mutual_fund_master WHERE isin = ?
        """, (isin,))
        row = cursor.fetchone()
        if not row:
            return 'equity'  # default assumption
        cat = row['fund_category']
        eq_pct = row['equity_pct'] or 0
        if cat == 'equity':
            return 'equity'
        if cat == 'hybrid' and eq_pct >= 65:
            return 'equity'
        if cat in ('debt', 'gold_commodity'):
            return 'debt'
        if cat == 'hybrid':
            return 'debt'
        # Unclassified — infer from equity_pct
        return 'equity' if eq_pct >= 65 else 'debt'


def _reverse_from_lots(lots: list, units_to_reverse: float, reversal_nav: float,
                       folio_id: int = None, tx_id: int = None) -> float:
    """Remove units from lots for a purchase reversal (not a sale).

    Searches from newest lot backwards for a matching NAV (within 1%).
    If no NAV match, removes from the newest lot.
    Returns the number of units that could not be matched.
    """
    target = units_to_reverse

    # First pass: find a lot with matching NAV (newest first)
    for i in range(len(lots) - 1, -1, -1):
        lot = lots[i]
        if lot['nav'] > 0 and reversal_nav > 0:
            if abs(lot['nav'] - reversal_nav) / lot['nav'] < 0.01:
                consumed = min(lot['units'], target)
                lot['cost'] = lot['cost'] * (lot['units'] - consumed) / lot['units'] if lot['units'] > 0 else 0
                lot['units'] -= consumed
                target -= consumed
                if lot['units'] < 0.0001:
                    lots.pop(i)
                if target < 0.0001:
                    return 0.0

    # Second pass: consume from newest lots if NAV match didn't cover everything
    for i in range(len(lots) - 1, -1, -1):
        if target < 0.0001:
            break
        lot = lots[i]
        consumed = min(lot['units'], target)
        lot['cost'] = lot['cost'] * (lot['units'] - consumed) / lot['units'] if lot['units'] > 0 else 0
        lot['units'] -= consumed
        target -= consumed
        if lot['units'] < 0.0001:
            lots.pop(i)

    if target > 0.01:
        logger.warning(f"Reversal over-consumption for folio_id={folio_id} tx_id={tx_id}: "
                       f"{target:.4f} units could not be matched")
    return target


def compute_fifo_lots(folio_id: int) -> List[dict]:
    """Build FIFO lots from buy transactions, consume on sell transactions.

    Purchase reversals (buy-type with negative units) undo lots at original cost,
    they do NOT generate realized gains.

    Returns remaining lots with positive units.
    Each lot: {tx_id, date, units, nav, cost, original_units}
    """
    skip_types = {'stt', 'stamp_duty', 'charges', 'segregated_portfolio', 'misc'}

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, tx_date, tx_type, units, nav, description
            FROM transactions
            WHERE folio_id = ? AND status = 'active'
            ORDER BY tx_date ASC, id ASC
        """, (folio_id,))
        txns = [dict(row) for row in cursor.fetchall()]

    lots = []
    for txn in txns:
        tx_type = (txn['tx_type'] or '').lower().strip()
        if tx_type in skip_types:
            continue

        units = txn['units'] or 0
        nav = txn['nav'] or 0

        if tx_type in BUY_TX_TYPES and units > 0:
            cost = units * nav  # bonus: nav=0 → cost=0
            lots.append({
                'tx_id': txn['id'],
                'date': txn['tx_date'],
                'units': units,
                'nav': nav,
                'cost': cost,
                'original_units': units,
            })
        elif tx_type in BUY_TX_TYPES and units < 0:
            # Purchase reversal — undo from lots at original cost (not a sale)
            if nav < 0 or nav > 100000:
                logger.warning(f"Skipping garbled reversal tx_id={txn['id']} "
                               f"(units={units}, nav={nav}) for folio_id={folio_id}")
                continue
            _reverse_from_lots(lots, abs(units), nav, folio_id, txn['id'])
        elif tx_type in SELL_TX_TYPES:
            # Actual sale — consume from oldest lots (FIFO)
            units_to_sell = abs(units)
            while units_to_sell > 0.0001 and lots:
                lot = lots[0]
                if lot['units'] <= units_to_sell + 0.0001:
                    units_to_sell -= lot['units']
                    lots.pop(0)
                else:
                    lot['cost'] = lot['cost'] * (lot['units'] - units_to_sell) / lot['units']
                    lot['units'] -= units_to_sell
                    units_to_sell = 0
            if units_to_sell > 0.01:
                logger.warning(f"FIFO over-consumption for folio_id={folio_id}: "
                               f"{units_to_sell:.4f} units could not be matched")

    return lots


def compute_unrealized_gains(folio_id: int, current_nav: float) -> List[dict]:
    """Enrich FIFO lots with unrealized gain info.

    Returns list of lots with: current_value, unrealized_gain, holding_days,
    is_long_term, gain_type.
    """
    lots = compute_fifo_lots(folio_id)
    today = date.today()
    enriched = []
    for lot in lots:
        lot_date = datetime.strptime(lot['date'], '%Y-%m-%d').date() if isinstance(lot['date'], str) else lot['date']
        holding_days = (today - lot_date).days
        current_value = lot['units'] * current_nav
        unrealized_gain = current_value - lot['cost']
        is_long_term = holding_days >= 365
        enriched.append({
            **lot,
            'current_value': round(current_value, 2),
            'unrealized_gain': round(unrealized_gain, 2),
            'holding_days': holding_days,
            'is_long_term': is_long_term,
            'gain_type': 'LTCL' if is_long_term else 'STCL',
        })
    return enriched


def compute_realized_gains_fy(investor_id: int) -> dict:
    """Compute realized gains for current FY across all folios.

    Returns: {equity_stcg, equity_ltcg, debt_gains, total_realized,
              ltcg_exemption_used, ltcg_exemption_remaining}
    """
    fy_start, fy_end = get_current_fy_dates()
    today = date.today()

    with get_db() as conn:
        cursor = conn.cursor()
        # Get all folios for investor
        cursor.execute("""
            SELECT f.id as folio_id, f.isin, f.scheme_name, f.folio_number
            FROM folios f WHERE f.investor_id = ?
        """, (investor_id,))
        folios = [dict(row) for row in cursor.fetchall()]

    equity_stcg = 0.0
    equity_ltcg = 0.0
    debt_gains = 0.0
    equity_stcg_details = []
    equity_ltcg_details = []
    debt_gains_details = []

    for folio in folios:
        isin = folio['isin']
        tax_type = get_fund_tax_type(isin) if isin else 'equity'

        # Replay FIFO for this folio, tracking realized gains on sell txns in current FY
        skip_types = {'stt', 'stamp_duty', 'charges', 'segregated_portfolio', 'misc'}
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, tx_date, tx_type, description, units, nav
                FROM transactions
                WHERE folio_id = ? AND status = 'active'
                ORDER BY tx_date ASC, id ASC
            """, (folio['folio_id'],))
            txns = [dict(row) for row in cursor.fetchall()]

        lots = []
        for txn in txns:
            tx_type = (txn['tx_type'] or '').lower().strip()
            if tx_type in skip_types:
                continue
            units = txn['units'] or 0
            nav = txn['nav'] or 0

            if tx_type in BUY_TX_TYPES and units > 0:
                lots.append({
                    'date': txn['tx_date'],
                    'units': units,
                    'nav': nav,
                    'cost': units * nav,
                })
            elif tx_type in BUY_TX_TYPES and units < 0:
                # Purchase reversal — undo from lots at original cost, no realized gain
                if nav < 0 or nav > 100000:
                    continue
                _reverse_from_lots(lots, abs(units), nav, folio['folio_id'], txn['id'])
            elif tx_type in SELL_TX_TYPES:
                # Actual sale — FIFO consume and track realized gains
                sell_date_str = txn['tx_date']
                sell_nav = nav
                in_fy = fy_start <= sell_date_str <= fy_end
                units_to_sell = abs(units)

                while units_to_sell > 0.0001 and lots:
                    lot = lots[0]
                    consumed = min(lot['units'], units_to_sell)
                    lot_cost_per_unit = lot['cost'] / lot['units'] if lot['units'] > 0 else 0
                    realized = consumed * (sell_nav - lot_cost_per_unit)

                    if in_fy:
                        lot_date = datetime.strptime(lot['date'], '%Y-%m-%d').date() if isinstance(lot['date'], str) else lot['date']
                        sell_date = datetime.strptime(sell_date_str, '%Y-%m-%d').date() if isinstance(sell_date_str, str) else sell_date_str
                        holding_days = (sell_date - lot_date).days
                        is_lt = holding_days >= 365

                        detail = {
                            'tx_id': txn['id'],
                            'folio_id': folio['folio_id'],
                            'scheme_name': folio['scheme_name'],
                            'folio_number': folio['folio_number'],
                            'sell_date': sell_date_str,
                            'description': txn.get('description', ''),
                            'units_sold': round(consumed, 4),
                            'buy_date': lot['date'],
                            'buy_nav': round(lot_cost_per_unit, 4),
                            'sell_nav': round(sell_nav, 4),
                            'realized_gain': round(realized, 2),
                            'holding_days': holding_days,
                        }

                        if tax_type == 'equity':
                            if is_lt:
                                equity_ltcg += realized
                                equity_ltcg_details.append(detail)
                            else:
                                equity_stcg += realized
                                equity_stcg_details.append(detail)
                        else:
                            debt_gains += realized
                            debt_gains_details.append(detail)

                    lot['cost'] -= consumed * lot_cost_per_unit
                    lot['units'] -= consumed
                    units_to_sell -= consumed
                    if lot['units'] < 0.0001:
                        lots.pop(0)

    ltcg_exemption = 125000.0  # ₹1.25L annual exemption
    ltcg_exemption_used = min(max(equity_ltcg, 0), ltcg_exemption)

    return {
        'equity_stcg': round(equity_stcg, 2),
        'equity_ltcg': round(equity_ltcg, 2),
        'debt_gains': round(debt_gains, 2),
        'total_realized': round(equity_stcg + equity_ltcg + debt_gains, 2),
        'ltcg_exemption_used': round(ltcg_exemption_used, 2),
        'ltcg_exemption_remaining': round(ltcg_exemption - ltcg_exemption_used, 2),
        'equity_stcg_details': equity_stcg_details,
        'equity_ltcg_details': equity_ltcg_details,
        'debt_gains_details': debt_gains_details,
    }


def compute_tax_harvesting(investor_id: int, tax_slab_pct: float = None) -> dict:
    """Orchestrator: compute tax-loss harvesting opportunities for an investor.

    Returns: {summary, opportunities, realized_gains, warnings}
    """
    from cas_parser.webapp.db.investors import get_investor_by_id
    from cas_parser.webapp.db.mutual_funds import get_similar_funds

    # Determine tax slab
    if tax_slab_pct is None:
        inv = get_investor_by_id(investor_id)
        tax_slab_pct = (inv or {}).get('tax_slab_pct') or 30.0

    today = date.today()

    with get_db() as conn:
        cursor = conn.cursor()
        # Get all folios with holdings for this investor
        cursor.execute("""
            SELECT f.id as folio_id, f.folio_number, f.scheme_name, f.isin, f.amc,
                   h.units as holding_units, h.current_value, h.cost_value,
                   COALESCE(mf.current_nav, h.nav) as current_nav,
                   COALESCE(mf.display_name, mf.amfi_scheme_name, f.scheme_name) as display_name,
                   mf.id as mf_id,
                   COALESCE(mf.fund_category, '') as fund_category,
                   COALESCE(mf.exit_load_pct, 1.0) as exit_load_pct,
                   COALESCE(mf.equity_pct, 0) as equity_pct
            FROM folios f
            JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
        """, (investor_id,))
        folios = [dict(row) for row in cursor.fetchall()]

    opportunities = []
    total_unrealized_loss = 0.0
    total_tax_savings = 0.0
    total_net_benefit = 0.0
    warnings = []

    for folio in folios:
        current_nav = folio['current_nav'] or 0
        if current_nav <= 0:
            continue

        isin = folio['isin']
        tax_type = get_fund_tax_type(isin) if isin else 'equity'
        lots = compute_unrealized_gains(folio['folio_id'], current_nav)

        # Validate FIFO lot sum vs holding units
        lot_units_sum = sum(l['units'] for l in lots)
        holding_units = folio['holding_units'] or 0
        if holding_units > 0 and abs(lot_units_sum - holding_units) / holding_units > 0.01:
            warnings.append(
                f"{folio['display_name']}: FIFO lots ({lot_units_sum:.4f}) diverge "
                f"from holding ({holding_units:.4f}) by "
                f"{abs(lot_units_sum - holding_units) / holding_units * 100:.1f}%"
            )

        # Only process loss lots
        loss_lots = [l for l in lots if l['unrealized_gain'] < -0.01]
        if not loss_lots:
            continue

        exit_load_pct = folio['exit_load_pct']

        for lot in loss_lots:
            loss = abs(lot['unrealized_gain'])

            # Determine tax rate
            if tax_type == 'equity':
                tax_rate = 0.125 if lot['is_long_term'] else 0.20
            else:
                tax_rate = tax_slab_pct / 100.0

            tax_savings = loss * tax_rate

            # Costs
            cv = lot['current_value']
            exit_load = cv * (exit_load_pct / 100.0) if lot['holding_days'] < 365 else 0
            stt = cv * 0.001 if tax_type == 'equity' else 0
            stamp_duty = cv * 0.00005
            total_costs = exit_load + stt + stamp_duty

            net_benefit = tax_savings - total_costs
            if net_benefit <= 0:
                continue

            # Urgency: equity lot approaching 12-month mark
            urgent = False
            urgency_days_remaining = None
            if tax_type == 'equity' and not lot['is_long_term'] and lot['holding_days'] >= 300:
                urgent = True
                urgency_days_remaining = 365 - lot['holding_days']

            opportunities.append({
                'folio_id': folio['folio_id'],
                'mf_id': folio['mf_id'],
                'isin': isin,
                'fund_name': folio['display_name'],
                'amc': folio['amc'],
                'lot_date': lot['date'],
                'lot_units': round(lot['units'], 4),
                'lot_cost': round(lot['cost'], 2),
                'current_nav': current_nav,
                'current_value': cv,
                'unrealized_loss': round(-lot['unrealized_gain'], 2),
                'holding_days': lot['holding_days'],
                'is_long_term': lot['is_long_term'],
                'gain_type': lot['gain_type'],
                'tax_type': tax_type,
                'tax_rate': round(tax_rate * 100, 1),
                'tax_savings': round(tax_savings, 2),
                'exit_load': round(exit_load, 2),
                'exit_load_pct': exit_load_pct,
                'stt': round(stt, 2),
                'stamp_duty': round(stamp_duty, 2),
                'total_costs': round(total_costs, 2),
                'net_benefit': round(net_benefit, 2),
                'urgent': urgent,
                'urgency_days_remaining': urgency_days_remaining,
                'similar_funds': get_similar_funds(isin) if isin else [],
            })

            total_unrealized_loss += loss
            total_tax_savings += tax_savings
            total_net_benefit += net_benefit

    # Sort: urgent first, then by net benefit descending
    opportunities.sort(key=lambda x: (not x['urgent'], -x['net_benefit']))

    realized = compute_realized_gains_fy(investor_id)
    urgent_count = sum(1 for o in opportunities if o['urgent'])

    return {
        'summary': {
            'total_unrealized_loss': round(total_unrealized_loss, 2),
            'total_tax_savings': round(total_tax_savings, 2),
            'total_net_benefit': round(total_net_benefit, 2),
            'opportunity_count': len(opportunities),
            'urgent_count': urgent_count,
            'tax_slab_pct': tax_slab_pct,
        },
        'opportunities': opportunities,
        'realized_gains': realized,
        'warnings': warnings,
    }


def update_investor_tax_slab(investor_id: int, tax_slab_pct: float) -> dict:
    """Update an investor's income tax slab percentage."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE investors SET tax_slab_pct = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (tax_slab_pct, investor_id))
        if cursor.rowcount > 0:
            return {'success': True}
        return {'success': False, 'error': 'Investor not found'}


def update_fund_exit_load(mf_id: int, exit_load_pct: float) -> dict:
    """Update exit load percentage for a mutual fund."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master SET exit_load_pct = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (exit_load_pct, mf_id))
        if cursor.rowcount > 0:
            return {'success': True}
        return {'success': False, 'error': 'Fund not found'}


def confirm_fund_allocation_review(mf_id: int) -> bool:
    """Mark a fund's allocation as reviewed (resets the 30-day timer)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master
            SET allocation_reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (mf_id,))
        return cursor.rowcount > 0


def get_funds_needing_review(days: int = 30) -> list:
    """Get funds whose allocation hasn't been reviewed in the given number of days."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, scheme_name, display_name, amfi_scheme_name, isin, amc,
                   allocation_reviewed_at,
                   (equity_pct + debt_pct + commodity_pct + cash_pct + others_pct) as alloc_sum
            FROM mutual_fund_master
            WHERE (equity_pct + debt_pct + commodity_pct + cash_pct + others_pct) >= 1
              AND (allocation_reviewed_at IS NULL
                   OR allocation_reviewed_at < datetime('now', ? || ' days'))
            ORDER BY allocation_reviewed_at ASC NULLS FIRST
        """, (f'-{days}',))
        return [dict(row) for row in cursor.fetchall()]


def get_portfolio_asset_allocation(investor_id: int) -> dict:
    """
    Calculate portfolio-level asset allocation based on fund-level splits.

    Returns weighted allocation across all holdings.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get holdings with current value and fund asset allocation
        cursor.execute("""
            SELECT
                h.units,
                COALESCE(mf.current_nav, h.nav) as nav,
                h.units * COALESCE(mf.current_nav, h.nav) as value,
                COALESCE(mf.equity_pct, 0) as equity_pct,
                COALESCE(mf.debt_pct, 0) as debt_pct,
                COALESCE(mf.commodity_pct, 0) as commodity_pct,
                COALESCE(mf.cash_pct, 0) as cash_pct,
                COALESCE(mf.others_pct, 0) as others_pct,
                COALESCE(mf.large_cap_pct, 0) as large_cap_pct,
                COALESCE(mf.mid_cap_pct, 0) as mid_cap_pct,
                COALESCE(mf.small_cap_pct, 0) as small_cap_pct,
                f.scheme_name
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
        """, (investor_id,))

        holdings = cursor.fetchall()

        total_value = 0
        equity_value = 0
        debt_value = 0
        commodity_value = 0
        cash_value = 0
        others_value = 0
        unallocated_value = 0
        large_cap_value = 0
        mid_cap_value = 0
        small_cap_value = 0

        holdings_detail = []

        for h in holdings:
            value = h['value'] or 0
            total_value += value

            # Check if allocation is defined (sums to ~100)
            alloc_sum = h['equity_pct'] + h['debt_pct'] + h['commodity_pct'] + h['cash_pct'] + h['others_pct']

            if alloc_sum < 1:  # Not defined
                unallocated_value += value
            else:
                fund_equity_value = value * h['equity_pct'] / 100
                equity_value += fund_equity_value
                debt_value += value * h['debt_pct'] / 100
                commodity_value += value * h['commodity_pct'] / 100
                cash_value += value * h['cash_pct'] / 100
                others_value += value * h['others_pct'] / 100

                # Market cap breakdown of equity portion
                cap_sum = h['large_cap_pct'] + h['mid_cap_pct'] + h['small_cap_pct']
                if cap_sum >= 1 and fund_equity_value > 0:
                    large_cap_value += fund_equity_value * h['large_cap_pct'] / 100
                    mid_cap_value += fund_equity_value * h['mid_cap_pct'] / 100
                    small_cap_value += fund_equity_value * h['small_cap_pct'] / 100

            holdings_detail.append({
                'scheme_name': h['scheme_name'],
                'value': value,
                'equity_pct': h['equity_pct'],
                'debt_pct': h['debt_pct'],
                'commodity_pct': h['commodity_pct'],
                'cash_pct': h['cash_pct'],
                'others_pct': h['others_pct'],
                'large_cap_pct': h['large_cap_pct'],
                'mid_cap_pct': h['mid_cap_pct'],
                'small_cap_pct': h['small_cap_pct'],
                'has_allocation': alloc_sum >= 1
            })

        # Count funds without allocation
        funds_without_allocation = len([h for h in holdings_detail if not h['has_allocation']])

        return {
            'total_value': total_value,
            'breakdown': {
                'equity': equity_value,
                'debt': debt_value,
                'commodity': commodity_value,
                'cash': cash_value,
                'others': others_value
            },
            'allocation': {
                'equity': {'value': equity_value, 'pct': (equity_value / total_value * 100) if total_value > 0 else 0},
                'debt': {'value': debt_value, 'pct': (debt_value / total_value * 100) if total_value > 0 else 0},
                'commodity': {'value': commodity_value, 'pct': (commodity_value / total_value * 100) if total_value > 0 else 0},
                'cash': {'value': cash_value, 'pct': (cash_value / total_value * 100) if total_value > 0 else 0},
                'others': {'value': others_value, 'pct': (others_value / total_value * 100) if total_value > 0 else 0},
                'unallocated': {'value': unallocated_value, 'pct': (unallocated_value / total_value * 100) if total_value > 0 else 0}
            },
            'market_cap': {
                'large': {'value': large_cap_value, 'pct': (large_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'mid': {'value': mid_cap_value, 'pct': (mid_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'small': {'value': small_cap_value, 'pct': (small_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'total_equity': equity_value
            },
            'funds_without_allocation': funds_without_allocation,
            'holdings': holdings_detail
        }
