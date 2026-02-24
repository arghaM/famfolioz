"""Manual asset management (FD, SGB, stocks, PPF) with calculations."""

import json
import logging
import math
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "calculate_fd_value",
    "calculate_fd_premature_value",
    "calculate_sgb_value",
    "calculate_ppf_value",
    "create_manual_asset",
    "update_manual_asset",
    "delete_manual_asset",
    "get_manual_asset",
    "get_manual_assets_by_investor",
    "get_manual_assets_summary",
    "get_maturing_fds",
    "get_matured_fds",
    "close_fd",
    "import_fd_csv",
    "get_combined_portfolio_value",
    "get_manual_asset_xirr_data",
    "get_manual_asset_types",
    "set_manual_asset_types",
    "get_ppf_epf_assets",
    "create_ppf_epf_asset",
    "get_asset_transactions",
    "add_asset_transaction",
    "delete_asset_transaction",
    "get_gold_lots",
    "create_gold_lot",
    "add_asset_price",
    "get_asset_prices",
    "get_latest_asset_price",
]


def calculate_fd_value(principal: float, interest_rate: float, tenure_months: int,
                       compounding: str, start_date: str, as_of_date: str = None) -> dict:
    """
    Calculate FD current value and maturity details.

    Args:
        principal: Principal amount
        interest_rate: Annual interest rate (e.g., 7.5 for 7.5%)
        tenure_months: Tenure in months
        compounding: 'monthly', 'quarterly', 'half_yearly', 'yearly'
        start_date: FD start date (YYYY-MM-DD)
        as_of_date: Calculate value as of this date (default: today)

    Returns:
        Dict with current_value, maturity_value, interest_earned, days_completed, etc.
    """
    from datetime import datetime, date as date_type
    from dateutil.relativedelta import relativedelta

    if not as_of_date:
        as_of_date = date_type.today().strftime('%Y-%m-%d')

    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    today = datetime.strptime(as_of_date, '%Y-%m-%d').date()
    maturity_date = start + relativedelta(months=tenure_months)

    days_completed = (today - start).days
    total_days = (maturity_date - start).days

    # Compounding frequency
    freq_map = {'monthly': 12, 'quarterly': 4, 'half_yearly': 2, 'yearly': 1}
    n = freq_map.get(compounding, 4)  # Default quarterly

    r = interest_rate / 100
    t_years = tenure_months / 12

    # Maturity value with compound interest: A = P(1 + r/n)^(nt)
    maturity_value = principal * ((1 + r/n) ** (n * t_years))

    # Current value (prorated based on days completed)
    if today >= maturity_date:
        current_value = maturity_value
        status = 'matured'
    else:
        elapsed_years = days_completed / 365
        current_value = principal * ((1 + r/n) ** (n * elapsed_years))
        status = 'active'

    interest_earned = current_value - principal
    maturity_interest = maturity_value - principal

    return {
        'principal': principal,
        'current_value': round(current_value, 2),
        'maturity_value': round(maturity_value, 2),
        'interest_earned': round(interest_earned, 2),
        'maturity_interest': round(maturity_interest, 2),
        'days_completed': days_completed,
        'total_days': total_days,
        'maturity_date': maturity_date.strftime('%Y-%m-%d'),
        'status': status,
        'progress_pct': min(100, round(days_completed / total_days * 100, 1)) if total_days > 0 else 0
    }


def calculate_fd_premature_value(principal: float, interest_rate: float,
                                  premature_penalty_pct: float, start_date: str,
                                  compounding: str = 'quarterly') -> dict:
    """
    Calculate FD value if broken today with premature withdrawal penalty.

    Args:
        principal: Principal amount
        interest_rate: Annual interest rate
        premature_penalty_pct: Penalty on interest rate (e.g., 1.0 for 1%)
        start_date: FD start date
        compounding: Compounding frequency

    Returns:
        Dict with premature_value, penalty_amount, effective_rate, etc.
    """
    from datetime import datetime, date as date_type

    today = date_type.today()
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    days_completed = (today - start).days

    if days_completed <= 0:
        return {
            'premature_value': principal,
            'penalty_amount': 0,
            'effective_rate': 0,
            'message': 'FD not yet started'
        }

    # Effective rate after penalty
    effective_rate = max(0, interest_rate - premature_penalty_pct)

    freq_map = {'monthly': 12, 'quarterly': 4, 'half_yearly': 2, 'yearly': 1}
    n = freq_map.get(compounding, 4)

    elapsed_years = days_completed / 365
    r = effective_rate / 100

    # Value with penalized rate
    premature_value = principal * ((1 + r/n) ** (n * elapsed_years))

    # Calculate what it would be without penalty
    r_full = interest_rate / 100
    full_value = principal * ((1 + r_full/n) ** (n * elapsed_years))

    penalty_amount = full_value - premature_value

    return {
        'premature_value': round(premature_value, 2),
        'penalty_amount': round(penalty_amount, 2),
        'effective_rate': effective_rate,
        'original_rate': interest_rate,
        'days_completed': days_completed,
        'interest_earned': round(premature_value - principal, 2)
    }


def calculate_sgb_value(issue_price: float, grams: float, interest_rate: float,
                        purchase_date: str, current_gold_price: float = None) -> dict:
    """
    Calculate SGB current value including gold appreciation and interest.

    Args:
        issue_price: SGB issue price per gram
        grams: Number of grams
        interest_rate: Annual interest rate (typically 2.5%)
        purchase_date: Purchase date
        current_gold_price: Current gold price per gram (if None, uses issue price)

    Returns:
        Dict with current_value, gold_value, interest_earned, appreciation, etc.
    """
    from datetime import datetime, date as date_type

    today = date_type.today()
    purchase = datetime.strptime(purchase_date, '%Y-%m-%d').date()
    days_held = (today - purchase).days

    if current_gold_price is None:
        current_gold_price = issue_price

    # Principal (face value)
    principal = issue_price * grams

    # Current gold value
    gold_value = current_gold_price * grams
    appreciation = gold_value - principal

    # Interest earned (simple interest, paid semi-annually)
    years_held = days_held / 365
    interest_earned = principal * (interest_rate / 100) * years_held

    # Total current value
    current_value = gold_value + interest_earned

    return {
        'principal': round(principal, 2),
        'current_value': round(current_value, 2),
        'gold_value': round(gold_value, 2),
        'appreciation': round(appreciation, 2),
        'appreciation_pct': round((appreciation / principal) * 100, 2) if principal > 0 else 0,
        'interest_earned': round(interest_earned, 2),
        'grams': grams,
        'issue_price': issue_price,
        'current_gold_price': current_gold_price,
        'days_held': days_held,
        'years_held': round(years_held, 2)
    }


def calculate_ppf_value(opening_balance: float, interest_rate: float,
                        purchase_date: str, purchase_value: float = 0) -> dict:
    """
    Calculate PPF current value with yearly compound interest.

    Args:
        opening_balance: Balance at account opening (before any deposits in this system)
        interest_rate: Annual interest rate (e.g., 7.1 for 7.1%)
        purchase_date: PPF account/deposit date (YYYY-MM-DD)
        purchase_value: Total deposits made (purchase_value from manual_assets)

    Returns:
        Dict with current_value, interest_earned, years_elapsed
    """
    from datetime import datetime, date as date_type

    today = date_type.today()
    start = datetime.strptime(purchase_date, '%Y-%m-%d').date()
    days_elapsed = (today - start).days

    if days_elapsed <= 0:
        total_principal = opening_balance + purchase_value
        return {
            'current_value': round(total_principal, 2),
            'interest_earned': 0,
            'years_elapsed': 0,
            'total_principal': round(total_principal, 2),
        }

    years_elapsed = days_elapsed / 365
    r = interest_rate / 100

    # Compound interest on opening balance (yearly compounding)
    opening_grown = opening_balance * ((1 + r) ** years_elapsed)

    # Compound interest on deposits (simplified: treat as lump sum from purchase_date)
    deposit_grown = purchase_value * ((1 + r) ** years_elapsed)

    current_value = opening_grown + deposit_grown
    total_principal = opening_balance + purchase_value
    interest_earned = current_value - total_principal

    return {
        'current_value': round(current_value, 2),
        'interest_earned': round(interest_earned, 2),
        'years_elapsed': round(years_elapsed, 2),
        'total_principal': round(total_principal, 2),
    }


def create_manual_asset(investor_id: int, asset_type: str, asset_class: str,
                        name: str, **kwargs) -> int:
    """
    Create a manual asset entry.

    Args:
        investor_id: The investor ID
        asset_type: Type of asset ('fd', 'sgb', 'stock', 'ppf', 'nps', 'other')
        asset_class: Asset class ('equity', 'debt', 'commodity', 'cash', 'others')
        name: Asset name
        **kwargs: Additional fields based on asset_type

    Returns:
        The created asset ID
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Base fields
        fields = ['investor_id', 'asset_type', 'asset_class', 'name']
        values = [investor_id, asset_type, asset_class, name]

        # Optional base fields
        optional_base = ['description', 'purchase_date', 'purchase_value', 'units',
                         'current_nav', 'current_value']
        for field in optional_base:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # FD specific fields
        fd_fields = ['fd_principal', 'fd_interest_rate', 'fd_tenure_months',
                     'fd_maturity_date', 'fd_compounding', 'fd_premature_penalty_pct',
                     'fd_bank_name']
        for field in fd_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # SGB specific fields
        sgb_fields = ['sgb_issue_price', 'sgb_interest_rate', 'sgb_maturity_date', 'sgb_grams']
        for field in sgb_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # Stock specific fields
        stock_fields = ['stock_symbol', 'stock_exchange', 'stock_quantity', 'stock_avg_price']
        for field in stock_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # PPF/NPS fields
        ppf_fields = ['ppf_account_number', 'ppf_maturity_date',
                      'ppf_interest_rate', 'ppf_compounding', 'ppf_opening_balance']
        for field in ppf_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # Gold specific fields
        gold_fields = ['gold_ref_no', 'gold_seller', 'gold_broker']
        for field in gold_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # Allocation fields
        alloc_fields = ['equity_pct', 'debt_pct', 'commodity_pct', 'cash_pct',
                        'others_pct', 'exclude_from_xirr']
        for field in alloc_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        placeholders = ', '.join(['?' for _ in fields])
        field_names = ', '.join(fields)

        cursor.execute(f"""
            INSERT INTO manual_assets ({field_names})
            VALUES ({placeholders})
        """, values)

        return cursor.lastrowid


def update_manual_asset(asset_id: int, **kwargs) -> dict:
    """Update a manual asset."""
    with get_db() as conn:
        cursor = conn.cursor()

        updates = []
        values = []

        # All updateable fields
        all_fields = [
            'name', 'description', 'asset_class', 'purchase_date', 'purchase_value',
            'units', 'current_nav', 'current_value', 'is_active', 'matured_on',
            'fd_principal', 'fd_interest_rate', 'fd_tenure_months', 'fd_maturity_date',
            'fd_compounding', 'fd_premature_penalty_pct', 'fd_bank_name',
            'sgb_issue_price', 'sgb_interest_rate', 'sgb_maturity_date', 'sgb_grams',
            'stock_symbol', 'stock_exchange', 'stock_quantity', 'stock_avg_price',
            'ppf_account_number', 'ppf_maturity_date',
            'ppf_interest_rate', 'ppf_compounding', 'ppf_opening_balance',
            'gold_ref_no', 'gold_seller', 'gold_broker',
            'equity_pct', 'debt_pct', 'commodity_pct', 'cash_pct', 'others_pct',
            'exclude_from_xirr',
        ]

        for field in all_fields:
            if field in kwargs:
                updates.append(f"{field} = ?")
                values.append(kwargs[field])

        if not updates:
            return {'success': False, 'error': 'No fields to update'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(asset_id)

        cursor.execute(f"""
            UPDATE manual_assets SET {', '.join(updates)}
            WHERE id = ?
        """, values)

        return {'success': cursor.rowcount > 0}


def delete_manual_asset(asset_id: int) -> dict:
    """Delete a manual asset."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM manual_assets WHERE id = ?", (asset_id,))
        return {'success': cursor.rowcount > 0}


def get_manual_asset(asset_id: int) -> Optional[dict]:
    """Get a manual asset by ID with calculated values."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM manual_assets WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        if not row:
            return None

        asset = dict(row)
        return _enrich_manual_asset(asset)


def get_manual_assets_by_investor(investor_id: int, include_inactive: bool = False) -> List[dict]:
    """Get all manual assets for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()

        if include_inactive:
            cursor.execute("""
                SELECT * FROM manual_assets
                WHERE investor_id = ?
                ORDER BY asset_type, name
            """, (investor_id,))
        else:
            cursor.execute("""
                SELECT * FROM manual_assets
                WHERE investor_id = ? AND is_active = 1
                ORDER BY asset_type, name
            """, (investor_id,))

        assets = []
        for row in cursor.fetchall():
            asset = dict(row)
            assets.append(_enrich_manual_asset(asset))

        return assets


def _derive_allocation_from_class(asset: dict) -> dict:
    """If all allocation percentages are zero, derive from asset_class."""
    alloc_sum = (
        (asset.get('equity_pct') or 0) +
        (asset.get('debt_pct') or 0) +
        (asset.get('commodity_pct') or 0) +
        (asset.get('cash_pct') or 0) +
        (asset.get('others_pct') or 0)
    )
    if alloc_sum < 1:
        # Map asset_class to 100% in the matching allocation field
        class_to_field = {
            'equity': 'equity_pct',
            'debt': 'debt_pct',
            'commodity': 'commodity_pct',
            'cash': 'cash_pct',
            'others': 'others_pct',
        }
        asset_class = asset.get('asset_class', 'others')
        field = class_to_field.get(asset_class, 'others_pct')
        asset[field] = 100
    return asset


def _enrich_manual_asset(asset: dict) -> dict:
    """Add calculated values to a manual asset based on its type."""
    asset_type = asset.get('asset_type', '')

    if asset_type == 'fd' and asset.get('fd_principal'):
        # Calculate FD current value
        if asset.get('purchase_date') and asset.get('fd_interest_rate') and asset.get('fd_tenure_months'):
            fd_calc = calculate_fd_value(
                principal=asset['fd_principal'],
                interest_rate=asset['fd_interest_rate'],
                tenure_months=asset['fd_tenure_months'],
                compounding=asset.get('fd_compounding', 'quarterly'),
                start_date=asset['purchase_date']
            )
            asset['calculated_value'] = fd_calc['current_value']
            asset['fd_details'] = fd_calc

            # Also calculate premature value
            if asset.get('fd_premature_penalty_pct') is not None:
                premature = calculate_fd_premature_value(
                    principal=asset['fd_principal'],
                    interest_rate=asset['fd_interest_rate'],
                    premature_penalty_pct=asset.get('fd_premature_penalty_pct', 1.0),
                    start_date=asset['purchase_date'],
                    compounding=asset.get('fd_compounding', 'quarterly')
                )
                asset['premature_details'] = premature

    elif asset_type == 'ppf' and asset.get('purchase_date'):
        # Calculate PPF current value
        ppf_calc = calculate_ppf_value(
            opening_balance=asset.get('ppf_opening_balance') or 0,
            interest_rate=asset.get('ppf_interest_rate') or 7.1,
            purchase_date=asset['purchase_date'],
            purchase_value=asset.get('purchase_value') or 0,
        )
        asset['calculated_value'] = ppf_calc['current_value']
        asset['ppf_details'] = ppf_calc

    elif asset_type == 'sgb' and asset.get('sgb_grams'):
        # Calculate SGB current value
        if asset.get('purchase_date') and asset.get('sgb_issue_price'):
            sgb_calc = calculate_sgb_value(
                issue_price=asset['sgb_issue_price'],
                grams=asset['sgb_grams'],
                interest_rate=asset.get('sgb_interest_rate', 2.5),
                purchase_date=asset['purchase_date'],
                current_gold_price=asset.get('current_nav')  # Use current_nav for current gold price
            )
            asset['calculated_value'] = sgb_calc['current_value']
            asset['sgb_details'] = sgb_calc

    elif asset_type == 'stock' and asset.get('stock_quantity'):
        # Calculate stock current value
        quantity = asset['stock_quantity']
        current_price = asset.get('current_nav', asset.get('stock_avg_price', 0))
        avg_price = asset.get('stock_avg_price', 0)

        current_value = quantity * current_price
        invested_value = quantity * avg_price

        asset['calculated_value'] = current_value
        asset['stock_details'] = {
            'quantity': quantity,
            'avg_price': avg_price,
            'current_price': current_price,
            'invested_value': invested_value,
            'current_value': current_value,
            'gain_loss': current_value - invested_value,
            'gain_loss_pct': ((current_value - invested_value) / invested_value * 100) if invested_value > 0 else 0
        }

    elif asset_type == 'gold':
        units = asset.get('units') or 0
        nav = asset.get('current_nav')  # latest manually-set price/gram
        if units > 0 and nav:
            market_value = round(units * nav, 2)
            asset['calculated_value'] = market_value
            asset['gold_details'] = {
                'grams': units,
                'price_per_gram': nav,
                'market_value': market_value,
                'invested': asset.get('purchase_value') or 0,
            }
        else:
            # No price set -> value = cost basis from transactions
            asset['calculated_value'] = asset.get('current_value') or asset.get('purchase_value') or 0

    else:
        # For other types, use stored current_value or calculate from units * current_nav
        if asset.get('current_value'):
            asset['calculated_value'] = asset['current_value']
        elif asset.get('units') and asset.get('current_nav'):
            asset['calculated_value'] = asset['units'] * asset['current_nav']
        else:
            asset['calculated_value'] = asset.get('purchase_value', 0)

    # Derive allocation from asset_class if not explicitly set
    _derive_allocation_from_class(asset)

    return asset


def get_manual_assets_summary(investor_id: int) -> dict:
    """Get summary of manual assets by asset class."""
    assets = get_manual_assets_by_investor(investor_id)

    summary = {
        'total_value': 0,
        'by_asset_class': {
            'equity': {'count': 0, 'value': 0, 'assets': []},
            'debt': {'count': 0, 'value': 0, 'assets': []},
            'commodity': {'count': 0, 'value': 0, 'assets': []},
            'cash': {'count': 0, 'value': 0, 'assets': []},
            'others': {'count': 0, 'value': 0, 'assets': []}
        },
        'by_asset_type': {}
    }

    for asset in assets:
        value = asset.get('calculated_value', 0) or 0
        asset_class = asset.get('asset_class', 'others')
        asset_type = asset.get('asset_type', 'other')

        summary['total_value'] += value

        if asset_class in summary['by_asset_class']:
            summary['by_asset_class'][asset_class]['count'] += 1
            summary['by_asset_class'][asset_class]['value'] += value
            summary['by_asset_class'][asset_class]['assets'].append({
                'id': asset['id'],
                'name': asset['name'],
                'value': value
            })

        if asset_type not in summary['by_asset_type']:
            summary['by_asset_type'][asset_type] = {'count': 0, 'value': 0}
        summary['by_asset_type'][asset_type]['count'] += 1
        summary['by_asset_type'][asset_type]['value'] += value

    return summary


def get_maturing_fds(days: int = 30) -> list:
    """Get FDs maturing within the next N days."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*, i.name as investor_name, i.pan as investor_pan
            FROM manual_assets ma
            JOIN investors i ON ma.investor_id = i.id
            WHERE ma.asset_type = 'fd'
              AND ma.is_active = 1
              AND (ma.fd_status IS NULL OR ma.fd_status = 'active')
              AND ma.fd_maturity_date IS NOT NULL
              AND ma.fd_maturity_date > date('now')
              AND ma.fd_maturity_date <= date('now', '+' || ? || ' days')
            ORDER BY ma.fd_maturity_date ASC
        """, (days,))
        rows = cursor.fetchall()
        return [_enrich_manual_asset({**dict(row)}) for row in rows]


def get_matured_fds() -> list:
    """Get FDs past maturity that aren't closed."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*, i.name as investor_name, i.pan as investor_pan
            FROM manual_assets ma
            JOIN investors i ON ma.investor_id = i.id
            WHERE ma.asset_type = 'fd'
              AND ma.is_active = 1
              AND (ma.fd_status IS NULL OR ma.fd_status IN ('active', 'matured'))
              AND ma.fd_maturity_date IS NOT NULL
              AND ma.fd_maturity_date <= date('now')
            ORDER BY ma.fd_maturity_date ASC
        """)
        rows = cursor.fetchall()
        return [_enrich_manual_asset({**dict(row)}) for row in rows]


def close_fd(asset_id: int, money_received: bool = True) -> bool:
    """Mark FD as closed with money received status."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE manual_assets
            SET fd_status = 'closed',
                fd_closed_date = date('now'),
                fd_money_received = ?,
                is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND asset_type = 'fd'
        """, (1 if money_received else 0, asset_id))
        return cursor.rowcount > 0


def import_fd_csv(investor_id: int, fd_rows: list) -> dict:
    """
    Import FDs from parsed CSV rows.

    Args:
        investor_id: The investor to associate FDs with
        fd_rows: List of dicts with keys: name, deposit_date, roi, tenor,
                 maturity_date, maturity_amount, balance

    Returns:
        dict with created count and any errors
    """
    created = 0
    errors = []

    for i, row in enumerate(fd_rows):
        try:
            # Calculate tenure from deposit date and maturity date
            deposit_date = row.get('deposit_date')
            maturity_date = row.get('maturity_date')
            tenure_months = 12  # default

            if deposit_date and maturity_date:
                try:
                    from datetime import datetime
                    # Parse dates
                    if isinstance(deposit_date, str):
                        dep_dt = datetime.strptime(deposit_date, '%Y-%m-%d')
                    else:
                        dep_dt = deposit_date

                    if isinstance(maturity_date, str):
                        mat_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
                    else:
                        mat_dt = maturity_date

                    # Calculate months difference
                    months = (mat_dt.year - dep_dt.year) * 12 + (mat_dt.month - dep_dt.month)
                    # Add 1 if maturity day >= deposit day (round up partial months)
                    if mat_dt.day >= dep_dt.day:
                        tenure_months = max(1, months)
                    else:
                        tenure_months = max(1, months)
                except Exception:
                    tenure_months = 12  # fallback

            # Parse principal (balance field)
            principal = row.get('balance', 0)
            if isinstance(principal, str):
                principal = float(principal.replace(',', '').strip())

            # Parse interest rate
            roi = row.get('roi', 7.0)
            if isinstance(roi, str):
                roi = float(roi.replace('%', '').strip())

            # Create the FD
            create_manual_asset(
                investor_id=investor_id,
                asset_type='fd',
                asset_class='debt',
                name=row.get('name', f'FD-{i+1}'),
                description=f"FD imported from CSV",
                purchase_date=row.get('deposit_date'),
                purchase_value=principal,
                fd_principal=principal,
                fd_interest_rate=roi,
                fd_tenure_months=tenure_months,
                fd_maturity_date=row.get('maturity_date'),
                fd_compounding='quarterly',
                fd_bank_name=row.get('bank_name', '')
            )
            created += 1

        except Exception as e:
            errors.append(f"Row {i+1}: {str(e)}")

    return {
        'created': created,
        'errors': errors,
        'total_rows': len(fd_rows)
    }


def get_combined_portfolio_value(investor_id: int) -> dict:
    """
    Get combined portfolio value including mutual funds and manual assets.

    Returns breakdown by asset class across both types.
    """
    from cas_parser.webapp.db.tax import get_portfolio_asset_allocation

    # Get mutual fund allocation
    mf_allocation = get_portfolio_asset_allocation(investor_id)

    # Get manual assets summary
    manual_summary = get_manual_assets_summary(investor_id)

    # Combine
    combined = {
        'mutual_funds_value': mf_allocation['total_value'],
        'manual_assets_value': manual_summary['total_value'],
        'total_value': mf_allocation['total_value'] + manual_summary['total_value'],
        'by_asset_class': {
            'equity': {
                'mf_value': mf_allocation['breakdown']['equity'],
                'manual_value': manual_summary['by_asset_class']['equity']['value'],
                'total': mf_allocation['breakdown']['equity'] + manual_summary['by_asset_class']['equity']['value']
            },
            'debt': {
                'mf_value': mf_allocation['breakdown']['debt'],
                'manual_value': manual_summary['by_asset_class']['debt']['value'],
                'total': mf_allocation['breakdown']['debt'] + manual_summary['by_asset_class']['debt']['value']
            },
            'commodity': {
                'mf_value': mf_allocation['breakdown']['commodity'],
                'manual_value': manual_summary['by_asset_class']['commodity']['value'],
                'total': mf_allocation['breakdown']['commodity'] + manual_summary['by_asset_class']['commodity']['value']
            },
            'cash': {
                'mf_value': mf_allocation['breakdown']['cash'],
                'manual_value': manual_summary['by_asset_class']['cash']['value'],
                'total': mf_allocation['breakdown']['cash'] + manual_summary['by_asset_class']['cash']['value']
            },
            'others': {
                'mf_value': mf_allocation['breakdown']['others'],
                'manual_value': manual_summary['by_asset_class']['others']['value'],
                'total': mf_allocation['breakdown']['others'] + manual_summary['by_asset_class']['others']['value']
            }
        },
        'manual_assets_by_type': manual_summary['by_asset_type']
    }

    # Calculate percentages
    total = combined['total_value']
    if total > 0:
        for cls in combined['by_asset_class']:
            combined['by_asset_class'][cls]['pct'] = round(
                combined['by_asset_class'][cls]['total'] / total * 100, 2
            )
    else:
        for cls in combined['by_asset_class']:
            combined['by_asset_class'][cls]['pct'] = 0

    return combined


# ---------------------------------------------------------------------------
# Asset type configuration (stored in app_config as JSON)
# ---------------------------------------------------------------------------

_DEFAULT_ASSET_TYPES = [
    {"key": "fd",          "label": "Fixed Deposit (FD)",  "icon": "bi-bank",            "status": "coming_soon"},
    {"key": "ppf_epf",     "label": "PPF / EPF",          "icon": "bi-piggy-bank",      "status": "active"},
    {"key": "gold",        "label": "Gold",               "icon": "bi-gem",             "status": "active"},
    {"key": "silver",      "label": "Silver",             "icon": "bi-gem",             "status": "coming_soon"},
    {"key": "post_office", "label": "Post Office",        "icon": "bi-mailbox",         "status": "coming_soon"},
    {"key": "insurance",   "label": "Insurance",          "icon": "bi-shield-check",    "status": "coming_soon"},
    {"key": "stocks",      "label": "Stocks",             "icon": "bi-graph-up-arrow",  "status": "coming_soon"},
    {"key": "loan",        "label": "Loan",               "icon": "bi-cash-coin",       "status": "coming_soon"},
    {"key": "jewellery",   "label": "Jewellery",          "icon": "bi-diamond",         "status": "coming_soon"},
]


def get_manual_asset_types() -> list:
    """Read manual asset types from app_config. Returns list of dicts."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM app_config WHERE key = 'manual_asset_types'")
        row = cursor.fetchone()
        if row and row['value']:
            try:
                return json.loads(row['value'])
            except (json.JSONDecodeError, TypeError):
                pass
        return _DEFAULT_ASSET_TYPES


def set_manual_asset_types(types: list) -> dict:
    """Save manual asset types list to app_config."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO app_config (key, value, updated_at)
            VALUES ('manual_asset_types', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
        """, (json.dumps(types),))
        return {'success': True}


# ---------------------------------------------------------------------------
# PPF / EPF transaction-based tracking
# ---------------------------------------------------------------------------

def get_ppf_epf_assets(investor_id: int) -> list:
    """Get PPF/EPF assets for the LOV dropdown."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, asset_type, current_value, purchase_value
            FROM manual_assets
            WHERE investor_id = ? AND asset_type IN ('ppf', 'epf') AND is_active = 1
            ORDER BY name
        """, (investor_id,))
        return [dict(row) for row in cursor.fetchall()]


def create_ppf_epf_asset(investor_id: int, sub_type: str, bank: str = None,
                          ref_no: str = None) -> dict:
    """Create a new PPF/EPF master record.

    Args:
        investor_id: Investor ID
        sub_type: 'ppf' or 'epf'
        bank: Bank name (optional)
        ref_no: Account/reference number (optional)

    Returns:
        dict with id and name of the created asset
    """
    label = sub_type.upper()
    parts = [label]
    if bank:
        parts.append(bank)
    if ref_no:
        parts.append(f"({ref_no})")
    name = " - ".join(parts[:2])
    if ref_no:
        name = name + f" ({ref_no})" if len(parts) == 3 and bank else f"{label} ({ref_no})"
        # Recalculate cleanly
        if bank and ref_no:
            name = f"{label} - {bank} ({ref_no})"
        elif ref_no:
            name = f"{label} ({ref_no})"
        elif bank:
            name = f"{label} - {bank}"

    asset_id = create_manual_asset(
        investor_id=investor_id,
        asset_type=sub_type,
        asset_class='debt',
        name=name,
        purchase_value=0,
        current_value=0,
        ppf_account_number=ref_no,
        debt_pct=100,
    )

    return {'id': asset_id, 'name': name}


def get_asset_transactions(asset_id: int) -> list:
    """Get transactions for an asset ordered by date ASC with running balance and running quantity."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, asset_id, tx_type, tx_date, amount, narration, quantity, rate, created_at
            FROM manual_asset_transactions
            WHERE asset_id = ?
            ORDER BY tx_date ASC, id ASC
        """, (asset_id,))
        rows = [dict(row) for row in cursor.fetchall()]

    balance = 0
    qty_balance = 0
    for row in rows:
        if row['tx_type'] in ('investment', 'interest', 'buy'):
            balance += row['amount']
        else:  # withdrawal, sell
            balance -= row['amount']
        row['running_balance'] = round(balance, 2)

        # Track running quantity for gold/commodity assets
        q = row.get('quantity') or 0
        if row['tx_type'] == 'buy':
            qty_balance += q
        elif row['tx_type'] == 'sell':
            qty_balance -= q
        row['running_quantity'] = round(qty_balance, 4)

    return rows


def add_asset_transaction(asset_id: int, tx_type: str, tx_date: str,
                           amount: float, narration: str = None,
                           quantity: float = None, rate: float = None) -> dict:
    """Insert a transaction and recompute the asset value.

    Args:
        asset_id: The manual_assets.id
        tx_type: 'investment', 'interest', 'withdrawal', 'buy', or 'sell'
        tx_date: Date string (YYYY-MM-DD)
        amount: Transaction amount (positive)
        narration: Optional note
        quantity: Optional quantity (grams for gold)
        rate: Optional rate per unit

    Returns:
        dict with success and the new transaction id
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO manual_asset_transactions (asset_id, tx_type, tx_date, amount, narration, quantity, rate)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (asset_id, tx_type, tx_date, amount, narration, quantity, rate))
        tx_id = cursor.lastrowid
        _recompute_asset_value(asset_id, cursor)
        return {'success': True, 'id': tx_id}


def delete_asset_transaction(tx_id: int) -> dict:
    """Delete a transaction and recompute the asset value.

    Returns:
        dict with success and asset_id for convenience
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT asset_id FROM manual_asset_transactions WHERE id = ?", (tx_id,))
        row = cursor.fetchone()
        if not row:
            return {'success': False, 'error': 'Transaction not found'}
        asset_id = row['asset_id']
        cursor.execute("DELETE FROM manual_asset_transactions WHERE id = ?", (tx_id,))
        _recompute_asset_value(asset_id, cursor)
        return {'success': True, 'asset_id': asset_id}


def _recompute_asset_value(asset_id: int, cursor) -> None:
    """Recompute current_value and purchase_value from transaction sums."""
    cursor.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN tx_type = 'investment' THEN amount ELSE 0 END), 0) AS total_investment,
            COALESCE(SUM(CASE WHEN tx_type = 'interest' THEN amount ELSE 0 END), 0) AS total_interest,
            COALESCE(SUM(CASE WHEN tx_type = 'withdrawal' THEN amount ELSE 0 END), 0) AS total_withdrawal,
            COALESCE(SUM(CASE WHEN tx_type = 'buy' THEN amount ELSE 0 END), 0) AS total_buy,
            COALESCE(SUM(CASE WHEN tx_type = 'sell' THEN amount ELSE 0 END), 0) AS total_sell,
            COALESCE(SUM(CASE WHEN tx_type = 'buy' THEN quantity ELSE 0 END), 0) AS total_buy_qty,
            COALESCE(SUM(CASE WHEN tx_type = 'sell' THEN quantity ELSE 0 END), 0) AS total_sell_qty
        FROM manual_asset_transactions
        WHERE asset_id = ?
    """, (asset_id,))
    row = cursor.fetchone()
    total_investment = row['total_investment']
    total_interest = row['total_interest']
    total_withdrawal = row['total_withdrawal']
    total_buy = row['total_buy']
    total_sell = row['total_sell']
    total_buy_qty = row['total_buy_qty']
    total_sell_qty = row['total_sell_qty']

    current_value = (total_investment + total_interest - total_withdrawal
                     + total_buy - total_sell)
    purchase_value = total_investment + total_buy
    units = total_buy_qty - total_sell_qty

    cursor.execute("""
        UPDATE manual_assets
        SET current_value = ?, purchase_value = ?, units = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (round(current_value, 2), round(purchase_value, 2), round(units, 4), asset_id))


# ---------------------------------------------------------------------------
# Gold lot management
# ---------------------------------------------------------------------------

def get_gold_lots(investor_id: int) -> list:
    """Get gold lots for the LOV dropdown with calculated_value."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, asset_type, current_value, purchase_value,
                   gold_ref_no, gold_seller, gold_broker, units, current_nav
            FROM manual_assets
            WHERE investor_id = ? AND asset_type = 'gold' AND is_active = 1
            ORDER BY name
        """, (investor_id,))
        lots = []
        for row in cursor.fetchall():
            lot = dict(row)
            units = lot.get('units') or 0
            nav = lot.get('current_nav')
            if units > 0 and nav:
                lot['calculated_value'] = round(units * nav, 2)
            else:
                lot['calculated_value'] = lot.get('current_value') or lot.get('purchase_value') or 0
            lots.append(lot)
        return lots


def create_gold_lot(investor_id: int, name: str, ref_no: str = None,
                    seller: str = None, broker: str = None) -> dict:
    """Create a new gold lot (manual_asset of type gold).

    Args:
        investor_id: Investor ID
        name: Lot description (e.g. "Physical Gold Bar 24K")
        ref_no: Reference / invoice number
        seller: Seller name
        broker: Agent / broker name

    Returns:
        dict with id and name of the created asset
    """
    asset_id = create_manual_asset(
        investor_id=investor_id,
        asset_type='gold',
        asset_class='commodity',
        name=name,
        commodity_pct=100,
        gold_ref_no=ref_no,
        gold_seller=seller,
        gold_broker=broker,
        purchase_value=0,
        current_value=0,
    )
    return {'id': asset_id, 'name': name}


def get_manual_asset_xirr_data(investor_id: int) -> list:
    """Get XIRR-compatible cashflow data for all transaction-based manual assets.

    Returns a list of dicts, one per asset, each with:
        - asset_id, asset_name, asset_type
        - current_value
        - cashflows: list of (date_str, amount) where negative=outflow, positive=inflow
    """
    from datetime import date as date_type
    with get_db() as conn:
        cursor = conn.cursor()
        # Get all active manual assets that have transactions
        cursor.execute("""
            SELECT ma.id, ma.name, ma.asset_type, ma.current_value,
                   ma.units, ma.current_nav
            FROM manual_assets ma
            WHERE ma.investor_id = ? AND ma.is_active = 1
              AND EXISTS (
                  SELECT 1 FROM manual_asset_transactions mat WHERE mat.asset_id = ma.id
              )
        """, (investor_id,))
        assets = [dict(r) for r in cursor.fetchall()]

        results = []
        today_str = date_type.today().isoformat()
        for asset in assets:
            asset_id = asset['id']
            cursor.execute("""
                SELECT tx_type, tx_date, amount
                FROM manual_asset_transactions
                WHERE asset_id = ?
                ORDER BY tx_date ASC
            """, (asset_id,))
            txns = cursor.fetchall()

            cashflows = []
            for tx in txns:
                amt = float(tx['amount'])
                if amt == 0:
                    continue
                tx_type = tx['tx_type']
                if tx_type in ('investment', 'buy'):
                    cashflows.append((tx['tx_date'], -amt))  # outflow
                elif tx_type in ('withdrawal', 'sell'):
                    cashflows.append((tx['tx_date'], amt))    # inflow
                # interest is internal growth, not an external cashflow — skip

            # Terminal value: use market value (units × nav) for gold if price is set,
            # otherwise fall back to current_value (cost basis)
            units = float(asset.get('units') or 0)
            nav = asset.get('current_nav')
            if asset['asset_type'] == 'gold' and units > 0 and nav:
                cv = round(units * float(nav), 2)
            else:
                cv = float(asset['current_value'] or 0)
            if cv > 0:
                cashflows.append((today_str, cv))

            if cashflows:
                results.append({
                    'asset_id': asset['id'],
                    'asset_name': asset['name'],
                    'asset_type': asset['asset_type'],
                    'current_value': cv,
                    'cashflows': cashflows,
                })
        return results


# ---------------------------------------------------------------------------
# Manual asset price history (for gold price tracking)
# ---------------------------------------------------------------------------

def add_asset_price(asset_id: int, price_date: str, price_per_unit: float) -> dict:
    """Record a price for a manual asset and update current_nav.

    Uses INSERT OR REPLACE so only one price per date is kept.

    Args:
        asset_id: The manual_assets.id
        price_date: Date string (YYYY-MM-DD)
        price_per_unit: Price per unit (e.g. per gram for gold)

    Returns:
        dict with success and the row id
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO manual_asset_prices (asset_id, price_date, price_per_unit)
            VALUES (?, ?, ?)
            ON CONFLICT(asset_id, price_date)
            DO UPDATE SET price_per_unit = excluded.price_per_unit,
                          created_at = CURRENT_TIMESTAMP
        """, (asset_id, price_date, price_per_unit))
        row_id = cursor.lastrowid

        # Find the latest price date for this asset to decide whether to update current_nav
        cursor.execute("""
            SELECT price_per_unit FROM manual_asset_prices
            WHERE asset_id = ?
            ORDER BY price_date DESC LIMIT 1
        """, (asset_id,))
        latest = cursor.fetchone()
        if latest:
            cursor.execute("""
                UPDATE manual_assets
                SET current_nav = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (latest['price_per_unit'], asset_id))

        return {'success': True, 'id': row_id}


def get_asset_prices(asset_id: int, limit: int = 20) -> list:
    """Get price history for an asset, newest first.

    Args:
        asset_id: The manual_assets.id
        limit: Max rows to return (default 20)

    Returns:
        List of dicts with id, price_date, price_per_unit, created_at
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, price_date, price_per_unit, created_at
            FROM manual_asset_prices
            WHERE asset_id = ?
            ORDER BY price_date DESC
            LIMIT ?
        """, (asset_id, limit))
        return [dict(row) for row in cursor.fetchall()]


def get_latest_asset_price(asset_id: int) -> Optional[dict]:
    """Get the most recent price entry for an asset.

    Returns:
        dict with id, price_date, price_per_unit, created_at or None
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, price_date, price_per_unit, created_at
            FROM manual_asset_prices
            WHERE asset_id = ?
            ORDER BY price_date DESC
            LIMIT 1
        """, (asset_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
