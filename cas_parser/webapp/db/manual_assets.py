"""Manual asset management (FD, SGB, stocks, PPF) with calculations."""

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
        ppf_fields = ['ppf_account_number', 'ppf_maturity_date']
        for field in ppf_fields:
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
            'ppf_account_number', 'ppf_maturity_date'
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

    else:
        # For other types, use stored current_value or calculate from units * current_nav
        if asset.get('current_value'):
            asset['calculated_value'] = asset['current_value']
        elif asset.get('units') and asset.get('current_nav'):
            asset['calculated_value'] = asset['units'] * asset['current_nav']
        else:
            asset['calculated_value'] = asset.get('purchase_value', 0)

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
