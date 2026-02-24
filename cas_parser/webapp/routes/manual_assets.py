import json
import logging
from datetime import datetime
from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db
from cas_parser.webapp.routes import DecimalEncoder
from cas_parser.webapp.auth import admin_required, check_investor_access, get_investor_id_for_asset, get_investor_id_for_asset_tx

logger = logging.getLogger(__name__)

manual_assets_bp = Blueprint('manual_assets', __name__)


@manual_assets_bp.route('/api/manual-assets', methods=['GET'])
def api_get_manual_assets():
    """Get all manual assets for an investor."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    check_investor_access(investor_id)
    include_inactive = request.args.get('include_inactive', 'false').lower() == 'true'
    assets = db.get_manual_assets_by_investor(investor_id, include_inactive)
    return jsonify(assets)


@manual_assets_bp.route('/api/manual-assets/summary', methods=['GET'])
def api_get_manual_assets_summary():
    """Get manual assets summary by asset class."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    check_investor_access(investor_id)
    summary = db.get_manual_assets_summary(investor_id)
    return jsonify(summary)


@manual_assets_bp.route('/api/manual-assets/combined', methods=['GET'])
def api_get_combined_portfolio():
    """Get combined portfolio value (MF + manual assets)."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    check_investor_access(investor_id)
    combined = db.get_combined_portfolio_value(investor_id)
    return jsonify(combined)


@manual_assets_bp.route('/api/manual-assets', methods=['POST'])
def api_create_manual_asset():
    """Create a new manual asset."""
    data = request.json

    required = ['investor_id', 'asset_type', 'asset_class', 'name']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    check_investor_access(data['investor_id'])

    # Validate asset_class
    valid_classes = ['equity', 'debt', 'commodity', 'cash', 'others']
    if data['asset_class'] not in valid_classes:
        return jsonify({'error': f'asset_class must be one of: {valid_classes}'}), 400

    # Validate asset_type
    valid_types = ['fd', 'sgb', 'stock', 'ppf', 'nps', 'house', 'car', 'gold', 'other']
    if data['asset_type'] not in valid_types:
        return jsonify({'error': f'asset_type must be one of: {valid_types}'}), 400

    try:
        asset_id = db.create_manual_asset(
            investor_id=data['investor_id'],
            asset_type=data['asset_type'],
            asset_class=data['asset_class'],
            name=data['name'],
            description=data.get('description'),
            purchase_date=data.get('purchase_date'),
            purchase_value=data.get('purchase_value'),
            units=data.get('units', 1),
            current_nav=data.get('current_nav'),
            current_value=data.get('current_value'),
            # FD fields
            fd_principal=data.get('fd_principal'),
            fd_interest_rate=data.get('fd_interest_rate'),
            fd_tenure_months=data.get('fd_tenure_months'),
            fd_maturity_date=data.get('fd_maturity_date'),
            fd_compounding=data.get('fd_compounding', 'quarterly'),
            fd_premature_penalty_pct=data.get('fd_premature_penalty_pct', 1.0),
            fd_bank_name=data.get('fd_bank_name'),
            # SGB fields
            sgb_issue_price=data.get('sgb_issue_price'),
            sgb_interest_rate=data.get('sgb_interest_rate', 2.5),
            sgb_maturity_date=data.get('sgb_maturity_date'),
            sgb_grams=data.get('sgb_grams'),
            # Stock fields
            stock_symbol=data.get('stock_symbol'),
            stock_exchange=data.get('stock_exchange'),
            stock_quantity=data.get('stock_quantity'),
            stock_avg_price=data.get('stock_avg_price'),
            # PPF/NPS fields
            ppf_account_number=data.get('ppf_account_number'),
            ppf_maturity_date=data.get('ppf_maturity_date'),
            ppf_interest_rate=data.get('ppf_interest_rate'),
            ppf_compounding=data.get('ppf_compounding'),
            ppf_opening_balance=data.get('ppf_opening_balance'),
            # Allocation fields
            equity_pct=data.get('equity_pct'),
            debt_pct=data.get('debt_pct'),
            commodity_pct=data.get('commodity_pct'),
            cash_pct=data.get('cash_pct'),
            others_pct=data.get('others_pct'),
            exclude_from_xirr=data.get('exclude_from_xirr'),
        )
        return jsonify({'success': True, 'id': asset_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>', methods=['GET'])
def api_get_manual_asset(asset_id):
    """Get a single manual asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    asset = db.get_manual_asset(asset_id)
    if not asset:
        return jsonify({'error': 'Asset not found'}), 404
    return jsonify(asset)


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>', methods=['PUT'])
def api_update_manual_asset(asset_id):
    """Update a manual asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    data = request.json
    result = db.update_manual_asset(asset_id, **data)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>', methods=['DELETE'])
def api_delete_manual_asset(asset_id):
    """Delete a manual asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    result = db.delete_manual_asset(asset_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify({'error': 'Asset not found'}), 404


@manual_assets_bp.route('/api/manual-assets/calculate/fd', methods=['POST'])
def api_calculate_fd():
    """Calculate FD current value and premature withdrawal value."""
    data = request.json

    required = ['principal', 'interest_rate', 'tenure_months', 'start_date']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    result = db.calculate_fd_value(
        principal=data['principal'],
        interest_rate=data['interest_rate'],
        tenure_months=data['tenure_months'],
        compounding=data.get('compounding', 'quarterly'),
        start_date=data['start_date'],
        as_of_date=data.get('as_of_date')
    )

    # Add premature calculation
    if data.get('premature_penalty_pct') is not None:
        premature = db.calculate_fd_premature_value(
            principal=data['principal'],
            interest_rate=data['interest_rate'],
            premature_penalty_pct=data.get('premature_penalty_pct', 1.0),
            start_date=data['start_date'],
            compounding=data.get('compounding', 'quarterly')
        )
        result['premature'] = premature

    return jsonify(result)


@manual_assets_bp.route('/api/manual-assets/calculate/sgb', methods=['POST'])
def api_calculate_sgb():
    """Calculate SGB current value."""
    data = request.json

    required = ['issue_price', 'grams', 'purchase_date']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    result = db.calculate_sgb_value(
        issue_price=data['issue_price'],
        grams=data['grams'],
        interest_rate=data.get('interest_rate', 2.5),
        purchase_date=data['purchase_date'],
        current_gold_price=data.get('current_gold_price')
    )

    return jsonify(result)


@manual_assets_bp.route('/api/manual-assets/calculate/ppf', methods=['POST'])
def api_calculate_ppf():
    """Calculate PPF current value with compound interest."""
    data = request.json

    required = ['purchase_date']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    result = db.calculate_ppf_value(
        opening_balance=float(data.get('opening_balance', 0) or 0),
        interest_rate=float(data.get('interest_rate', 7.1) or 7.1),
        purchase_date=data['purchase_date'],
        purchase_value=float(data.get('purchase_value', 0) or 0),
    )

    return jsonify(result)


def _parse_date(date_str: str) -> str:
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()
    formats = [
        '%Y-%m-%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%d-%b-%Y',
        '%d-%b-%y',      # 07-Jan-21 (2-digit year)
        '%d %b %Y',
        '%d %b %y',      # 07 Jan 21 (2-digit year)
        '%d-%B-%Y',
        '%d-%B-%y',      # 07-January-21 (2-digit year)
        '%m/%d/%Y',
        '%m/%d/%y',      # 01/07/21 (2-digit year)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return date_str  # Return as-is if can't parse


@manual_assets_bp.route('/api/fd/upload-csv', methods=['POST'])
def api_upload_fd_csv():
    """Parse and validate FD CSV, return preview for confirmation."""
    import csv
    import io

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400

    investor_id = request.form.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    check_investor_access(investor_id)

    try:
        # Read CSV content
        content = file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))

        # Get existing FD account IDs for duplicate detection
        existing_fds = db.get_manual_assets_by_investor(investor_id)
        existing_account_ids = set(
            fd['name'].strip().lower() for fd in existing_fds
            if fd.get('asset_type') == 'fd' and fd.get('name')
        )

        preview_rows = []
        row_num = 0

        for row in reader:
            row_num += 1
            normalized = {}
            errors = []

            # Normalize column names
            for key, value in row.items():
                if not key:
                    continue
                key_lower = key.lower().strip()
                if 'account' in key_lower or 'nick' in key_lower:
                    normalized['name'] = value.strip() if value else ''
                elif 'deposit' in key_lower and 'date' in key_lower:
                    normalized['deposit_date'] = _parse_date(value) if value else None
                elif 'roi' in key_lower or 'interest' in key_lower or 'rate' in key_lower:
                    normalized['roi'] = value.strip() if value else ''
                elif 'maturity' in key_lower and 'date' in key_lower:
                    normalized['maturity_date'] = _parse_date(value) if value else None
                elif 'maturity' in key_lower and 'amount' in key_lower:
                    normalized['maturity_amount'] = value.strip() if value else ''
                elif 'balance' in key_lower or 'principal' in key_lower or 'amount' in key_lower:
                    if 'maturity' not in key_lower:
                        normalized['balance'] = value.strip() if value else ''
                elif 'bank' in key_lower:
                    normalized['bank_name'] = value.strip() if value else ''

            # Validation
            name = normalized.get('name', '')
            balance = normalized.get('balance', '')
            deposit_date = normalized.get('deposit_date')
            maturity_date = normalized.get('maturity_date')
            roi = normalized.get('roi', '')

            # Check for empty/invalid rows
            if not name and not balance:
                errors.append('Empty row - no account name or balance')
            else:
                if not name:
                    errors.append('Missing account name')
                if not balance or balance == '0':
                    errors.append('Missing or zero balance')
                else:
                    try:
                        bal_val = float(balance.replace(',', ''))
                        if bal_val <= 0:
                            errors.append('Balance must be positive')
                    except ValueError:
                        errors.append(f'Invalid balance: {balance}')

                if not deposit_date:
                    errors.append('Missing deposit date')
                if not maturity_date:
                    errors.append('Missing maturity date')
                if not roi:
                    errors.append('Missing interest rate')

                # Check for duplicates
                if name and name.strip().lower() in existing_account_ids:
                    errors.append('Duplicate: FD already exists')

            # Parse balance for display
            try:
                balance_num = float(balance.replace(',', '')) if balance else 0
            except:
                balance_num = 0

            preview_rows.append({
                'row_num': row_num,
                'name': name,
                'deposit_date': deposit_date or normalized.get('deposit_date', ''),
                'maturity_date': maturity_date or normalized.get('maturity_date', ''),
                'roi': roi,
                'balance': balance_num,
                'bank_name': normalized.get('bank_name', ''),
                'is_valid': len(errors) == 0,
                'errors': errors
            })

        valid_count = sum(1 for r in preview_rows if r['is_valid'])
        invalid_count = len(preview_rows) - valid_count

        return jsonify({
            'preview': True,
            'rows': preview_rows,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'total_count': len(preview_rows)
        })

    except Exception as e:
        logger.error(f"FD CSV upload error: {e}")
        return jsonify({'error': str(e)}), 500


@manual_assets_bp.route('/api/fd/import', methods=['POST'])
def api_import_fd():
    """Import validated FD rows into database."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    investor_id = data.get('investor_id')
    rows = data.get('rows', [])

    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    check_investor_access(investor_id)

    # Filter only valid rows
    valid_rows = [r for r in rows if r.get('is_valid', False)]

    if not valid_rows:
        return jsonify({'error': 'No valid rows to import'}), 400

    # Get existing FD account IDs for final duplicate check
    existing_fds = db.get_manual_assets_by_investor(investor_id)
    existing_account_ids = set(
        fd['name'].strip().lower() for fd in existing_fds
        if fd.get('asset_type') == 'fd' and fd.get('name')
    )

    created = 0
    skipped = 0

    for row in valid_rows:
        name = row.get('name', '').strip()

        # Final duplicate check
        if name.lower() in existing_account_ids:
            skipped += 1
            continue

        try:
            balance = float(str(row.get('balance', 0)).replace(',', ''))
            roi = float(str(row.get('roi', '7.0')).replace('%', ''))

            # Calculate tenure from dates
            deposit_date = row.get('deposit_date')
            maturity_date = row.get('maturity_date')
            tenure_months = 12  # default

            if deposit_date and maturity_date:
                try:
                    dep_dt = datetime.strptime(deposit_date, '%Y-%m-%d')
                    mat_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
                    months = (mat_dt.year - dep_dt.year) * 12 + (mat_dt.month - dep_dt.month)
                    tenure_months = max(1, months)
                except:
                    tenure_months = 12

            db.create_manual_asset(
                investor_id=investor_id,
                asset_type='fd',
                asset_class='debt',
                name=name,
                description='FD imported from CSV',
                purchase_date=deposit_date,
                purchase_value=balance,
                fd_principal=balance,
                fd_interest_rate=roi,
                fd_tenure_months=tenure_months,
                fd_maturity_date=maturity_date,
                fd_compounding='quarterly',
                fd_bank_name=row.get('bank_name', '')
            )
            created += 1
            existing_account_ids.add(name.lower())
        except Exception as e:
            logger.error(f"Error importing FD row: {e}")
            skipped += 1

    return jsonify({
        'success': True,
        'created': created,
        'skipped': skipped
    })


@manual_assets_bp.route('/api/fd/maturing', methods=['GET'])
@admin_required
def api_get_maturing_fds():
    """Get FDs maturing within next N days."""
    days = request.args.get('days', 30, type=int)
    fds = db.get_maturing_fds(days)
    return jsonify(fds)


@manual_assets_bp.route('/api/fd/matured', methods=['GET'])
@admin_required
def api_get_matured_fds():
    """Get FDs past maturity that aren't closed."""
    fds = db.get_matured_fds()
    return jsonify(fds)


@manual_assets_bp.route('/api/fd/<int:asset_id>/close', methods=['POST'])
def api_close_fd(asset_id):
    """Mark FD as closed with money received status."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    data = request.get_json() or {}
    money_received = data.get('money_received', True)
    success = db.close_fd(asset_id, money_received)
    if success:
        return jsonify({'success': True})
    return jsonify({'error': 'FD not found'}), 404


# ---------------------------------------------------------------------------
# Manual asset types configuration
# ---------------------------------------------------------------------------

@manual_assets_bp.route('/api/manual-asset-types', methods=['GET'])
def api_get_manual_asset_types():
    """Get configured manual asset types."""
    types = db.get_manual_asset_types()
    return jsonify(types)


@manual_assets_bp.route('/api/manual-asset-types', methods=['PUT'])
@admin_required
def api_set_manual_asset_types():
    """Update manual asset types (admin only)."""
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({'error': 'Expected a list of asset types'}), 400
    result = db.set_manual_asset_types(data)
    return jsonify(result)


# ---------------------------------------------------------------------------
# PPF / EPF transaction-based endpoints
# ---------------------------------------------------------------------------

@manual_assets_bp.route('/api/ppf-epf-assets', methods=['GET'])
def api_get_ppf_epf_assets():
    """Get PPF/EPF assets for an investor (LOV dropdown)."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400
    check_investor_access(investor_id)
    assets = db.get_ppf_epf_assets(investor_id)
    return jsonify(assets)


@manual_assets_bp.route('/api/ppf-epf-assets', methods=['POST'])
def api_create_ppf_epf_asset():
    """Create a new PPF/EPF master record."""
    data = request.get_json()
    investor_id = data.get('investor_id')
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400
    check_investor_access(investor_id)

    sub_type = data.get('sub_type', 'ppf')
    if sub_type not in ('ppf', 'epf'):
        return jsonify({'error': 'sub_type must be ppf or epf'}), 400

    result = db.create_ppf_epf_asset(
        investor_id=investor_id,
        sub_type=sub_type,
        bank=data.get('bank'),
        ref_no=data.get('ref_no'),
    )
    return jsonify(result)


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>/transactions', methods=['GET'])
def api_get_asset_transactions(asset_id):
    """Get transactions for an asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    txns = db.get_asset_transactions(asset_id)
    return jsonify(txns)


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>/transactions', methods=['POST'])
def api_add_asset_transaction(asset_id):
    """Add a transaction to an asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    data = request.get_json()

    tx_type = data.get('tx_type')
    if tx_type not in ('investment', 'interest', 'withdrawal', 'buy', 'sell'):
        return jsonify({'error': 'tx_type must be investment, interest, withdrawal, buy, or sell'}), 400

    tx_date = data.get('tx_date')
    if not tx_date:
        return jsonify({'error': 'tx_date is required'}), 400

    amount = data.get('amount')
    if not amount or float(amount) <= 0:
        return jsonify({'error': 'amount must be positive'}), 400

    result = db.add_asset_transaction(
        asset_id=asset_id,
        tx_type=tx_type,
        tx_date=tx_date,
        amount=float(amount),
        narration=data.get('narration'),
        quantity=float(data['quantity']) if data.get('quantity') else None,
        rate=float(data['rate']) if data.get('rate') else None,
    )
    return jsonify(result)


@manual_assets_bp.route('/api/manual-assets/transactions/<int:tx_id>', methods=['DELETE'])
def api_delete_asset_transaction(tx_id):
    """Delete an asset transaction."""
    check_investor_access(get_investor_id_for_asset_tx(tx_id))
    result = db.delete_asset_transaction(tx_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404


# ---------------------------------------------------------------------------
# Gold lot endpoints
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Manual asset price history (for gold price tracking)
# ---------------------------------------------------------------------------

@manual_assets_bp.route('/api/manual-assets/<int:asset_id>/prices', methods=['GET'])
def api_get_asset_prices(asset_id):
    """Get price history for an asset."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    limit = request.args.get('limit', 20, type=int)
    prices = db.get_asset_prices(asset_id, limit=limit)
    return jsonify(prices)


@manual_assets_bp.route('/api/manual-assets/<int:asset_id>/prices', methods=['POST'])
def api_add_asset_price(asset_id):
    """Record a price for an asset (e.g. gold price per gram)."""
    check_investor_access(get_investor_id_for_asset(asset_id))
    data = request.get_json()

    price_date = data.get('price_date')
    if not price_date:
        return jsonify({'error': 'price_date is required'}), 400

    price_per_unit = data.get('price_per_unit')
    if not price_per_unit or float(price_per_unit) <= 0:
        return jsonify({'error': 'price_per_unit must be positive'}), 400

    result = db.add_asset_price(asset_id, price_date, float(price_per_unit))
    return jsonify(result)


@manual_assets_bp.route('/api/gold-assets', methods=['GET'])
def api_get_gold_assets():
    """Get gold lots for an investor (LOV dropdown)."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400
    check_investor_access(investor_id)
    lots = db.get_gold_lots(investor_id)
    return jsonify(lots)


@manual_assets_bp.route('/api/gold-assets', methods=['POST'])
def api_create_gold_asset():
    """Create a new gold lot."""
    data = request.get_json()
    investor_id = data.get('investor_id')
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400
    check_investor_access(investor_id)

    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400

    result = db.create_gold_lot(
        investor_id=investor_id,
        name=name,
        ref_no=data.get('ref_no'),
        seller=data.get('seller'),
        broker=data.get('broker'),
    )
    return jsonify(result)
