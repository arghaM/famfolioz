"""
Flask web application for CAS PDF Parser.

Provides a web UI for uploading CAS PDFs, viewing parsed data,
and managing investor portfolios with persistence.
"""

import json
import os
import tempfile
from decimal import Decimal

from flask import Flask, jsonify, render_template, request, redirect, url_for

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cas_parser.main import parse_cas_pdf
from cas_parser.webapp import data as db
from cas_parser.webapp.xirr import build_cashflows_for_folio, xirr

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


# ==================== Page Routes ====================

@app.route('/')
def index():
    """Render the dashboard page."""
    return render_template('dashboard.html')


@app.route('/upload')
def upload():
    """Render the upload page."""
    return render_template('upload.html')


@app.route('/investor/<int:investor_id>')
def investor_detail(investor_id):
    """Render investor detail page."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('index'))
    return render_template('investor.html', investor=investor)


@app.route('/folio/<int:folio_id>')
def folio_detail(folio_id):
    """Render folio/investment detail page."""
    return render_template('folio.html', folio_id=folio_id)


@app.route('/map-folios')
def map_folios_page():
    """Render folio mapping page."""
    return render_template('map_folios.html')


# ==================== API Routes ====================

@app.route('/api/investors', methods=['GET'])
def api_get_investors():
    """Get all investors."""
    investors = db.get_all_investors()
    return jsonify(investors)


@app.route('/api/investors', methods=['POST'])
def api_create_investor():
    """Create a new investor."""
    data = request.json
    investor_id = db.create_investor(
        name=data.get('name', ''),
        pan=data.get('pan'),
        email=data.get('email'),
        mobile=data.get('mobile')
    )
    return jsonify({'id': investor_id})


@app.route('/api/investors/<int:investor_id>', methods=['GET'])
def api_get_investor(investor_id):
    """Get investor details."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return jsonify({'error': 'Investor not found'}), 404
    return jsonify(investor)


@app.route('/api/investors/<int:investor_id>', methods=['PUT'])
def api_update_investor(investor_id):
    """Update investor details."""
    data = request.json

    result = db.update_investor(
        investor_id=investor_id,
        name=data.get('name'),
        email=data.get('email'),
        mobile=data.get('mobile')
    )

    if result:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Failed to update'}), 400


@app.route('/api/investors/<int:investor_id>/holdings', methods=['GET'])
def api_get_investor_holdings(investor_id):
    """Get all holdings for an investor."""
    holdings = db.get_holdings_by_investor(investor_id)
    return jsonify(holdings)


@app.route('/api/investors/<int:investor_id>/transactions', methods=['GET'])
def api_get_investor_transactions(investor_id):
    """Get transactions for an investor."""
    limit = request.args.get('limit', 500, type=int)
    transactions = db.get_transactions_by_investor(investor_id, limit=limit)
    return jsonify(transactions)


@app.route('/api/investors/<int:investor_id>/stats', methods=['GET'])
def api_get_investor_stats(investor_id):
    """Get statistics for an investor."""
    stats = db.get_transaction_stats(investor_id)
    return jsonify(stats)


@app.route('/api/folios/<int:folio_id>/transactions', methods=['GET'])
def api_get_folio_transactions(folio_id):
    """Get all transactions for a folio."""
    transactions = db.get_transactions_by_folio(folio_id)
    return jsonify(transactions)


@app.route('/api/unmapped-folios', methods=['GET'])
def api_get_unmapped_folios():
    """Get all unmapped folios."""
    folios = db.get_unmapped_folios()
    return jsonify(folios)


@app.route('/api/map-folios', methods=['POST'])
def api_map_folios():
    """Map folios to an investor."""
    data = request.json
    investor_id = data.get('investor_id')
    folio_ids = data.get('folio_ids', [])

    if not investor_id:
        # Create new investor
        investor_id = db.create_investor(
            name=data.get('investor_name', 'Unknown'),
            pan=data.get('pan'),
            email=data.get('email'),
            mobile=data.get('mobile')
        )

    db.map_folios_to_investor(folio_ids, investor_id)

    return jsonify({
        'success': True,
        'investor_id': investor_id,
        'mapped_count': len(folio_ids)
    })


@app.route('/api/folios/<int:folio_id>/info', methods=['GET'])
def api_get_folio_info(folio_id):
    """Get folio metadata with investor and holdings info."""
    folio = db.get_folio_by_id(folio_id)
    if not folio:
        return jsonify({'error': 'Folio not found'}), 404
    return jsonify(folio)


@app.route('/api/unmap-folio', methods=['POST'])
def api_unmap_folio():
    """Remove investor mapping from a folio."""
    data = request.json
    folio_id = data.get('folio_id')
    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400
    db.unmap_folio(folio_id)
    return jsonify({'success': True})


@app.route('/api/parse', methods=['POST'])
def api_parse_pdf():
    """
    Parse an uploaded CAS PDF and store in database.

    Returns import summary including any unmapped folios.
    """
    # Check if file was uploaded
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'File must be a PDF'}), 400

    password = request.form.get('password', '')

    # Save to temporary file
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        file.save(tmp.name)
        tmp_path = tmp.name

    try:
        # Parse the PDF
        statement = parse_cas_pdf(tmp_path, password=password if password else None)

        # Convert to dict
        parsed_data = statement.to_dict()

        # Import into database
        import_result = db.import_parsed_data(parsed_data, source_filename=file.filename)

        # Add parsed data summary
        import_result['parsed_summary'] = {
            'investor_name': parsed_data['investor']['name'],
            'investor_pan': parsed_data['investor']['pan'],
            'total_holdings': len(parsed_data['holdings']),
            'total_transactions': len(parsed_data['transactions']),
            'validation': parsed_data['validation']
        }

        return jsonify(import_result)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@app.route('/api/nav/refresh', methods=['POST'])
def api_refresh_nav():
    """Refresh NAV data from AMFI for mapped funds and take portfolio snapshots."""
    try:
        result = db.fetch_and_update_nav()

        # Also take portfolio snapshots for all investors
        if result.get('success'):
            snapshot_result = db.take_all_portfolio_snapshots()
            result['snapshots'] = snapshot_result

        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/nav/history/<isin>', methods=['GET'])
def api_get_nav_history(isin):
    """Get historical NAV for a scheme."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    history = db.get_nav_history(isin, start_date, end_date)
    return jsonify(history)


@app.route('/api/nav/history-dates', methods=['GET'])
def api_get_nav_history_dates():
    """Get all dates with NAV history."""
    dates = db.get_nav_history_dates()
    return jsonify(dates)


@app.route('/api/nav/status', methods=['GET'])
def api_nav_status():
    """Get NAV update status."""
    last_update = db.get_last_nav_update()
    stats = db.get_mutual_fund_stats()
    return jsonify({
        'last_update': last_update,
        'stats': stats
    })


@app.route('/api/investors/<int:investor_id>/holdings-live', methods=['GET'])
def api_get_investor_holdings_live(investor_id):
    """Get all holdings for an investor with live NAV."""
    holdings = db.get_holdings_by_investor(investor_id)
    holdings = db.get_nav_for_holdings(holdings)
    return jsonify(holdings)


@app.route('/api/folios/<int:folio_id>/xirr', methods=['GET'])
def api_get_folio_xirr(folio_id):
    """Get XIRR for a specific folio."""
    data = db.get_xirr_data_for_folio(folio_id)
    cashflows = build_cashflows_for_folio(
        data['transactions'], data['current_value']
    )
    xirr_val = xirr(cashflows)
    return jsonify({
        'folio_id': folio_id,
        'xirr': round(xirr_val * 100, 2) if xirr_val is not None else None,
        'current_value': data['current_value'],
        'cashflow_count': len(cashflows),
    })


@app.route('/api/investors/<int:investor_id>/xirr', methods=['GET'])
def api_get_investor_xirr(investor_id):
    """Get XIRR for all folios of an investor + portfolio-level and per-ISIN XIRR."""
    folio_data_list = db.get_xirr_data_for_investor(investor_id)
    all_cashflows = []
    folios = []
    isin_cashflows = {}  # isin -> list of cashflows

    for data in folio_data_list:
        cashflows = build_cashflows_for_folio(
            data['transactions'], data['current_value']
        )
        xirr_val = xirr(cashflows)
        folio_isin = data.get('isin')
        folios.append({
            'folio_id': data['folio_id'],
            'scheme_name': data['scheme_name'],
            'folio_number': data['folio_number'],
            'isin': folio_isin,
            'xirr': round(xirr_val * 100, 2) if xirr_val is not None else None,
            'current_value': data['current_value'],
            'cashflow_count': len(cashflows),
        })
        # Only include folios with valid XIRR in portfolio calculation
        # (folios with bad data that can't compute XIRR would corrupt portfolio XIRR)
        if xirr_val is not None:
            all_cashflows.extend(cashflows)
            # Accumulate per-ISIN cashflows
            if folio_isin:
                if folio_isin not in isin_cashflows:
                    isin_cashflows[folio_isin] = []
                isin_cashflows[folio_isin].extend(cashflows)

    # Compute per-ISIN aggregated XIRR
    isin_xirr = {}
    for isin, cfs in isin_cashflows.items():
        xirr_val = xirr(cfs)
        isin_xirr[isin] = round(xirr_val * 100, 2) if xirr_val is not None else None

    portfolio_xirr = xirr(all_cashflows)
    return jsonify({
        'portfolio_xirr': round(portfolio_xirr * 100, 2) if portfolio_xirr is not None else None,
        'folios': folios,
        'isin_xirr': isin_xirr,
    })


@app.route('/api/investors/<int:investor_id>/portfolio-history', methods=['GET'])
def api_get_portfolio_history(investor_id):
    """Get historical portfolio valuation."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    history = db.get_portfolio_history(investor_id, start_date, end_date)
    return jsonify(history)


@app.route('/api/investors/<int:investor_id>/valuation/<valuation_date>', methods=['GET'])
def api_get_portfolio_valuation(investor_id, valuation_date):
    """Get portfolio valuation on a specific date."""
    valuation = db.get_portfolio_valuation_on_date(investor_id, valuation_date)
    return jsonify(valuation)


@app.route('/api/investors/<int:investor_id>/snapshot', methods=['POST'])
def api_take_portfolio_snapshot(investor_id):
    """Take a portfolio snapshot for an investor."""
    data = request.json or {}
    snapshot_date = data.get('snapshot_date')
    result = db.take_portfolio_snapshot(investor_id, snapshot_date)
    return jsonify(result)


# ==================== Mutual Fund Master Routes ====================

@app.route('/mutual-funds')
def mutual_funds_page():
    """Render mutual fund master page."""
    return render_template('mutual_funds.html')


@app.route('/api/mutual-funds', methods=['GET'])
def api_get_mutual_funds():
    """Get all mutual funds."""
    funds = db.get_all_mutual_funds()
    return jsonify(funds)


@app.route('/api/mutual-funds/unmapped', methods=['GET'])
def api_get_unmapped_mutual_funds():
    """Get unmapped mutual funds."""
    funds = db.get_unmapped_mutual_funds()
    return jsonify(funds)


@app.route('/api/mutual-funds/stats', methods=['GET'])
def api_get_mutual_fund_stats():
    """Get mutual fund statistics."""
    stats = db.get_mutual_fund_stats()
    return jsonify(stats)


@app.route('/api/mutual-funds/<int:mf_id>/map', methods=['POST'])
def api_map_mutual_fund(mf_id):
    """Map a mutual fund to AMFI code."""
    data = request.json
    amfi_code = data.get('amfi_code', '').strip()
    amfi_scheme_name = data.get('amfi_scheme_name', '')

    if not amfi_code:
        return jsonify({'error': 'AMFI code is required'}), 400

    success = db.map_mutual_fund_to_amfi(mf_id, amfi_code, amfi_scheme_name)
    return jsonify({'success': success})


@app.route('/api/mutual-funds/<int:mf_id>/name', methods=['PUT'])
def api_update_fund_name(mf_id):
    """Update display name for a mutual fund."""
    data = request.json
    display_name = data.get('display_name', '').strip()

    success = db.update_fund_display_name(mf_id, display_name)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Fund not found'}), 404


@app.route('/api/mutual-funds/<int:mf_id>/allocation', methods=['POST'])
def api_update_fund_allocation(mf_id):
    """Update asset allocation for a mutual fund."""
    data = request.json

    result = db.update_fund_asset_allocation(
        mf_id=mf_id,
        equity_pct=float(data.get('equity_pct', 0) or 0),
        debt_pct=float(data.get('debt_pct', 0) or 0),
        commodity_pct=float(data.get('commodity_pct', 0) or 0),
        cash_pct=float(data.get('cash_pct', 0) or 0),
        others_pct=float(data.get('others_pct', 0) or 0),
        large_cap_pct=float(data.get('large_cap_pct', 0) or 0),
        mid_cap_pct=float(data.get('mid_cap_pct', 0) or 0),
        small_cap_pct=float(data.get('small_cap_pct', 0) or 0)
    )

    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/mutual-funds/<int:mf_id>/classification', methods=['PUT'])
def api_update_fund_classification(mf_id):
    """Update fund category and geography classification."""
    data = request.json
    fund_category = data.get('fund_category') or None
    geography = data.get('geography') or None

    result = db.update_fund_classification(mf_id, fund_category, geography)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/mutual-funds/<int:mf_id>/detail', methods=['GET'])
def api_get_fund_detail(mf_id):
    """Return fund with holdings and sectors arrays."""
    fund = db.get_fund_detail(mf_id)
    if not fund:
        return jsonify({'error': 'Fund not found'}), 404
    return jsonify(fund)


@app.route('/api/mutual-funds/<int:mf_id>/holdings', methods=['PUT'])
def api_update_fund_holdings(mf_id):
    """Replace all stock holdings for a fund."""
    data = request.json
    holdings = data.get('holdings', [])
    result = db.update_fund_holdings(mf_id, holdings)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/mutual-funds/<int:mf_id>/sectors', methods=['PUT'])
def api_update_fund_sectors(mf_id):
    """Replace all sector allocations for a fund."""
    data = request.json
    sectors = data.get('sectors', [])
    result = db.update_fund_sectors(mf_id, sectors)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/mutual-funds/<int:mf_id>/review', methods=['PUT'])
def api_confirm_fund_review(mf_id):
    """Confirm allocation review for a fund (resets 30-day timer)."""
    success = db.confirm_fund_allocation_review(mf_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Fund not found'}), 404


@app.route('/api/mutual-funds/review-alerts', methods=['GET'])
def api_get_review_alerts():
    """Get funds needing allocation review (30+ days since last review)."""
    days = request.args.get('days', 30, type=int)
    funds = db.get_funds_needing_review(days)
    return jsonify({'count': len(funds), 'funds': funds})


@app.route('/api/investors/<int:investor_id>/alerts', methods=['GET'])
def api_get_investor_alerts(investor_id):
    """Get aggregated alerts for an investor."""
    return jsonify(db.get_investor_alerts(investor_id))


@app.route('/api/investors/<int:investor_id>/asset-allocation', methods=['GET'])
def api_get_asset_allocation(investor_id):
    """Get portfolio-level asset allocation."""
    allocation = db.get_portfolio_asset_allocation(investor_id)
    return jsonify(allocation)


@app.route('/api/amfi/search', methods=['GET'])
def api_search_amfi():
    """Search AMFI schemes."""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify([])

    results = db.search_amfi_schemes(query)
    return jsonify(results)


# ==================== Transaction Edit Routes ====================

@app.route('/api/transactions/<int:tx_id>', methods=['GET'])
def api_get_transaction(tx_id):
    """Get a single transaction."""
    tx = db.get_transaction_by_id(tx_id)
    if not tx:
        return jsonify({'error': 'Transaction not found'}), 404
    tx['version_count'] = db.get_transaction_version_count(tx_id)
    return jsonify(tx)


@app.route('/api/transactions/<int:tx_id>', methods=['PUT'])
def api_update_transaction(tx_id):
    """Update a transaction with mandatory comment."""
    data = request.json

    edit_comment = data.get('edit_comment', '').strip()
    if not edit_comment:
        return jsonify({'error': 'Edit comment is required'}), 400

    result = db.update_transaction(
        tx_id=tx_id,
        tx_date=data.get('tx_date'),
        tx_type=data.get('tx_type'),
        description=data.get('description'),
        amount=float(data.get('amount', 0) or 0),
        units=float(data.get('units', 0)),
        nav=float(data.get('nav', 0) or 0),
        balance_units=float(data.get('balance_units', 0)),
        edit_comment=edit_comment,
        edited_by=data.get('edited_by')
    )

    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/transactions/<int:tx_id>/versions', methods=['GET'])
def api_get_transaction_versions(tx_id):
    """Get all versions of a transaction."""
    versions = db.get_transaction_versions(tx_id)
    return jsonify(versions)


# ==================== Conflict Resolution Routes ====================

@app.route('/resolve-conflicts')
def resolve_conflicts_page():
    """Render conflict resolution page."""
    return render_template('resolve_conflicts.html')


@app.route('/api/conflicts', methods=['GET'])
def api_get_conflicts():
    """Get all pending conflict groups."""
    groups = db.get_pending_conflict_groups()
    return jsonify(groups)


@app.route('/api/conflicts/stats', methods=['GET'])
def api_get_conflict_stats():
    """Get conflict statistics."""
    stats = db.get_conflict_stats()
    return jsonify(stats)


@app.route('/api/conflicts/<conflict_group_id>', methods=['GET'])
def api_get_conflict_group(conflict_group_id):
    """Get transactions in a conflict group."""
    transactions = db.get_conflict_group_transactions(conflict_group_id)
    return jsonify(transactions)


@app.route('/api/conflicts/<conflict_group_id>/resolve', methods=['POST'])
def api_resolve_conflict(conflict_group_id):
    """Resolve a conflict by selecting which transactions to keep."""
    data = request.json
    selected_hashes = data.get('selected_hashes', [])

    if not selected_hashes:
        return jsonify({'error': 'At least one transaction must be selected'}), 400

    result = db.resolve_conflict(conflict_group_id, selected_hashes)
    return jsonify(result)


# ==================== Goals Routes ====================

@app.route('/investor/<int:investor_id>/goals')
def goals_page(investor_id):
    """Render goals page for an investor."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('index'))
    return render_template('goals.html', investor=investor)


@app.route('/api/investors/<int:investor_id>/goals', methods=['GET'])
def api_get_goals(investor_id):
    """Get all goals for an investor."""
    goals = db.get_goals_by_investor(investor_id)
    return jsonify(goals)


@app.route('/api/investors/<int:investor_id>/goals', methods=['POST'])
def api_create_goal(investor_id):
    """Create a new goal."""
    data = request.json

    if not data.get('name'):
        return jsonify({'error': 'Goal name is required'}), 400

    goal_id = db.create_goal(
        investor_id=investor_id,
        name=data.get('name'),
        description=data.get('description'),
        target_amount=float(data.get('target_amount', 0) or 0),
        target_date=data.get('target_date'),
        target_equity_pct=float(data.get('target_equity_pct', 0) or 0),
        target_debt_pct=float(data.get('target_debt_pct', 0) or 0),
        target_commodity_pct=float(data.get('target_commodity_pct', 0) or 0),
        target_cash_pct=float(data.get('target_cash_pct', 0) or 0),
        target_others_pct=float(data.get('target_others_pct', 0) or 0)
    )

    return jsonify({'success': True, 'goal_id': goal_id})


@app.route('/api/goals/<int:goal_id>', methods=['GET'])
def api_get_goal(goal_id):
    """Get a single goal with details."""
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404
    return jsonify(goal)


@app.route('/api/goals/<int:goal_id>', methods=['PUT'])
def api_update_goal(goal_id):
    """Update a goal."""
    data = request.json

    result = db.update_goal(
        goal_id=goal_id,
        name=data.get('name'),
        description=data.get('description'),
        target_amount=float(data.get('target_amount')) if data.get('target_amount') is not None else None,
        target_date=data.get('target_date'),
        target_equity_pct=float(data.get('target_equity_pct')) if data.get('target_equity_pct') is not None else None,
        target_debt_pct=float(data.get('target_debt_pct')) if data.get('target_debt_pct') is not None else None,
        target_commodity_pct=float(data.get('target_commodity_pct')) if data.get('target_commodity_pct') is not None else None,
        target_cash_pct=float(data.get('target_cash_pct')) if data.get('target_cash_pct') is not None else None,
        target_others_pct=float(data.get('target_others_pct')) if data.get('target_others_pct') is not None else None
    )

    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/goals/<int:goal_id>', methods=['DELETE'])
def api_delete_goal(goal_id):
    """Delete a goal."""
    result = db.delete_goal(goal_id)
    return jsonify(result)


@app.route('/api/goals/<int:goal_id>/link', methods=['POST'])
def api_link_folio_to_goal(goal_id):
    """Link a folio to a goal."""
    data = request.json
    folio_id = data.get('folio_id')

    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400

    result = db.link_folio_to_goal(goal_id, folio_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/goals/<int:goal_id>/unlink', methods=['POST'])
def api_unlink_folio_from_goal(goal_id):
    """Unlink a folio from a goal."""
    data = request.json
    folio_id = data.get('folio_id')

    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400

    result = db.unlink_folio_from_goal(goal_id, folio_id)
    return jsonify(result)


@app.route('/api/goals/<int:goal_id>/available-folios', methods=['GET'])
def api_get_available_folios(goal_id):
    """Get folios that can be linked to this goal."""
    # Get the goal to find investor_id
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404

    folios = db.get_unlinked_folios_for_goal(goal_id, goal['investor_id'])
    return jsonify(folios)


# ==================== Goal Notes Routes ====================

@app.route('/api/goals/<int:goal_id>/notes', methods=['GET'])
def api_get_goal_notes(goal_id):
    """Get all notes for a goal."""
    limit = request.args.get('limit', 50, type=int)
    notes = db.get_goal_notes(goal_id, limit=limit)
    return jsonify(notes)


@app.route('/api/goals/<int:goal_id>/notes', methods=['POST'])
def api_create_goal_note(goal_id):
    """Create a new note for a goal."""
    data = request.json

    content = data.get('content', '').strip()
    if not content:
        return jsonify({'error': 'Note content is required'}), 400

    note_id = db.create_goal_note(
        goal_id=goal_id,
        content=content,
        title=data.get('title', '').strip() or None,
        note_type=data.get('note_type', 'thought'),
        mood=data.get('mood')
    )

    return jsonify({'success': True, 'note_id': note_id})


@app.route('/api/notes/<int:note_id>', methods=['GET'])
def api_get_note(note_id):
    """Get a single note by ID."""
    note = db.get_goal_note_by_id(note_id)
    if not note:
        return jsonify({'error': 'Note not found'}), 404
    return jsonify(note)


@app.route('/api/notes/<int:note_id>', methods=['PUT'])
def api_update_note(note_id):
    """Update a note."""
    data = request.json

    result = db.update_goal_note(
        note_id=note_id,
        content=data.get('content'),
        title=data.get('title'),
        note_type=data.get('note_type'),
        mood=data.get('mood')
    )

    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/notes/<int:note_id>', methods=['DELETE'])
def api_delete_note(note_id):
    """Delete a note."""
    result = db.delete_goal_note(note_id)
    return jsonify(result)


@app.route('/api/investors/<int:investor_id>/notes-timeline', methods=['GET'])
def api_get_notes_timeline(investor_id):
    """Get a timeline of all notes across all goals for an investor."""
    limit = request.args.get('limit', 100, type=int)
    notes = db.get_goal_notes_timeline(investor_id, limit=limit)
    return jsonify(notes)


# ==================== Tax Harvesting Routes ====================

@app.route('/investor/<int:investor_id>/tax-harvesting')
def tax_harvesting_page(investor_id):
    """Render tax-loss harvesting page for an investor."""
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return redirect(url_for('index'))
    return render_template('tax_harvesting.html', investor=investor)


@app.route('/api/investors/<int:investor_id>/tax-harvesting', methods=['GET'])
def api_get_tax_harvesting(investor_id):
    """Compute tax-loss harvesting analysis."""
    tax_slab = request.args.get('tax_slab', type=float)
    result = db.compute_tax_harvesting(investor_id, tax_slab)
    return jsonify(result)


@app.route('/api/investors/<int:investor_id>/tax-slab', methods=['PUT'])
def api_update_tax_slab(investor_id):
    """Update investor's tax slab percentage."""
    data = request.json
    tax_slab_pct = data.get('tax_slab_pct')
    if tax_slab_pct is None:
        return jsonify({'error': 'tax_slab_pct is required'}), 400
    result = db.update_investor_tax_slab(investor_id, float(tax_slab_pct))
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404


@app.route('/api/mutual-funds/<int:mf_id>/exit-load', methods=['PUT'])
def api_update_exit_load(mf_id):
    """Update exit load percentage for a mutual fund."""
    data = request.json
    exit_load_pct = data.get('exit_load_pct')
    if exit_load_pct is None:
        return jsonify({'error': 'exit_load_pct is required'}), 400
    result = db.update_fund_exit_load(mf_id, float(exit_load_pct))
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404


# ==================== Backup/Restore Routes ====================

@app.route('/settings')
def settings_page():
    """Render settings/admin page."""
    return render_template('settings.html')


@app.route('/api/backup', methods=['POST'])
def api_backup():
    """Create a backup of static tables."""
    try:
        result = db.backup_static_tables()
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/backups', methods=['GET'])
def api_list_backups():
    """List all available backups."""
    backups = db.list_backups()
    return jsonify(backups)


@app.route('/api/restore', methods=['POST'])
def api_restore():
    """Restore static tables from backup."""
    data = request.json or {}
    backup_file = data.get('backup_file')

    try:
        result = db.restore_static_tables(backup_file)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/backups/download/<filename>')
def api_download_backup(filename):
    """Download a backup file."""
    from flask import send_from_directory
    from pathlib import Path

    backup_dir = Path(__file__).parent / 'backups'
    backup_file = backup_dir / filename

    if not backup_file.exists():
        return jsonify({'error': 'Backup not found'}), 404

    # Security check - ensure filename is safe
    if '..' in filename or '/' in filename:
        return jsonify({'error': 'Invalid filename'}), 400

    return send_from_directory(
        backup_dir,
        filename,
        as_attachment=True,
        download_name=filename
    )


@app.route('/api/reset-database', methods=['POST'])
def api_reset_database():
    """Reset the entire database. WARNING: Destructive!"""
    data = request.json or {}

    # Require confirmation
    if data.get('confirm') != 'RESET':
        return jsonify({'success': False, 'error': 'Must confirm with {"confirm": "RESET"}'}), 400

    try:
        result = db.reset_database()
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'ok'})


# ==================== ISIN Resolver Routes ====================

@app.route('/api/isin-resolver/refresh-amfi', methods=['POST'])
def api_refresh_amfi():
    """Refresh AMFI scheme database."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        resolver = get_isin_resolver()
        success = resolver.refresh_amfi_data()
        return jsonify({
            'success': success,
            'scheme_count': resolver.get_amfi_scheme_count()
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/isin-resolver/status', methods=['GET'])
def api_isin_resolver_status():
    """Get ISIN resolver status."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        resolver = get_isin_resolver()
        return jsonify({
            'amfi_scheme_count': resolver.get_amfi_scheme_count(),
            'manual_mappings': resolver.get_manual_mappings()
        })
    except Exception as e:
        return jsonify({'amfi_scheme_count': 0, 'manual_mappings': {}, 'error': str(e)})


@app.route('/api/isin-resolver/mappings', methods=['GET'])
def api_get_isin_mappings():
    """Get all manual ISIN mappings."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        mappings = get_isin_resolver().get_manual_mappings()
        return jsonify(mappings)
    except Exception as e:
        return jsonify({}), 500


@app.route('/api/isin-resolver/mappings', methods=['POST'])
def api_add_isin_mapping():
    """Add a manual ISIN mapping."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        data = request.json
        scheme_pattern = data.get('scheme_pattern', '')
        isin = data.get('isin', '')

        if not scheme_pattern or not isin:
            return jsonify({'success': False, 'error': 'scheme_pattern and isin are required'}), 400

        if len(isin) != 12 or not isin.startswith('INF'):
            return jsonify({'success': False, 'error': 'ISIN must be 12 characters starting with INF'}), 400

        success = get_isin_resolver().add_manual_mapping(scheme_pattern, isin)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/isin-resolver/mappings/<path:scheme_pattern>', methods=['DELETE'])
def api_delete_isin_mapping(scheme_pattern):
    """Delete a manual ISIN mapping."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        success = get_isin_resolver().remove_manual_mapping(scheme_pattern)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/isin-resolver/resolve', methods=['POST'])
def api_resolve_isin():
    """Try to resolve an ISIN from partial ISIN and scheme name."""
    try:
        from cas_parser.isin_resolver import resolve_isin
        data = request.json
        partial_isin = data.get('partial_isin', '')
        scheme_name = data.get('scheme_name', '')
        amc = data.get('amc')

        resolved = resolve_isin(partial_isin, scheme_name, amc)
        return jsonify({
            'resolved_isin': resolved,
            'success': resolved is not None
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/mutual-funds/unresolved-isins', methods=['GET'])
def api_get_unresolved_isins():
    """Get mutual funds with missing or invalid ISINs, with suggested corrections."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        resolver = get_isin_resolver()

        funds = db.get_all_mutual_funds()
        unresolved = []
        for fund in funds:
            isin = fund.get('isin', '')
            if not isin or not isin.startswith('INF') or len(isin) != 12 or isin.startswith('UNKNOWN_'):
                # Try to find suggested ISIN
                partial = isin.replace('UNKNOWN_', '') if isin and isin.startswith('UNKNOWN_') else ''
                scheme_name = fund.get('scheme_name', '')

                suggested = resolver.resolve_isin(partial, scheme_name, fund.get('amc'))
                fund['suggested_isin'] = suggested
                fund['partial_isin'] = partial
                unresolved.append(fund)
        return jsonify(unresolved)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify([]), 500


@app.route('/api/isin-resolver/search', methods=['GET'])
def api_search_isin():
    """Search for ISIN by scheme name in AMFI database."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        from difflib import SequenceMatcher

        query = request.args.get('q', '').strip().lower()
        if not query or len(query) < 3:
            return jsonify([])

        resolver = get_isin_resolver()
        results = []

        for isin, info in resolver._amfi_data.items():
            scheme_name = info.get('scheme_name', '')
            if query in scheme_name.lower():
                score = SequenceMatcher(None, query, scheme_name.lower()).ratio()
                results.append({
                    'isin': isin,
                    'scheme_name': scheme_name,
                    'amc': info.get('amc', ''),
                    'score': score
                })

        # Sort by score descending, limit to 20 results
        results.sort(key=lambda x: x['score'], reverse=True)
        return jsonify(results[:20])
    except Exception as e:
        return jsonify([]), 500


@app.route('/api/mutual-funds/<int:fund_id>/update-isin', methods=['POST'])
def api_update_fund_isin(fund_id):
    """Manually update a mutual fund's ISIN."""
    try:
        data = request.json
        new_isin = data.get('isin', '')

        if len(new_isin) != 12 or not new_isin.startswith('INF'):
            return jsonify({'success': False, 'error': 'ISIN must be 12 characters starting with INF'}), 400

        with db.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE mutual_fund_master SET isin = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (new_isin, fund_id))

            if cursor.rowcount > 0:
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Fund not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Quarantine Routes ====================

@app.route('/api/quarantine', methods=['GET'])
def api_get_quarantine():
    """Get all quarantined items."""
    items = db.get_quarantined_items()
    return jsonify(items)


@app.route('/api/quarantine/summary', methods=['GET'])
def api_get_quarantine_summary():
    """Get summary of quarantined items grouped by partial ISIN."""
    summary = db.get_quarantine_summary()
    return jsonify(summary)


@app.route('/api/quarantine/stats', methods=['GET'])
def api_get_quarantine_stats():
    """Get quarantine statistics."""
    stats = db.get_quarantine_stats()
    return jsonify(stats)


@app.route('/api/quarantine/resolve', methods=['POST'])
def api_resolve_quarantine():
    """Resolve quarantined items by providing the correct ISIN."""
    data = request.json
    partial_isin = data.get('partial_isin', '')  # Can be empty string for completely missing ISINs
    scheme_name = data.get('scheme_name', '')  # Used when partial_isin is empty
    resolved_isin = data.get('resolved_isin', '')

    if not resolved_isin or len(resolved_isin) != 12 or not resolved_isin.startswith('INF'):
        return jsonify({'success': False, 'error': 'resolved_isin must be 12 characters starting with INF'}), 400

    # Use scheme_name as fallback identifier when partial_isin is empty
    result = db.resolve_quarantine(partial_isin, resolved_isin, scheme_name)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/quarantine/<partial_isin>', methods=['DELETE'])
def api_delete_quarantine(partial_isin):
    """Delete quarantined items for a partial ISIN."""
    result = db.delete_quarantine_items(partial_isin)
    return jsonify(result)


@app.route('/api/quarantine/delete', methods=['POST'])
def api_delete_quarantine_post():
    """Delete quarantined items (POST version for when scheme_name is needed)."""
    data = request.json
    partial_isin = data.get('partial_isin', '')
    scheme_name = data.get('scheme_name', '')

    result = db.delete_quarantine_items(partial_isin, scheme_name)
    return jsonify(result)


# ==================== Validation Routes ====================

@app.route('/api/validation/issues', methods=['GET'])
def api_get_validation_issues():
    """Get all open validation issues."""
    investor_id = request.args.get('investor_id', type=int)
    status = request.args.get('status', 'open')
    issues = db.get_validation_issues(investor_id, status)
    return jsonify(issues)


@app.route('/api/validation/run', methods=['POST'])
def api_run_validation():
    """Run validation on all folios or for a specific investor."""
    data = request.json or {}
    investor_id = data.get('investor_id')
    result = db.run_post_import_validation(investor_id)
    return jsonify(result)


@app.route('/api/validation/folio/<int:folio_id>', methods=['GET'])
def api_validate_folio(folio_id):
    """Validate a specific folio."""
    result = db.validate_folio_units(folio_id)
    return jsonify(result)


@app.route('/api/validation/issues/<int:issue_id>/resolve', methods=['POST'])
def api_resolve_validation_issue(issue_id):
    """Mark a validation issue as resolved."""
    result = db.resolve_validation_issue(issue_id)
    return jsonify(result)


# ==================== Manual Assets Routes ====================

@app.route('/api/manual-assets', methods=['GET'])
def api_get_manual_assets():
    """Get all manual assets for an investor."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    include_inactive = request.args.get('include_inactive', 'false').lower() == 'true'
    assets = db.get_manual_assets_by_investor(investor_id, include_inactive)
    return jsonify(assets)


@app.route('/api/manual-assets/summary', methods=['GET'])
def api_get_manual_assets_summary():
    """Get manual assets summary by asset class."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    summary = db.get_manual_assets_summary(investor_id)
    return jsonify(summary)


@app.route('/api/manual-assets/combined', methods=['GET'])
def api_get_combined_portfolio():
    """Get combined portfolio value (MF + manual assets)."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

    combined = db.get_combined_portfolio_value(investor_id)
    return jsonify(combined)


@app.route('/api/manual-assets', methods=['POST'])
def api_create_manual_asset():
    """Create a new manual asset."""
    data = request.json

    required = ['investor_id', 'asset_type', 'asset_class', 'name']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    # Validate asset_class
    valid_classes = ['equity', 'debt', 'commodity', 'cash', 'others']
    if data['asset_class'] not in valid_classes:
        return jsonify({'error': f'asset_class must be one of: {valid_classes}'}), 400

    # Validate asset_type
    valid_types = ['fd', 'sgb', 'stock', 'ppf', 'nps', 'other']
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
            ppf_maturity_date=data.get('ppf_maturity_date')
        )
        return jsonify({'success': True, 'id': asset_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/manual-assets/<int:asset_id>', methods=['GET'])
def api_get_manual_asset(asset_id):
    """Get a single manual asset."""
    asset = db.get_manual_asset(asset_id)
    if not asset:
        return jsonify({'error': 'Asset not found'}), 404
    return jsonify(asset)


@app.route('/api/manual-assets/<int:asset_id>', methods=['PUT'])
def api_update_manual_asset(asset_id):
    """Update a manual asset."""
    data = request.json
    result = db.update_manual_asset(asset_id, **data)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@app.route('/api/manual-assets/<int:asset_id>', methods=['DELETE'])
def api_delete_manual_asset(asset_id):
    """Delete a manual asset."""
    result = db.delete_manual_asset(asset_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify({'error': 'Asset not found'}), 404


@app.route('/api/manual-assets/calculate/fd', methods=['POST'])
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


@app.route('/api/manual-assets/calculate/sgb', methods=['POST'])
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


@app.route('/manual-assets')
def page_manual_assets():
    """Manual assets management page."""
    investor_id = request.args.get('investor_id', type=int)
    if not investor_id:
        return redirect('/')
    return render_template('manual_assets.html', investor_id=investor_id)


# ==================== FD CSV Upload Routes ====================

@app.route('/api/fd/upload-csv', methods=['POST'])
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


@app.route('/api/fd/import', methods=['POST'])
def api_import_fd():
    """Import validated FD rows into database."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    investor_id = data.get('investor_id')
    rows = data.get('rows', [])

    if not investor_id:
        return jsonify({'error': 'investor_id is required'}), 400

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
                    from datetime import datetime
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
            from datetime import datetime
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return date_str  # Return as-is if can't parse


@app.route('/api/fd/maturing', methods=['GET'])
def api_get_maturing_fds():
    """Get FDs maturing within next N days."""
    days = request.args.get('days', 30, type=int)
    fds = db.get_maturing_fds(days)
    return jsonify(fds)


@app.route('/api/fd/matured', methods=['GET'])
def api_get_matured_fds():
    """Get FDs past maturity that aren't closed."""
    fds = db.get_matured_fds()
    return jsonify(fds)


@app.route('/api/fd/<int:asset_id>/close', methods=['POST'])
def api_close_fd(asset_id):
    """Mark FD as closed with money received status."""
    data = request.get_json() or {}
    money_received = data.get('money_received', True)
    success = db.close_fd(asset_id, money_received)
    if success:
        return jsonify({'success': True})
    return jsonify({'error': 'FD not found'}), 404


# ==================== NPS Routes ====================

@app.route('/nps')
def page_nps():
    """NPS management page."""
    return render_template('nps.html')


@app.route('/nps/<int:subscriber_id>')
def page_nps_subscriber(subscriber_id):
    """NPS subscriber detail page."""
    subscriber = db.get_nps_subscriber(subscriber_id=subscriber_id)
    if not subscriber:
        return redirect('/nps')
    return render_template('nps_subscriber.html', subscriber=subscriber)


@app.route('/api/nps/subscribers', methods=['GET'])
def api_get_nps_subscribers():
    """Get all NPS subscribers."""
    investor_id = request.args.get('investor_id', type=int)
    if investor_id:
        subscribers = db.get_nps_subscribers_by_investor(investor_id)
    else:
        subscribers = db.get_all_nps_subscribers()
    return jsonify(subscribers)


@app.route('/api/nps/subscribers/<int:subscriber_id>', methods=['GET'])
def api_get_nps_subscriber(subscriber_id):
    """Get a single NPS subscriber."""
    subscriber = db.get_nps_subscriber(subscriber_id=subscriber_id)
    if not subscriber:
        return jsonify({'error': 'Subscriber not found'}), 404
    return jsonify(subscriber)


@app.route('/api/nps/subscribers/<int:subscriber_id>/schemes', methods=['GET'])
def api_get_nps_schemes(subscriber_id):
    """Get NPS schemes for a subscriber."""
    schemes = db.get_nps_schemes(subscriber_id)
    return jsonify(schemes)


@app.route('/api/nps/subscribers/<int:subscriber_id>/transactions', methods=['GET'])
def api_get_nps_transactions(subscriber_id):
    """Get NPS transactions for a subscriber."""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    scheme_type = request.args.get('scheme_type')
    contribution_type = request.args.get('contribution_type')

    if scheme_type:
        transactions = db.get_nps_transactions_by_scheme(subscriber_id, scheme_type)
    elif contribution_type:
        transactions = db.get_nps_transactions_by_contribution(subscriber_id, contribution_type)
    else:
        transactions = db.get_nps_transactions(subscriber_id, limit, offset)

    return jsonify(transactions)


@app.route('/api/nps/transactions/<int:transaction_id>/notes', methods=['PUT'])
def api_update_nps_transaction_notes(transaction_id):
    """Update notes for an NPS transaction."""
    data = request.json
    notes = data.get('notes', '')

    result = db.update_nps_transaction_notes(transaction_id, notes)
    if result.get('success'):
        return jsonify(result)
    return jsonify({'error': 'Transaction not found'}), 404


@app.route('/api/nps/transactions/<int:transaction_id>', methods=['GET'])
def api_get_nps_transaction(transaction_id):
    """Get a single NPS transaction."""
    transaction = db.get_nps_transaction(transaction_id)
    if not transaction:
        return jsonify({'error': 'Transaction not found'}), 404
    return jsonify(transaction)


@app.route('/api/nps/subscribers/<int:subscriber_id>/summary', methods=['GET'])
def api_get_nps_summary(subscriber_id):
    """Get NPS portfolio summary for a subscriber."""
    summary = db.get_nps_portfolio_summary(subscriber_id)
    return jsonify(summary)


@app.route('/api/nps/upload', methods=['POST'])
def api_upload_nps():
    """Upload and parse NPS statement PDF."""
    import sys
    import os

    # Add parent directory to path for imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from nps_parser import NPSParser

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'File must be a PDF'}), 400

    investor_id = request.form.get('investor_id', type=int)
    password = request.form.get('password', '')

    # Save uploaded file temporarily
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
        file.save(tmp.name)
        tmp_path = tmp.name

    try:
        # Parse the NPS statement
        parser = NPSParser(password=password if password else None)
        statement = parser.parse(tmp_path)

        # Import into database
        result = db.import_nps_statement(
            statement_data=statement.to_dict(),
            investor_id=investor_id
        )

        # Add validation info to result
        result['validation'] = {
            'is_valid': statement.validation.is_valid,
            'errors': statement.validation.errors,
            'warnings': statement.validation.warnings
        }

        return jsonify(result)

    except Exception as e:
        import traceback
        logger.error(f"NPS upload failed: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_path)
        except:
            pass


@app.route('/api/nps/link', methods=['POST'])
def api_link_nps_to_investor():
    """Link an NPS account to an investor."""
    data = request.json
    pran = data.get('pran')
    investor_id = data.get('investor_id')

    if not pran or not investor_id:
        return jsonify({'error': 'pran and investor_id are required'}), 400

    result = db.link_nps_to_investor(pran, investor_id)
    return jsonify(result)


@app.route('/api/nps/nav/<pfm_name>/<scheme_type>', methods=['GET'])
def api_get_nps_nav(pfm_name, scheme_type):
    """Get NAV history for an NPS scheme."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    history = db.get_nps_nav_history(pfm_name, scheme_type, start_date, end_date)
    return jsonify(history)


@app.route('/api/nps/nav', methods=['POST'])
def api_save_nps_nav():
    """Save NPS NAV data."""
    data = request.json

    required = ['pfm_name', 'scheme_type', 'nav_date', 'nav']
    for field in required:
        if not data.get(field):
            return jsonify({'error': f'{field} is required'}), 400

    success = db.save_nps_nav(
        pfm_name=data['pfm_name'],
        scheme_type=data['scheme_type'],
        nav_date=data['nav_date'],
        nav=data['nav']
    )

    return jsonify({'success': success})


@app.route('/api/nps/unmapped', methods=['GET'])
def api_get_unmapped_nps():
    """Get all NPS accounts not linked to any investor."""
    subscribers = db.get_unmapped_nps_subscribers()
    return jsonify(subscribers)


@app.route('/api/nps/unlink', methods=['POST'])
def api_unlink_nps():
    """Unlink an NPS account from its investor."""
    data = request.json
    pran = data.get('pran')

    if not pran:
        return jsonify({'error': 'pran is required'}), 400

    result = db.unlink_nps_from_investor(pran)
    return jsonify(result)


# ==================== Feature Requests ====================

@app.route('/api/feature-requests', methods=['GET'])
def api_get_feature_requests():
    """Get all feature requests."""
    return jsonify(db.get_feature_requests())


@app.route('/api/feature-requests', methods=['POST'])
def api_create_feature_request():
    """Create a new feature request."""
    data = request.json
    title = (data.get('title') or '').strip()
    if not title:
        return jsonify({'error': 'Title is required'}), 400
    req_id = db.create_feature_request(
        page=data.get('page', ''),
        title=title,
        description=data.get('description', '')
    )
    return jsonify({'success': True, 'id': req_id})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
