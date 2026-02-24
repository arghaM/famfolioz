from flask import Blueprint, jsonify, request, g
from cas_parser.webapp import data as db
from cas_parser.webapp.auth import admin_required, check_investor_access

investors_bp = Blueprint('investors', __name__)


@investors_bp.route('/api/investors', methods=['GET'])
def api_get_investors():
    """Get all investors (filtered by access for members)."""
    investors = db.get_all_investors()
    user = g.get('current_user')
    if user and user['role'] != 'admin':
        accessible = set(db.get_accessible_investor_ids(user['id']))
        investors = [i for i in investors if i['id'] in accessible]
    return jsonify(investors)


@investors_bp.route('/api/investors', methods=['POST'])
@admin_required
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


@investors_bp.route('/api/investors/<int:investor_id>', methods=['GET'])
def api_get_investor(investor_id):
    """Get investor details."""
    check_investor_access(investor_id)
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return jsonify({'error': 'Investor not found'}), 404
    return jsonify(investor)


@investors_bp.route('/api/investors/<int:investor_id>', methods=['PUT'])
def api_update_investor(investor_id):
    """Update investor details."""
    check_investor_access(investor_id)
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


@investors_bp.route('/api/investors/<int:investor_id>/holdings', methods=['GET'])
def api_get_investor_holdings(investor_id):
    """Get all holdings for an investor."""
    check_investor_access(investor_id)
    holdings = db.get_holdings_by_investor(investor_id)
    return jsonify(holdings)


@investors_bp.route('/api/investors/<int:investor_id>/transactions', methods=['GET'])
def api_get_investor_transactions(investor_id):
    """Get transactions for an investor."""
    check_investor_access(investor_id)
    limit = request.args.get('limit', 500, type=int)
    transactions = db.get_transactions_by_investor(investor_id, limit=limit)
    return jsonify(transactions)


@investors_bp.route('/api/investors/<int:investor_id>/stats', methods=['GET'])
def api_get_investor_stats(investor_id):
    """Get statistics for an investor."""
    check_investor_access(investor_id)
    stats = db.get_transaction_stats(investor_id)
    return jsonify(stats)


@investors_bp.route('/api/investors/<int:investor_id>/holdings-live', methods=['GET'])
def api_get_investor_holdings_live(investor_id):
    """Get all holdings for an investor with live NAV."""
    check_investor_access(investor_id)
    holdings = db.get_holdings_by_investor(investor_id)
    holdings = db.get_nav_for_holdings(holdings)
    return jsonify(holdings)


@investors_bp.route('/api/investors/<int:investor_id>/alerts', methods=['GET'])
def api_get_investor_alerts(investor_id):
    """Get aggregated alerts for an investor."""
    check_investor_access(investor_id)
    return jsonify(db.get_investor_alerts(investor_id))


@investors_bp.route('/api/investors/<int:investor_id>/asset-allocation', methods=['GET'])
def api_get_asset_allocation(investor_id):
    """Get portfolio-level asset allocation."""
    check_investor_access(investor_id)
    allocation = db.get_portfolio_asset_allocation(investor_id)
    return jsonify(allocation)


@investors_bp.route('/api/investors/<int:investor_id>/notes-timeline', methods=['GET'])
def api_get_notes_timeline(investor_id):
    """Get a timeline of all notes across all goals for an investor."""
    check_investor_access(investor_id)
    limit = request.args.get('limit', 100, type=int)
    notes = db.get_goal_notes_timeline(investor_id, limit=limit)
    return jsonify(notes)


@investors_bp.route('/api/investors/<int:investor_id>/tax-harvesting', methods=['GET'])
def api_get_tax_harvesting(investor_id):
    """Compute tax-loss harvesting analysis."""
    check_investor_access(investor_id)
    tax_slab = request.args.get('tax_slab', type=float)
    result = db.compute_tax_harvesting(investor_id, tax_slab)
    return jsonify(result)


@investors_bp.route('/api/investors/<int:investor_id>/tax-slab', methods=['PUT'])
def api_update_tax_slab(investor_id):
    """Update investor's tax slab percentage."""
    check_investor_access(investor_id)
    data = request.json
    tax_slab_pct = data.get('tax_slab_pct')
    if tax_slab_pct is None:
        return jsonify({'error': 'tax_slab_pct is required'}), 400
    result = db.update_investor_tax_slab(investor_id, float(tax_slab_pct))
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404


@investors_bp.route('/api/investors/sync-status', methods=['GET'])
@admin_required
def api_get_sync_status():
    """Get last transaction date and CAS upload info per investor."""
    from cas_parser.webapp.db.connection import get_db
    with get_db() as conn:
        rows = conn.execute("""
            SELECT i.id, i.name, i.last_cas_upload,
                   MAX(t.tx_date) as last_tx_date,
                   COUNT(t.id) as tx_count
            FROM investors i
            LEFT JOIN folios f ON f.investor_id = i.id
            LEFT JOIN transactions t ON t.folio_id = f.id AND t.status = 'active'
            GROUP BY i.id
            ORDER BY i.name
        """).fetchall()
        return jsonify([dict(r) for r in rows])
