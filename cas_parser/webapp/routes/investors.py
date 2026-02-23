from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db

investors_bp = Blueprint('investors', __name__)


@investors_bp.route('/api/investors', methods=['GET'])
def api_get_investors():
    """Get all investors."""
    investors = db.get_all_investors()
    return jsonify(investors)


@investors_bp.route('/api/investors', methods=['POST'])
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
    investor = db.get_investor_by_id(investor_id)
    if not investor:
        return jsonify({'error': 'Investor not found'}), 404
    return jsonify(investor)


@investors_bp.route('/api/investors/<int:investor_id>', methods=['PUT'])
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


@investors_bp.route('/api/investors/<int:investor_id>/holdings', methods=['GET'])
def api_get_investor_holdings(investor_id):
    """Get all holdings for an investor."""
    holdings = db.get_holdings_by_investor(investor_id)
    return jsonify(holdings)


@investors_bp.route('/api/investors/<int:investor_id>/transactions', methods=['GET'])
def api_get_investor_transactions(investor_id):
    """Get transactions for an investor."""
    limit = request.args.get('limit', 500, type=int)
    transactions = db.get_transactions_by_investor(investor_id, limit=limit)
    return jsonify(transactions)


@investors_bp.route('/api/investors/<int:investor_id>/stats', methods=['GET'])
def api_get_investor_stats(investor_id):
    """Get statistics for an investor."""
    stats = db.get_transaction_stats(investor_id)
    return jsonify(stats)


@investors_bp.route('/api/investors/<int:investor_id>/holdings-live', methods=['GET'])
def api_get_investor_holdings_live(investor_id):
    """Get all holdings for an investor with live NAV."""
    holdings = db.get_holdings_by_investor(investor_id)
    holdings = db.get_nav_for_holdings(holdings)
    return jsonify(holdings)


@investors_bp.route('/api/investors/<int:investor_id>/alerts', methods=['GET'])
def api_get_investor_alerts(investor_id):
    """Get aggregated alerts for an investor."""
    return jsonify(db.get_investor_alerts(investor_id))


@investors_bp.route('/api/investors/<int:investor_id>/asset-allocation', methods=['GET'])
def api_get_asset_allocation(investor_id):
    """Get portfolio-level asset allocation."""
    allocation = db.get_portfolio_asset_allocation(investor_id)
    return jsonify(allocation)


@investors_bp.route('/api/investors/<int:investor_id>/notes-timeline', methods=['GET'])
def api_get_notes_timeline(investor_id):
    """Get a timeline of all notes across all goals for an investor."""
    limit = request.args.get('limit', 100, type=int)
    notes = db.get_goal_notes_timeline(investor_id, limit=limit)
    return jsonify(notes)


@investors_bp.route('/api/investors/<int:investor_id>/tax-harvesting', methods=['GET'])
def api_get_tax_harvesting(investor_id):
    """Compute tax-loss harvesting analysis."""
    tax_slab = request.args.get('tax_slab', type=float)
    result = db.compute_tax_harvesting(investor_id, tax_slab)
    return jsonify(result)


@investors_bp.route('/api/investors/<int:investor_id>/tax-slab', methods=['PUT'])
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
