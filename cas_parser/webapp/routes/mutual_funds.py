import json
import urllib.request
import urllib.parse
from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db
from cas_parser.webapp.auth import admin_required

mutual_funds_bp = Blueprint('mutual_funds', __name__)


@mutual_funds_bp.route('/api/mutual-funds', methods=['GET'])
def api_get_mutual_funds():
    """Get all mutual funds."""
    funds = db.get_all_mutual_funds()
    return jsonify(funds)


@mutual_funds_bp.route('/api/mutual-funds/unmapped', methods=['GET'])
def api_get_unmapped_mutual_funds():
    """Get unmapped mutual funds."""
    funds = db.get_unmapped_mutual_funds()
    return jsonify(funds)


@mutual_funds_bp.route('/api/mutual-funds/stats', methods=['GET'])
def api_get_mutual_fund_stats():
    """Get mutual fund statistics."""
    stats = db.get_mutual_fund_stats()
    return jsonify(stats)


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/map', methods=['POST'])
@admin_required
def api_map_mutual_fund(mf_id):
    """Map a mutual fund to AMFI code."""
    data = request.json
    amfi_code = data.get('amfi_code', '').strip()
    amfi_scheme_name = data.get('amfi_scheme_name', '')

    if not amfi_code:
        return jsonify({'error': 'AMFI code is required'}), 400

    success = db.map_mutual_fund_to_amfi(mf_id, amfi_code, amfi_scheme_name)
    return jsonify({'success': success})


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/name', methods=['PUT'])
@admin_required
def api_update_fund_name(mf_id):
    """Update display name for a mutual fund."""
    data = request.json
    display_name = data.get('display_name', '').strip()

    success = db.update_fund_display_name(mf_id, display_name)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Fund not found'}), 404


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/allocation', methods=['POST'])
@admin_required
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


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/classification', methods=['PUT'])
@admin_required
def api_update_fund_classification(mf_id):
    """Update fund category and geography classification."""
    data = request.json
    fund_category = data.get('fund_category') or None
    geography = data.get('geography') or None

    result = db.update_fund_classification(mf_id, fund_category, geography)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/detail', methods=['GET'])
def api_get_fund_detail(mf_id):
    """Return fund with holdings and sectors arrays."""
    fund = db.get_fund_detail(mf_id)
    if not fund:
        return jsonify({'error': 'Fund not found'}), 404
    return jsonify(fund)


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/holdings', methods=['PUT'])
@admin_required
def api_update_fund_holdings(mf_id):
    """Replace all stock holdings for a fund."""
    data = request.json
    holdings = data.get('holdings', [])
    result = db.update_fund_holdings(mf_id, holdings)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/sectors', methods=['PUT'])
@admin_required
def api_update_fund_sectors(mf_id):
    """Replace all sector allocations for a fund."""
    data = request.json
    sectors = data.get('sectors', [])
    result = db.update_fund_sectors(mf_id, sectors)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/review', methods=['PUT'])
@admin_required
def api_confirm_fund_review(mf_id):
    """Confirm allocation review for a fund (resets 30-day timer)."""
    success = db.confirm_fund_allocation_review(mf_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Fund not found'}), 404


@mutual_funds_bp.route('/api/mutual-funds/review-alerts', methods=['GET'])
def api_get_review_alerts():
    """Get funds needing allocation review (30+ days since last review)."""
    days = request.args.get('days', 30, type=int)
    funds = db.get_funds_needing_review(days)
    return jsonify({'count': len(funds), 'funds': funds})


@mutual_funds_bp.route('/api/amfi/search', methods=['GET'])
def api_search_amfi():
    """Search AMFI schemes."""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify([])

    results = db.search_amfi_schemes(query)
    return jsonify(results)


@mutual_funds_bp.route('/api/mutual-funds/<int:mf_id>/exit-load', methods=['PUT'])
@admin_required
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


# ==================== ISIN Resolver Routes ====================

@mutual_funds_bp.route('/api/isin-resolver/refresh-amfi', methods=['POST'])
@admin_required
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


@mutual_funds_bp.route('/api/isin-resolver/status', methods=['GET'])
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


@mutual_funds_bp.route('/api/isin-resolver/mappings', methods=['GET'])
def api_get_isin_mappings():
    """Get all manual ISIN mappings."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        mappings = get_isin_resolver().get_manual_mappings()
        return jsonify(mappings)
    except Exception as e:
        return jsonify({}), 500


@mutual_funds_bp.route('/api/isin-resolver/mappings', methods=['POST'])
@admin_required
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


@mutual_funds_bp.route('/api/isin-resolver/mappings/<path:scheme_pattern>', methods=['DELETE'])
@admin_required
def api_delete_isin_mapping(scheme_pattern):
    """Delete a manual ISIN mapping."""
    try:
        from cas_parser.isin_resolver import get_isin_resolver
        success = get_isin_resolver().remove_manual_mapping(scheme_pattern)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@mutual_funds_bp.route('/api/isin-resolver/resolve', methods=['POST'])
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


@mutual_funds_bp.route('/api/mutual-funds/unresolved-isins', methods=['GET'])
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


@mutual_funds_bp.route('/api/isin-resolver/search', methods=['GET'])
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


@mutual_funds_bp.route('/api/mutual-funds/<int:fund_id>/update-isin', methods=['POST'])
@admin_required
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
