from datetime import date, datetime

from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db
from cas_parser.webapp.xirr import xirr, build_cashflows_for_folio
from cas_parser.webapp.auth import check_investor_access, get_investor_id_for_goal, get_investor_id_for_note

goals_bp = Blueprint('goals', __name__)


@goals_bp.route('/api/investors/<int:investor_id>/goals', methods=['GET'])
def api_get_goals(investor_id):
    """Get all goals for an investor."""
    check_investor_access(investor_id)
    goals = db.get_goals_by_investor(investor_id)
    return jsonify(goals)


@goals_bp.route('/api/investors/<int:investor_id>/goals', methods=['POST'])
def api_create_goal(investor_id):
    """Create a new goal."""
    check_investor_access(investor_id)
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


@goals_bp.route('/api/goals/<int:goal_id>', methods=['GET'])
def api_get_goal(goal_id):
    """Get a single goal with details."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404
    return jsonify(goal)


@goals_bp.route('/api/goals/<int:goal_id>', methods=['PUT'])
def api_update_goal(goal_id):
    """Update a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
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


@goals_bp.route('/api/goals/<int:goal_id>', methods=['DELETE'])
def api_delete_goal(goal_id):
    """Delete a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    result = db.delete_goal(goal_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/link', methods=['POST'])
def api_link_folio_to_goal(goal_id):
    """Link a folio to a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    folio_id = data.get('folio_id')

    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400

    result = db.link_folio_to_goal(goal_id, folio_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@goals_bp.route('/api/goals/<int:goal_id>/unlink', methods=['POST'])
def api_unlink_folio_from_goal(goal_id):
    """Unlink a folio from a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    folio_id = data.get('folio_id')

    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400

    result = db.unlink_folio_from_goal(goal_id, folio_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/available-folios', methods=['GET'])
def api_get_available_folios(goal_id):
    """Get folios that can be linked to this goal."""
    investor_id = get_investor_id_for_goal(goal_id)
    check_investor_access(investor_id)

    folios = db.get_unlinked_folios_for_goal(goal_id, investor_id)
    return jsonify(folios)


# ==================== Goal-Asset Linking Routes ====================

@goals_bp.route('/api/goals/<int:goal_id>/link-asset', methods=['POST'])
def api_link_asset_to_goal(goal_id):
    """Link a manual asset to a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    asset_id = data.get('asset_id')

    if not asset_id:
        return jsonify({'error': 'asset_id is required'}), 400

    result = db.link_asset_to_goal(goal_id, asset_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@goals_bp.route('/api/goals/<int:goal_id>/unlink-asset', methods=['POST'])
def api_unlink_asset_from_goal(goal_id):
    """Unlink a manual asset from a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    asset_id = data.get('asset_id')

    if not asset_id:
        return jsonify({'error': 'asset_id is required'}), 400

    result = db.unlink_asset_from_goal(goal_id, asset_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/available-assets', methods=['GET'])
def api_get_available_assets(goal_id):
    """Get manual assets that can be linked to this goal."""
    investor_id = get_investor_id_for_goal(goal_id)
    check_investor_access(investor_id)

    assets = db.get_unlinked_assets_for_goal(goal_id, investor_id)
    return jsonify(assets)


# ==================== Goal-NPS Linking Routes ====================

@goals_bp.route('/api/goals/<int:goal_id>/link-nps', methods=['POST'])
def api_link_nps_to_goal(goal_id):
    """Link an NPS subscriber account to a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    subscriber_id = data.get('subscriber_id')

    if not subscriber_id:
        return jsonify({'error': 'subscriber_id is required'}), 400

    result = db.link_nps_to_goal(goal_id, subscriber_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@goals_bp.route('/api/goals/<int:goal_id>/unlink-nps', methods=['POST'])
def api_unlink_nps_from_goal(goal_id):
    """Unlink an NPS subscriber account from a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    subscriber_id = data.get('subscriber_id')

    if not subscriber_id:
        return jsonify({'error': 'subscriber_id is required'}), 400

    result = db.unlink_nps_from_goal(goal_id, subscriber_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/available-nps', methods=['GET'])
def api_get_available_nps(goal_id):
    """Get NPS subscriber accounts that can be linked to this goal."""
    investor_id = get_investor_id_for_goal(goal_id)
    check_investor_access(investor_id)

    accounts = db.get_unlinked_nps_for_goal(goal_id, investor_id)
    return jsonify(accounts)


# ==================== Goal Notes Routes ====================

@goals_bp.route('/api/goals/<int:goal_id>/notes', methods=['GET'])
def api_get_goal_notes(goal_id):
    """Get all notes for a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    limit = request.args.get('limit', 50, type=int)
    notes = db.get_goal_notes(goal_id, limit=limit)
    return jsonify(notes)


@goals_bp.route('/api/goals/<int:goal_id>/notes', methods=['POST'])
def api_create_goal_note(goal_id):
    """Create a new note for a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
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


@goals_bp.route('/api/notes/<int:note_id>', methods=['GET'])
def api_get_note(note_id):
    """Get a single note by ID."""
    check_investor_access(get_investor_id_for_note(note_id))
    note = db.get_goal_note_by_id(note_id)
    if not note:
        return jsonify({'error': 'Note not found'}), 404
    return jsonify(note)


@goals_bp.route('/api/notes/<int:note_id>', methods=['PUT'])
def api_update_note(note_id):
    """Update a note."""
    check_investor_access(get_investor_id_for_note(note_id))
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


@goals_bp.route('/api/notes/<int:note_id>', methods=['DELETE'])
def api_delete_note(note_id):
    """Delete a note."""
    check_investor_access(get_investor_id_for_note(note_id))
    result = db.delete_goal_note(note_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/allocation-detail', methods=['GET'])
def api_get_goal_allocation_detail(goal_id):
    """Get detailed per-fund allocation breakdown for a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404
    detail = db.get_goal_allocation_detail(goal_id)
    return jsonify(detail)


@goals_bp.route('/api/goals/<int:goal_id>/phases', methods=['GET'])
def api_get_goal_phases(goal_id):
    """Get all phases with equity sub-allocations for a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    phases = db.get_goal_phases(goal_id)
    return jsonify(phases)


@goals_bp.route('/api/goals/<int:goal_id>/phases', methods=['PUT'])
def api_save_goal_phases(goal_id):
    """Save/replace all phases for a goal."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    data = request.json
    phases_data = data.get('phases', [])
    result = db.save_goal_phases(goal_id, phases_data)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 400


@goals_bp.route('/api/goals/<int:goal_id>/xirr', methods=['GET'])
def api_get_goal_xirr(goal_id):
    """Compute XIRR for a goal -- per-fund + goal-level aggregate."""
    check_investor_access(get_investor_id_for_goal(goal_id))
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404

    linked_folios = goal.get('linked_folios', [])
    linked_assets = goal.get('linked_assets', [])
    linked_nps = goal.get('linked_nps', [])

    # Report excluded (non-XIRR) manual assets
    excluded_assets = [a for a in linked_assets if a.get('exclude_from_xirr')]

    if not linked_folios:
        return jsonify({
            'goal_xirr': None, 'funds': [],
            'message': 'No linked mutual fund investments',
            'excluded_assets': excluded_assets,
            'linked_nps': linked_nps,
        })

    today = date.today()
    all_cashflows = []
    funds = []

    for lf in linked_folios:
        folio_id = lf['folio_id']
        data = db.get_xirr_data_for_folio(folio_id)
        transactions = data['transactions']

        if not transactions:
            continue

        cashflows = build_cashflows_for_folio(transactions, data['current_value'])
        xirr_val = xirr(cashflows)

        # Determine tenure from first transaction date
        first_tx_date = None
        for tx in transactions:
            tx_date = tx['tx_date']
            if isinstance(tx_date, str):
                tx_date = datetime.strptime(tx_date, '%Y-%m-%d').date()
            if first_tx_date is None or tx_date < first_tx_date:
                first_tx_date = tx_date

        tenure_days = (today - first_tx_date).days if first_tx_date else 0
        tenure_years = tenure_days / 365.25
        is_young = tenure_years < 1.0

        fund_entry = {
            'folio_id': folio_id,
            'scheme_name': data['scheme_name'],
            'folio_number': data['folio_number'],
            'isin': data.get('isin'),
            'current_value': data['current_value'],
            'xirr': round(xirr_val * 100, 2) if xirr_val is not None else None,
            'cashflow_count': len(cashflows),
            'first_tx_date': str(first_tx_date) if first_tx_date else None,
            'tenure_days': tenure_days,
            'tenure_years': round(tenure_years, 1),
            'is_young': is_young,
        }
        funds.append(fund_entry)

        # Include in goal-level aggregate only if XIRR computed successfully
        if xirr_val is not None:
            all_cashflows.extend(cashflows)

    # Goal-level XIRR from all cashflows combined
    goal_xirr_val = xirr(all_cashflows) if all_cashflows else None

    # Also compute XIRR excluding young funds (< 1 year)
    mature_cashflows = []
    for lf in linked_folios:
        folio_id = lf['folio_id']
        fund_entry = next((f for f in funds if f['folio_id'] == folio_id), None)
        if fund_entry and not fund_entry['is_young'] and fund_entry['xirr'] is not None:
            data = db.get_xirr_data_for_folio(folio_id)
            cfs = build_cashflows_for_folio(data['transactions'], data['current_value'])
            mature_cashflows.extend(cfs)

    mature_xirr_val = xirr(mature_cashflows) if mature_cashflows else None

    # Sort by current value descending
    funds.sort(key=lambda f: f['current_value'] or 0, reverse=True)

    return jsonify({
        'goal_xirr': round(goal_xirr_val * 100, 2) if goal_xirr_val is not None else None,
        'goal_xirr_mature_only': round(mature_xirr_val * 100, 2) if mature_xirr_val is not None else None,
        'total_value': goal.get('current_value', 0),
        'funds': funds,
        'young_fund_count': sum(1 for f in funds if f['is_young']),
        'mature_fund_count': sum(1 for f in funds if not f['is_young']),
        'excluded_assets': excluded_assets,
        'linked_nps': linked_nps,
    })
