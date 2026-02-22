from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db

goals_bp = Blueprint('goals', __name__)


@goals_bp.route('/api/investors/<int:investor_id>/goals', methods=['GET'])
def api_get_goals(investor_id):
    """Get all goals for an investor."""
    goals = db.get_goals_by_investor(investor_id)
    return jsonify(goals)


@goals_bp.route('/api/investors/<int:investor_id>/goals', methods=['POST'])
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


@goals_bp.route('/api/goals/<int:goal_id>', methods=['GET'])
def api_get_goal(goal_id):
    """Get a single goal with details."""
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404
    return jsonify(goal)


@goals_bp.route('/api/goals/<int:goal_id>', methods=['PUT'])
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


@goals_bp.route('/api/goals/<int:goal_id>', methods=['DELETE'])
def api_delete_goal(goal_id):
    """Delete a goal."""
    result = db.delete_goal(goal_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/link', methods=['POST'])
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


@goals_bp.route('/api/goals/<int:goal_id>/unlink', methods=['POST'])
def api_unlink_folio_from_goal(goal_id):
    """Unlink a folio from a goal."""
    data = request.json
    folio_id = data.get('folio_id')

    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400

    result = db.unlink_folio_from_goal(goal_id, folio_id)
    return jsonify(result)


@goals_bp.route('/api/goals/<int:goal_id>/available-folios', methods=['GET'])
def api_get_available_folios(goal_id):
    """Get folios that can be linked to this goal."""
    # Get the goal to find investor_id
    goal = db.get_goal_by_id(goal_id)
    if not goal:
        return jsonify({'error': 'Goal not found'}), 404

    folios = db.get_unlinked_folios_for_goal(goal_id, goal['investor_id'])
    return jsonify(folios)


# ==================== Goal Notes Routes ====================

@goals_bp.route('/api/goals/<int:goal_id>/notes', methods=['GET'])
def api_get_goal_notes(goal_id):
    """Get all notes for a goal."""
    limit = request.args.get('limit', 50, type=int)
    notes = db.get_goal_notes(goal_id, limit=limit)
    return jsonify(notes)


@goals_bp.route('/api/goals/<int:goal_id>/notes', methods=['POST'])
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


@goals_bp.route('/api/notes/<int:note_id>', methods=['GET'])
def api_get_note(note_id):
    """Get a single note by ID."""
    note = db.get_goal_note_by_id(note_id)
    if not note:
        return jsonify({'error': 'Note not found'}), 404
    return jsonify(note)


@goals_bp.route('/api/notes/<int:note_id>', methods=['PUT'])
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


@goals_bp.route('/api/notes/<int:note_id>', methods=['DELETE'])
def api_delete_note(note_id):
    """Delete a note."""
    result = db.delete_goal_note(note_id)
    return jsonify(result)
