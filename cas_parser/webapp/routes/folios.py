from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db
from cas_parser.webapp.auth import admin_required, check_investor_access, get_investor_id_for_folio

folios_bp = Blueprint('folios', __name__)


@folios_bp.route('/api/folios/<int:folio_id>/transactions', methods=['GET'])
def api_get_folio_transactions(folio_id):
    """Get all transactions for a folio."""
    investor_id = get_investor_id_for_folio(folio_id)
    if investor_id:
        check_investor_access(investor_id)
    transactions = db.get_transactions_by_folio(folio_id)
    return jsonify(transactions)


@folios_bp.route('/api/folios/all', methods=['GET'])
@admin_required
def api_get_all_folios():
    """Get all folios with investor assignment info."""
    folios = db.get_all_folios_with_assignments()
    return jsonify(folios)


@folios_bp.route('/api/unmapped-folios', methods=['GET'])
@admin_required
def api_get_unmapped_folios():
    """Get all unmapped folios."""
    folios = db.get_unmapped_folios()
    return jsonify(folios)


@folios_bp.route('/api/map-folios', methods=['POST'])
@admin_required
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


@folios_bp.route('/api/folios/<int:folio_id>/info', methods=['GET'])
def api_get_folio_info(folio_id):
    """Get folio metadata with investor and holdings info."""
    investor_id = get_investor_id_for_folio(folio_id)
    if investor_id:
        check_investor_access(investor_id)
    folio = db.get_folio_by_id(folio_id)
    if not folio:
        return jsonify({'error': 'Folio not found'}), 404
    return jsonify(folio)


@folios_bp.route('/api/unmap-folio', methods=['POST'])
@admin_required
def api_unmap_folio():
    """Remove investor mapping from a folio."""
    data = request.json
    folio_id = data.get('folio_id')
    if not folio_id:
        return jsonify({'error': 'folio_id is required'}), 400
    db.unmap_folio(folio_id)
    return jsonify({'success': True})
