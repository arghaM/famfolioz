import os
import tempfile
from flask import Blueprint, jsonify, request
from cas_parser.main import parse_cas_pdf
from cas_parser.webapp import data as db

transactions_bp = Blueprint('transactions', __name__)


@transactions_bp.route('/api/parse', methods=['POST'])
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


@transactions_bp.route('/api/transactions/<int:tx_id>', methods=['GET'])
def api_get_transaction(tx_id):
    """Get a single transaction."""
    tx = db.get_transaction_by_id(tx_id)
    if not tx:
        return jsonify({'error': 'Transaction not found'}), 404
    tx['version_count'] = db.get_transaction_version_count(tx_id)
    return jsonify(tx)


@transactions_bp.route('/api/transactions/<int:tx_id>', methods=['PUT'])
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


@transactions_bp.route('/api/transactions/<int:tx_id>/versions', methods=['GET'])
def api_get_transaction_versions(tx_id):
    """Get all versions of a transaction."""
    versions = db.get_transaction_versions(tx_id)
    return jsonify(versions)


@transactions_bp.route('/api/conflicts', methods=['GET'])
def api_get_conflicts():
    """Get all pending conflict groups."""
    groups = db.get_pending_conflict_groups()
    return jsonify(groups)


@transactions_bp.route('/api/conflicts/stats', methods=['GET'])
def api_get_conflict_stats():
    """Get conflict statistics."""
    stats = db.get_conflict_stats()
    return jsonify(stats)


@transactions_bp.route('/api/conflicts/<conflict_group_id>', methods=['GET'])
def api_get_conflict_group(conflict_group_id):
    """Get transactions in a conflict group."""
    transactions = db.get_conflict_group_transactions(conflict_group_id)
    return jsonify(transactions)


@transactions_bp.route('/api/conflicts/<conflict_group_id>/resolve', methods=['POST'])
def api_resolve_conflict(conflict_group_id):
    """Resolve a conflict by selecting which transactions to keep."""
    data = request.json
    selected_hashes = data.get('selected_hashes', [])

    if not selected_hashes:
        return jsonify({'error': 'At least one transaction must be selected'}), 400

    result = db.resolve_conflict(conflict_group_id, selected_hashes)
    return jsonify(result)
