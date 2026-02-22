import os
import tempfile
from flask import Blueprint, jsonify, request
from cas_parser.webapp import data as db

import logging
logger = logging.getLogger(__name__)

nps_bp = Blueprint('nps', __name__)


@nps_bp.route('/api/nps/subscribers', methods=['GET'])
def api_get_nps_subscribers():
    """Get all NPS subscribers."""
    investor_id = request.args.get('investor_id', type=int)
    if investor_id:
        subscribers = db.get_nps_subscribers_by_investor(investor_id)
    else:
        subscribers = db.get_all_nps_subscribers()
    return jsonify(subscribers)


@nps_bp.route('/api/nps/subscribers/<int:subscriber_id>', methods=['GET'])
def api_get_nps_subscriber(subscriber_id):
    """Get a single NPS subscriber."""
    subscriber = db.get_nps_subscriber(subscriber_id=subscriber_id)
    if not subscriber:
        return jsonify({'error': 'Subscriber not found'}), 404
    return jsonify(subscriber)


@nps_bp.route('/api/nps/subscribers/<int:subscriber_id>/schemes', methods=['GET'])
def api_get_nps_schemes(subscriber_id):
    """Get NPS schemes for a subscriber."""
    schemes = db.get_nps_schemes(subscriber_id)
    return jsonify(schemes)


@nps_bp.route('/api/nps/subscribers/<int:subscriber_id>/transactions', methods=['GET'])
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


@nps_bp.route('/api/nps/transactions/<int:transaction_id>/notes', methods=['PUT'])
def api_update_nps_transaction_notes(transaction_id):
    """Update notes for an NPS transaction."""
    data = request.json
    notes = data.get('notes', '')

    result = db.update_nps_transaction_notes(transaction_id, notes)
    if result.get('success'):
        return jsonify(result)
    return jsonify({'error': 'Transaction not found'}), 404


@nps_bp.route('/api/nps/transactions/<int:transaction_id>', methods=['GET'])
def api_get_nps_transaction(transaction_id):
    """Get a single NPS transaction."""
    transaction = db.get_nps_transaction(transaction_id)
    if not transaction:
        return jsonify({'error': 'Transaction not found'}), 404
    return jsonify(transaction)


@nps_bp.route('/api/nps/subscribers/<int:subscriber_id>/summary', methods=['GET'])
def api_get_nps_summary(subscriber_id):
    """Get NPS portfolio summary for a subscriber."""
    summary = db.get_nps_portfolio_summary(subscriber_id)
    return jsonify(summary)


@nps_bp.route('/api/nps/upload', methods=['POST'])
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


@nps_bp.route('/api/nps/link', methods=['POST'])
def api_link_nps_to_investor():
    """Link an NPS account to an investor."""
    data = request.json
    pran = data.get('pran')
    investor_id = data.get('investor_id')

    if not pran or not investor_id:
        return jsonify({'error': 'pran and investor_id are required'}), 400

    result = db.link_nps_to_investor(pran, investor_id)
    return jsonify(result)


@nps_bp.route('/api/nps/nav/<pfm_name>/<scheme_type>', methods=['GET'])
def api_get_nps_nav(pfm_name, scheme_type):
    """Get NAV history for an NPS scheme."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    history = db.get_nps_nav_history(pfm_name, scheme_type, start_date, end_date)
    return jsonify(history)


@nps_bp.route('/api/nps/nav', methods=['POST'])
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


@nps_bp.route('/api/nps/unmapped', methods=['GET'])
def api_get_unmapped_nps():
    """Get all NPS accounts not linked to any investor."""
    subscribers = db.get_unmapped_nps_subscribers()
    return jsonify(subscribers)


@nps_bp.route('/api/nps/unlink', methods=['POST'])
def api_unlink_nps():
    """Unlink an NPS account from its investor."""
    data = request.json
    pran = data.get('pran')

    if not pran:
        return jsonify({'error': 'pran is required'}), 400

    result = db.unlink_nps_from_investor(pran)
    return jsonify(result)
