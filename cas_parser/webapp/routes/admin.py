import os
from pathlib import Path
from flask import Blueprint, jsonify, request, send_file, send_from_directory
from cas_parser.webapp import data as db

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/api/config/<key>', methods=['GET'])
def api_get_config(key):
    """Get a config value."""
    value = db.get_config(key)
    return jsonify({'key': key, 'value': value})


@admin_bp.route('/api/config/<key>', methods=['PUT'])
def api_set_config(key):
    """Set a config value."""
    data = request.json
    if not data or 'value' not in data:
        return jsonify({'error': 'Missing value'}), 400
    db.set_config(key, data['value'])
    return jsonify({'key': key, 'value': data['value']})


@admin_bp.route('/api/backup', methods=['POST'])
def api_backup():
    """Create a backup of static tables."""
    try:
        result = db.backup_static_tables()
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/api/backups', methods=['GET'])
def api_list_backups():
    """List all available backups."""
    backups = db.list_backups()
    return jsonify(backups)


@admin_bp.route('/api/restore', methods=['POST'])
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


@admin_bp.route('/api/backups/download/<filename>')
def api_download_backup(filename):
    """Download a backup file."""
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


@admin_bp.route('/api/reset-database', methods=['POST'])
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


@admin_bp.route('/api/quarantine', methods=['GET'])
def api_get_quarantine():
    """Get all quarantined items."""
    items = db.get_quarantined_items()
    return jsonify(items)


@admin_bp.route('/api/quarantine/summary', methods=['GET'])
def api_get_quarantine_summary():
    """Get summary of quarantined items grouped by partial ISIN."""
    summary = db.get_quarantine_summary()
    return jsonify(summary)


@admin_bp.route('/api/quarantine/stats', methods=['GET'])
def api_get_quarantine_stats():
    """Get quarantine statistics."""
    stats = db.get_quarantine_stats()
    return jsonify(stats)


@admin_bp.route('/api/quarantine/resolve', methods=['POST'])
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


@admin_bp.route('/api/quarantine/<partial_isin>', methods=['DELETE'])
def api_delete_quarantine(partial_isin):
    """Delete quarantined items for a partial ISIN."""
    result = db.delete_quarantine_items(partial_isin)
    return jsonify(result)


@admin_bp.route('/api/quarantine/delete', methods=['POST'])
def api_delete_quarantine_post():
    """Delete quarantined items (POST version for when scheme_name is needed)."""
    data = request.json
    partial_isin = data.get('partial_isin', '')
    scheme_name = data.get('scheme_name', '')

    result = db.delete_quarantine_items(partial_isin, scheme_name)
    return jsonify(result)


@admin_bp.route('/api/validation/issues', methods=['GET'])
def api_get_validation_issues():
    """Get all open validation issues."""
    investor_id = request.args.get('investor_id', type=int)
    status = request.args.get('status', 'open')
    issues = db.get_validation_issues(investor_id, status)
    return jsonify(issues)


@admin_bp.route('/api/validation/run', methods=['POST'])
def api_run_validation():
    """Run validation on all folios or for a specific investor."""
    data = request.json or {}
    investor_id = data.get('investor_id')
    result = db.run_post_import_validation(investor_id)
    return jsonify(result)


@admin_bp.route('/api/validation/folio/<int:folio_id>', methods=['GET'])
def api_validate_folio(folio_id):
    """Validate a specific folio."""
    result = db.validate_folio_units(folio_id)
    return jsonify(result)


@admin_bp.route('/api/validation/issues/<int:issue_id>/resolve', methods=['POST'])
def api_resolve_validation_issue(issue_id):
    """Mark a validation issue as resolved."""
    result = db.resolve_validation_issue(issue_id)
    return jsonify(result)


@admin_bp.route('/api/feature-requests', methods=['GET'])
def api_get_feature_requests():
    """Get all feature requests."""
    return jsonify(db.get_feature_requests())


@admin_bp.route('/api/feature-requests', methods=['POST'])
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
