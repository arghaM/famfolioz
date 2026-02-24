import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from flask import Blueprint, jsonify, request, send_file, send_from_directory
from cas_parser.webapp import data as db
from cas_parser.webapp.db.connection import DB_PATH, BACKUP_DIR, init_db
from cas_parser.webapp.auth import admin_required

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/api/config/<key>', methods=['GET'])
def api_get_config(key):
    """Get a config value."""
    value = db.get_config(key)
    return jsonify({'key': key, 'value': value})


@admin_bp.route('/api/config/<key>', methods=['PUT'])
@admin_required
def api_set_config(key):
    """Set a config value."""
    data = request.json
    if not data or 'value' not in data:
        return jsonify({'error': 'Missing value'}), 400
    db.set_config(key, data['value'])
    return jsonify({'key': key, 'value': data['value']})


@admin_bp.route('/api/backup', methods=['POST'])
@admin_required
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
@admin_required
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
    backup_dir = BACKUP_DIR
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


@admin_bp.route('/api/backup/full-db')
@admin_required
def api_download_full_db():
    """Download the entire database file as a backup."""
    if not DB_PATH.exists():
        return jsonify({'error': 'Database file not found'}), 404

    timestamp = datetime.now().strftime('%Y%m%d')
    return send_file(
        str(DB_PATH),
        as_attachment=True,
        download_name=f'famfolioz_backup_{timestamp}.db'
    )


@admin_bp.route('/api/backup/full-db/size')
def api_full_db_size():
    """Get the size of the database file."""
    if not DB_PATH.exists():
        return jsonify({'size_bytes': 0, 'size_mb': '0'})

    size_bytes = DB_PATH.stat().st_size
    size_mb = round(size_bytes / (1024 * 1024), 1)
    return jsonify({'size_bytes': size_bytes, 'size_mb': f'{size_mb}'})


@admin_bp.route('/api/restore/full-db', methods=['POST'])
@admin_required
def api_restore_full_db():
    """Restore from an uploaded .db file. Replaces the entire database."""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400

    uploaded = request.files['file']
    if not uploaded.filename or not uploaded.filename.endswith('.db'):
        return jsonify({'success': False, 'error': 'File must be a .db file'}), 400

    # Save to a temp location first for validation
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = BACKUP_DIR / f'_upload_temp_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'

    try:
        uploaded.save(str(temp_path))

        # Validate: check it's a valid SQLite file
        try:
            conn = sqlite3.connect(str(temp_path))
            conn.execute('SELECT count(*) FROM sqlite_master')
            conn.close()
        except Exception:
            temp_path.unlink(missing_ok=True)
            return jsonify({'success': False, 'error': 'Invalid SQLite database file'}), 400

        # Safety: backup current database before replacing
        safety_name = f'pre_restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
        safety_path = BACKUP_DIR / safety_name
        if DB_PATH.exists():
            shutil.copy2(str(DB_PATH), str(safety_path))

        # Replace database with uploaded file
        shutil.move(str(temp_path), str(DB_PATH))

        # Run init_db() for any schema migrations the uploaded DB might lack
        init_db()

        return jsonify({
            'success': True,
            'message': 'Database restored successfully',
            'safety_backup': safety_name
        })

    except Exception as e:
        temp_path.unlink(missing_ok=True)
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/api/restore/config-upload', methods=['POST'])
@admin_required
def api_restore_config_upload():
    """Restore config from an uploaded .json backup file."""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400

    uploaded = request.files['file']
    if not uploaded.filename or not uploaded.filename.endswith('.json'):
        return jsonify({'success': False, 'error': 'File must be a .json file'}), 400

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Save with timestamp prefix to avoid collisions
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    saved_name = f'uploaded_{timestamp}_{uploaded.filename}'
    saved_path = BACKUP_DIR / saved_name

    try:
        uploaded.save(str(saved_path))

        # Validate it's valid JSON with expected structure
        import json
        with open(saved_path) as f:
            data = json.load(f)
        if not isinstance(data, dict) or 'version' not in data:
            saved_path.unlink(missing_ok=True)
            return jsonify({'success': False, 'error': 'Invalid backup file format'}), 400

        # Use existing restore function
        result = db.restore_static_tables(str(saved_path))
        return jsonify(result)

    except json.JSONDecodeError:
        saved_path.unlink(missing_ok=True)
        return jsonify({'success': False, 'error': 'Invalid JSON file'}), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@admin_bp.route('/api/reset-database', methods=['POST'])
@admin_required
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
@admin_required
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
@admin_required
def api_delete_quarantine(partial_isin):
    """Delete quarantined items for a partial ISIN."""
    result = db.delete_quarantine_items(partial_isin)
    return jsonify(result)


@admin_bp.route('/api/quarantine/delete', methods=['POST'])
@admin_required
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
@admin_required
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
@admin_required
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
