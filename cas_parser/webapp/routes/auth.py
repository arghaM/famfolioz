"""Authentication routes: login, logout, setup, user management, custodian access."""

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for, g

from werkzeug.security import check_password_hash

from cas_parser.webapp import data as db
from cas_parser.webapp.auth import admin_required, check_investor_access

auth_bp = Blueprint('auth', __name__)


# ---------------------------------------------------------------------------
# Login / Logout / Setup
# ---------------------------------------------------------------------------

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')

    username = (request.form.get('username') or '').strip()
    password = request.form.get('password') or ''

    user = db.get_user_by_username(username)
    if not user or not check_password_hash(user['password_hash'], password):
        return render_template('login.html', error='Invalid username or password',
                               username=username)

    if not user['is_active']:
        return render_template('login.html', error='Account is disabled',
                               username=username)

    session.clear()
    session['user_id'] = user['id']
    session.permanent = True
    db.record_login(user['id'])
    return redirect(url_for('pages.index'))


@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))


@auth_bp.route('/setup', methods=['GET', 'POST'])
def setup():
    # Only allow setup when no users exist
    if db.user_count() > 0:
        return redirect(url_for('pages.index'))

    if request.method == 'GET':
        return render_template('setup.html')

    username = (request.form.get('username') or '').strip()
    display_name = (request.form.get('display_name') or '').strip()
    password = request.form.get('password') or ''
    confirm = request.form.get('confirm_password') or ''

    error = None
    if not username:
        error = 'Username is required'
    elif not display_name:
        error = 'Display name is required'
    elif len(password) < 4:
        error = 'Password must be at least 4 characters'
    elif password != confirm:
        error = 'Passwords do not match'

    if error:
        return render_template('setup.html', error=error,
                               username=username, display_name=display_name)

    user_id = db.create_user(username, password, display_name, role='admin')
    session.clear()
    session['user_id'] = user_id
    session.permanent = True
    return redirect(url_for('pages.index'))


# ---------------------------------------------------------------------------
# User Management API (admin only)
# ---------------------------------------------------------------------------

@auth_bp.route('/api/users', methods=['GET'])
@admin_required
def api_get_users():
    users = db.get_all_users()
    return jsonify(users)


@auth_bp.route('/api/users', methods=['POST'])
@admin_required
def api_create_user():
    data = request.json or {}
    username = (data.get('username') or '').strip()
    display_name = (data.get('display_name') or '').strip()
    password = data.get('password') or ''
    role = data.get('role', 'member')
    investor_id = data.get('investor_id')

    if not username:
        return jsonify({'error': 'Username is required'}), 400
    if not display_name:
        return jsonify({'error': 'Display name is required'}), 400
    if len(password) < 4:
        return jsonify({'error': 'Password must be at least 4 characters'}), 400
    if role not in ('admin', 'member'):
        return jsonify({'error': 'Role must be admin or member'}), 400

    if db.get_user_by_username(username):
        return jsonify({'error': f'Username "{username}" already exists'}), 409

    user_id = db.create_user(username, password, display_name, role, investor_id)
    return jsonify({'success': True, 'id': user_id}), 201


@auth_bp.route('/api/users/<int:user_id>', methods=['GET'])
@admin_required
def api_get_user(user_id):
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    user.pop('password_hash', None)
    return jsonify(user)


@auth_bp.route('/api/users/<int:user_id>', methods=['PUT'])
@admin_required
def api_update_user(user_id):
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    data = request.json or {}
    new_role = data.get('role')
    new_active = data.get('is_active')

    # Last admin protection
    if user['role'] == 'admin' and user['is_active']:
        demoting = new_role == 'member'
        disabling = new_active == 0 or new_active is False
        if demoting or disabling:
            if db.count_active_admins() <= 1:
                return jsonify({'error': 'Cannot demote or disable the last active admin'}), 400

    db.update_user(
        user_id,
        display_name=data.get('display_name'),
        role=new_role,
        investor_id=data.get('investor_id'),
        is_active=int(new_active) if new_active is not None else None,
    )
    return jsonify({'success': True})


@auth_bp.route('/api/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def api_reset_password(user_id):
    user = db.get_user_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    data = request.json or {}
    password = data.get('password', '')
    if len(password) < 4:
        return jsonify({'error': 'Password must be at least 4 characters'}), 400
    db.update_password(user_id, password)
    return jsonify({'success': True})


@auth_bp.route('/api/users/<int:user_id>/change-password', methods=['POST'])
def api_change_password(user_id):
    """Allow a user to change their own password."""
    current_user = g.get('current_user')
    if not current_user:
        return jsonify({'error': 'Not authenticated'}), 401
    # Users can only change their own password (admins can use reset-password for others)
    if current_user['id'] != user_id:
        return jsonify({'error': 'Forbidden'}), 403

    data = request.json or {}
    current_password = data.get('current_password', '')
    new_password = data.get('new_password', '')

    user = db.get_user_by_id(user_id)
    if not check_password_hash(user['password_hash'], current_password):
        return jsonify({'error': 'Current password is incorrect'}), 400
    if len(new_password) < 4:
        return jsonify({'error': 'New password must be at least 4 characters'}), 400

    db.update_password(user_id, new_password)
    return jsonify({'success': True})


# ---------------------------------------------------------------------------
# Custodian Access API
# ---------------------------------------------------------------------------

@auth_bp.route('/api/investors/<int:investor_id>/custodians', methods=['GET'])
def api_get_custodians(investor_id):
    """Get custodian users for an investor. Admin or investor owner only."""
    check_investor_access(investor_id)
    custodians = db.get_custodians_for_investor(investor_id)
    return jsonify(custodians)


@auth_bp.route('/api/investors/<int:investor_id>/custodians', methods=['POST'])
def api_grant_custodian(investor_id):
    """Grant custodian access. Admin or investor owner only."""
    current_user = g.get('current_user')
    # Only admin or the investor's owner can grant
    if current_user['role'] != 'admin' and current_user.get('investor_id') != investor_id:
        return jsonify({'error': 'Forbidden'}), 403

    data = request.json or {}
    custodian_user_id = data.get('user_id')
    if not custodian_user_id:
        return jsonify({'error': 'user_id is required'}), 400

    target_user = db.get_user_by_id(custodian_user_id)
    if not target_user:
        return jsonify({'error': 'User not found'}), 404

    try:
        access_id = db.grant_custodian_access(investor_id, custodian_user_id, current_user['id'])
        return jsonify({'success': True, 'id': access_id}), 201
    except Exception:
        return jsonify({'error': 'Access already granted'}), 409


@auth_bp.route('/api/investors/<int:investor_id>/custodians/<int:custodian_user_id>', methods=['DELETE'])
def api_revoke_custodian(investor_id, custodian_user_id):
    """Revoke custodian access. Admin or investor owner only."""
    current_user = g.get('current_user')
    if current_user['role'] != 'admin' and current_user.get('investor_id') != investor_id:
        return jsonify({'error': 'Forbidden'}), 403

    deleted = db.revoke_custodian_access(investor_id, custodian_user_id)
    if not deleted:
        return jsonify({'error': 'Access not found'}), 404
    return jsonify({'success': True})
