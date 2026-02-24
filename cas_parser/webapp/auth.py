"""Authentication and authorization module.

Provides:
- init_auth(app)       — wire before_request, context_processor, secret key
- @admin_required      — decorator: 403 for non-admin users
- check_investor_access(investor_id) — abort 403 if user cannot access investor
- Indirect ownership resolvers for goal_id, folio_id, note_id, tx_id, etc.
"""

import os
import secrets
from datetime import timedelta
from functools import wraps

from flask import abort, g, redirect, request, session, url_for

from cas_parser.webapp.db.auth import (
    get_accessible_investor_ids,
    get_user_by_id,
    is_custodian,
    user_count,
)
from cas_parser.webapp.db.admin import get_config, set_config


# ---------------------------------------------------------------------------
# Secret key management
# ---------------------------------------------------------------------------

def _ensure_secret_key(app):
    """Load or generate a persistent secret key from the app_config table."""
    key = get_config('flask_secret_key')
    if not key:
        key = secrets.token_hex(32)
        set_config('flask_secret_key', key)
    app.secret_key = key


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def admin_required(f):
    """Decorator: require the current user to be an admin."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = g.get('current_user')
        if not user or user['role'] != 'admin':
            if request.path.startswith('/api/'):
                abort(403)
            return redirect(url_for('pages.index'))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Access control helpers
# ---------------------------------------------------------------------------

def check_investor_access(investor_id):
    """Abort 403 if the current user cannot access this investor.

    Admins always pass. Members pass if the investor is their own
    or they have custodian access.
    """
    user = g.get('current_user')
    if not user:
        abort(401)
    if user['role'] == 'admin':
        return
    if user.get('investor_id') == investor_id:
        return
    if is_custodian(user['id'], investor_id):
        return
    if request.path.startswith('/api/'):
        abort(403)
    return redirect(url_for('pages.index'))


# ---------------------------------------------------------------------------
# Indirect ownership resolvers  (entity_id → investor_id)
# ---------------------------------------------------------------------------

def get_investor_id_for_goal(goal_id):
    from cas_parser.webapp.db.goals import get_goal_by_id
    goal = get_goal_by_id(goal_id)
    if not goal:
        abort(404)
    return goal['investor_id']


def get_investor_id_for_folio(folio_id):
    from cas_parser.webapp.db.folios import get_folio_by_id
    folio = get_folio_by_id(folio_id)
    if not folio:
        abort(404)
    return folio.get('investor_id')


def get_investor_id_for_note(note_id):
    from cas_parser.webapp.db.goals import get_goal_note_by_id
    note = get_goal_note_by_id(note_id)
    if not note:
        abort(404)
    return get_investor_id_for_goal(note['goal_id'])


def get_investor_id_for_tx(tx_id):
    from cas_parser.webapp.db.transactions import get_transaction_by_id
    tx = get_transaction_by_id(tx_id)
    if not tx:
        abort(404)
    return get_investor_id_for_folio(tx['folio_id'])


def get_investor_id_for_asset(asset_id):
    from cas_parser.webapp.db.manual_assets import get_manual_asset
    asset = get_manual_asset(asset_id)
    if not asset:
        abort(404)
    return asset['investor_id']


def get_investor_id_for_asset_tx(tx_id):
    from cas_parser.webapp.db.connection import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.investor_id
            FROM manual_asset_transactions mat
            JOIN manual_assets ma ON mat.asset_id = ma.id
            WHERE mat.id = ?
        """, (tx_id,))
        row = cursor.fetchone()
        if not row:
            abort(404)
        return row['investor_id']


def get_investor_id_for_nps_subscriber(subscriber_id):
    from cas_parser.webapp.db.nps import get_nps_subscriber
    sub = get_nps_subscriber(subscriber_id=subscriber_id)
    if not sub:
        abort(404)
    return sub.get('investor_id')


def get_investor_id_for_nps_tx(tx_id):
    from cas_parser.webapp.db.nps import get_nps_transaction
    tx = get_nps_transaction(tx_id)
    if not tx:
        abort(404)
    return get_investor_id_for_nps_subscriber(tx['subscriber_id'])


# ---------------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------------

def init_auth(app):
    """Wire authentication into the Flask app."""
    _ensure_secret_key(app)

    # Session config
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

    # Paths that don't require login
    PUBLIC_PATHS = {'/login', '/setup', '/health', '/static'}

    @app.before_request
    def require_auth():
        path = request.path

        # Allow static files and public paths
        if path.startswith('/static'):
            return
        if path in PUBLIC_PATHS:
            return

        # First-run: no users exist → redirect to setup
        if user_count() == 0:
            if path != '/setup':
                return redirect('/setup')
            return

        # Not logged in → redirect or 401
        user_id = session.get('user_id')
        if not user_id:
            if path.startswith('/api/'):
                abort(401)
            return redirect(url_for('auth.login'))

        user = get_user_by_id(user_id)
        if not user or not user['is_active']:
            session.clear()
            if path.startswith('/api/'):
                abort(401)
            return redirect(url_for('auth.login'))

        g.current_user = user

    @app.context_processor
    def inject_user():
        return {'current_user': g.get('current_user')}
