"""User and custodian access CRUD operations."""

from datetime import datetime
from typing import List, Optional

from werkzeug.security import check_password_hash, generate_password_hash

from cas_parser.webapp.db.connection import get_db

__all__ = [
    'create_user',
    'get_user_by_id',
    'get_user_by_username',
    'get_all_users',
    'update_user',
    'update_password',
    'count_active_admins',
    'user_count',
    'record_login',
    'grant_custodian_access',
    'revoke_custodian_access',
    'get_custodians_for_investor',
    'is_custodian',
    'get_accessible_investor_ids',
]


def create_user(username: str, password: str, display_name: str,
                role: str = 'member', investor_id: int = None) -> int:
    """Create a new user. Returns the user ID."""
    password_hash = generate_password_hash(password, method='pbkdf2:sha256')
    with get_db() as conn:
        cursor = conn.execute(
            """INSERT INTO users (username, password_hash, display_name, role, investor_id)
               VALUES (?, ?, ?, ?, ?)""",
            (username, password_hash, display_name, role, investor_id)
        )
        return cursor.lastrowid


def get_user_by_id(user_id: int) -> Optional[dict]:
    """Get a user by ID."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return dict(row) if row else None


def get_user_by_username(username: str) -> Optional[dict]:
    """Get a user by username."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()
        return dict(row) if row else None


def get_all_users() -> List[dict]:
    """Get all users (excludes password_hash)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, username, display_name, role, investor_id,
                      is_active, last_login, created_at, updated_at
               FROM users ORDER BY created_at"""
        ).fetchall()
        return [dict(row) for row in rows]


def update_user(user_id: int, display_name: str = None, role: str = None,
                investor_id: object = None, is_active: int = None) -> bool:
    """Update user fields. Pass investor_id=0 to unlink. Returns True if updated."""
    fields = []
    values = []
    if display_name is not None:
        fields.append("display_name = ?")
        values.append(display_name)
    if role is not None:
        fields.append("role = ?")
        values.append(role)
    if investor_id is not None:
        fields.append("investor_id = ?")
        values.append(investor_id if investor_id != 0 else None)
    if is_active is not None:
        fields.append("is_active = ?")
        values.append(is_active)
    if not fields:
        return False
    fields.append("updated_at = CURRENT_TIMESTAMP")
    values.append(user_id)
    with get_db() as conn:
        conn.execute(
            f"UPDATE users SET {', '.join(fields)} WHERE id = ?", values
        )
        return True


def update_password(user_id: int, new_password: str) -> bool:
    """Update a user's password. Returns True if updated."""
    password_hash = generate_password_hash(new_password, method='pbkdf2:sha256')
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (password_hash, user_id)
        )
        return True


def count_active_admins() -> int:
    """Count the number of active admin users."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()
        return row['cnt']


def user_count() -> int:
    """Count total users."""
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
        return row['cnt']


def record_login(user_id: int):
    """Update last_login timestamp."""
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,)
        )


def grant_custodian_access(investor_id: int, custodian_user_id: int,
                           granted_by_user_id: int) -> int:
    """Grant custodian access. Returns the access record ID."""
    with get_db() as conn:
        cursor = conn.execute(
            """INSERT INTO custodian_access (investor_id, custodian_user_id, granted_by_user_id)
               VALUES (?, ?, ?)""",
            (investor_id, custodian_user_id, granted_by_user_id)
        )
        return cursor.lastrowid


def revoke_custodian_access(investor_id: int, custodian_user_id: int) -> bool:
    """Revoke custodian access. Returns True if a row was deleted."""
    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM custodian_access WHERE investor_id = ? AND custodian_user_id = ?",
            (investor_id, custodian_user_id)
        )
        return cursor.rowcount > 0


def get_custodians_for_investor(investor_id: int) -> List[dict]:
    """Get all custodian users for an investor."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT ca.id, ca.investor_id, ca.custodian_user_id, ca.granted_by_user_id,
                      ca.created_at, u.username, u.display_name
               FROM custodian_access ca
               JOIN users u ON u.id = ca.custodian_user_id
               WHERE ca.investor_id = ?""",
            (investor_id,)
        ).fetchall()
        return [dict(row) for row in rows]


def is_custodian(user_id: int, investor_id: int) -> bool:
    """Check if a user has custodian access to an investor."""
    with get_db() as conn:
        row = conn.execute(
            """SELECT 1 FROM custodian_access
               WHERE custodian_user_id = ? AND investor_id = ?""",
            (user_id, investor_id)
        ).fetchone()
        return row is not None


def get_accessible_investor_ids(user_id: int) -> List[int]:
    """Get all investor IDs a user can access (own + custodian)."""
    with get_db() as conn:
        # Own investor
        user = conn.execute(
            "SELECT investor_id FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        ids = set()
        if user and user['investor_id']:
            ids.add(user['investor_id'])
        # Custodian access
        rows = conn.execute(
            "SELECT investor_id FROM custodian_access WHERE custodian_user_id = ?",
            (user_id,)
        ).fetchall()
        for row in rows:
            ids.add(row['investor_id'])
        return list(ids)
