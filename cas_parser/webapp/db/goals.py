"""Investment goals, notes/journal, and folio-goal linking."""

import logging
import sqlite3
from typing import List, Optional

from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "create_goal",
    "update_goal",
    "delete_goal",
    "get_goal_by_id",
    "get_goals_by_investor",
    "link_folio_to_goal",
    "unlink_folio_from_goal",
    "get_unlinked_folios_for_goal",
    "create_goal_note",
    "get_goal_notes",
    "get_goal_note_by_id",
    "update_goal_note",
    "delete_goal_note",
    "get_goal_notes_timeline",
]


def create_goal(investor_id: int, name: str, target_amount: float = 0,
                target_date: str = None, description: str = None,
                target_equity_pct: float = 0, target_debt_pct: float = 0,
                target_commodity_pct: float = 0, target_cash_pct: float = 0,
                target_others_pct: float = 0) -> int:
    """Create a new investment goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO goals (investor_id, name, description, target_amount, target_date,
                             target_equity_pct, target_debt_pct, target_commodity_pct,
                             target_cash_pct, target_others_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (investor_id, name, description, target_amount, target_date,
              target_equity_pct, target_debt_pct, target_commodity_pct,
              target_cash_pct, target_others_pct))
        return cursor.lastrowid


def update_goal(goal_id: int, name: str = None, target_amount: float = None,
                target_date: str = None, description: str = None,
                target_equity_pct: float = None, target_debt_pct: float = None,
                target_commodity_pct: float = None, target_cash_pct: float = None,
                target_others_pct: float = None) -> dict:
    """Update a goal's details."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Build dynamic update
        updates = []
        values = []

        if name is not None:
            updates.append("name = ?")
            values.append(name)
        if target_amount is not None:
            updates.append("target_amount = ?")
            values.append(target_amount)
        if target_date is not None:
            updates.append("target_date = ?")
            values.append(target_date if target_date else None)
        if description is not None:
            updates.append("description = ?")
            values.append(description)
        if target_equity_pct is not None:
            updates.append("target_equity_pct = ?")
            values.append(target_equity_pct)
        if target_debt_pct is not None:
            updates.append("target_debt_pct = ?")
            values.append(target_debt_pct)
        if target_commodity_pct is not None:
            updates.append("target_commodity_pct = ?")
            values.append(target_commodity_pct)
        if target_cash_pct is not None:
            updates.append("target_cash_pct = ?")
            values.append(target_cash_pct)
        if target_others_pct is not None:
            updates.append("target_others_pct = ?")
            values.append(target_others_pct)

        if not updates:
            return {'success': False, 'error': 'No updates provided'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(goal_id)

        cursor.execute(f"""
            UPDATE goals SET {', '.join(updates)} WHERE id = ?
        """, values)

        return {'success': cursor.rowcount > 0}


def delete_goal(goal_id: int) -> dict:
    """Delete a goal and its folio links."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Delete folio links first
        cursor.execute("DELETE FROM goal_folios WHERE goal_id = ?", (goal_id,))

        # Delete the goal
        cursor.execute("DELETE FROM goals WHERE id = ?", (goal_id,))

        return {'success': cursor.rowcount > 0}


def get_goal_by_id(goal_id: int) -> dict:
    """Get a goal by ID with current value and allocation."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM goals WHERE id = ?
        """, (goal_id,))

        row = cursor.fetchone()
        if not row:
            return None

        goal = dict(row)

        # Get linked folios and calculate current value/allocation
        goal.update(_calculate_goal_values(cursor, goal_id))

        return goal


def get_goals_by_investor(investor_id: int) -> List[dict]:
    """Get all goals for an investor with current values."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM goals WHERE investor_id = ? ORDER BY created_at
        """, (investor_id,))

        goals = []
        for row in cursor.fetchall():
            goal = dict(row)
            goal.update(_calculate_goal_values(cursor, goal['id']))
            goals.append(goal)

        return goals


def _calculate_goal_values(cursor, goal_id: int) -> dict:
    """Calculate current value and allocation for a goal based on linked folios."""
    # Get linked folios with their values
    cursor.execute("""
        SELECT
            gf.folio_id,
            f.scheme_name,
            f.folio_number,
            h.units,
            COALESCE(mf.current_nav, h.nav) as nav,
            h.units * COALESCE(mf.current_nav, h.nav) as value,
            COALESCE(mf.equity_pct, 0) as equity_pct,
            COALESCE(mf.debt_pct, 0) as debt_pct,
            COALESCE(mf.commodity_pct, 0) as commodity_pct,
            COALESCE(mf.cash_pct, 0) as cash_pct,
            COALESCE(mf.others_pct, 0) as others_pct
        FROM goal_folios gf
        JOIN folios f ON f.id = gf.folio_id
        LEFT JOIN holdings h ON h.folio_id = f.id
        LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
        WHERE gf.goal_id = ?
    """, (goal_id,))

    linked_folios = []
    total_value = 0
    equity_value = 0
    debt_value = 0
    commodity_value = 0
    cash_value = 0
    others_value = 0

    for row in cursor.fetchall():
        folio = dict(row)
        value = folio['value'] or 0
        total_value += value

        # Calculate allocation
        alloc_sum = folio['equity_pct'] + folio['debt_pct'] + folio['commodity_pct'] + folio['cash_pct'] + folio['others_pct']
        if alloc_sum >= 1:
            equity_value += value * folio['equity_pct'] / 100
            debt_value += value * folio['debt_pct'] / 100
            commodity_value += value * folio['commodity_pct'] / 100
            cash_value += value * folio['cash_pct'] / 100
            others_value += value * folio['others_pct'] / 100

        linked_folios.append({
            'folio_id': folio['folio_id'],
            'scheme_name': folio['scheme_name'],
            'folio_number': folio['folio_number'],
            'units': folio['units'],
            'nav': folio['nav'],
            'value': value
        })

    return {
        'linked_folios': linked_folios,
        'linked_count': len(linked_folios),
        'current_value': total_value,
        'actual_allocation': {
            'equity': equity_value,
            'debt': debt_value,
            'commodity': commodity_value,
            'cash': cash_value,
            'others': others_value
        },
        'actual_allocation_pct': {
            'equity': (equity_value / total_value * 100) if total_value > 0 else 0,
            'debt': (debt_value / total_value * 100) if total_value > 0 else 0,
            'commodity': (commodity_value / total_value * 100) if total_value > 0 else 0,
            'cash': (cash_value / total_value * 100) if total_value > 0 else 0,
            'others': (others_value / total_value * 100) if total_value > 0 else 0
        }
    }


def link_folio_to_goal(goal_id: int, folio_id: int) -> dict:
    """Link a folio to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO goal_folios (goal_id, folio_id) VALUES (?, ?)
            """, (goal_id, folio_id))
            return {'success': True}
        except sqlite3.IntegrityError:
            return {'success': False, 'error': 'Folio already linked to this goal'}


def unlink_folio_from_goal(goal_id: int, folio_id: int) -> dict:
    """Unlink a folio from a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM goal_folios WHERE goal_id = ? AND folio_id = ?
        """, (goal_id, folio_id))
        return {'success': cursor.rowcount > 0}


def get_unlinked_folios_for_goal(goal_id: int, investor_id: int) -> List[dict]:
    """Get folios that are not linked to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                f.id as folio_id,
                f.scheme_name,
                f.folio_number,
                h.units,
                COALESCE(mf.current_nav, h.nav) as nav,
                h.units * COALESCE(mf.current_nav, h.nav) as value
            FROM folios f
            LEFT JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
              AND f.id NOT IN (SELECT folio_id FROM goal_folios WHERE goal_id = ?)
            ORDER BY f.scheme_name
        """, (investor_id, goal_id))

        return [dict(row) for row in cursor.fetchall()]


# ==================== Goal Notes Functions ====================

def create_goal_note(goal_id: int, content: str, title: str = None,
                     note_type: str = 'thought', mood: str = None) -> int:
    """
    Create a new note for a goal.

    Args:
        goal_id: The goal to add the note to
        content: The note content (required)
        title: Optional title for the note
        note_type: Type of note - 'thought', 'decision', 'milestone', 'review'
        mood: Optional mood indicator - 'optimistic', 'cautious', 'worried', 'confident', 'neutral'

    Returns:
        The ID of the created note
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO goal_notes (goal_id, content, title, note_type, mood)
            VALUES (?, ?, ?, ?, ?)
        """, (goal_id, content, title, note_type, mood))
        return cursor.lastrowid


def get_goal_notes(goal_id: int, limit: int = 50) -> List[dict]:
    """Get all notes for a goal, ordered by creation date descending."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, goal_id, note_type, title, content, mood,
                   created_at, updated_at
            FROM goal_notes
            WHERE goal_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (goal_id, limit))
        return [dict(row) for row in cursor.fetchall()]


def get_goal_note_by_id(note_id: int) -> dict:
    """Get a single note by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, goal_id, note_type, title, content, mood,
                   created_at, updated_at
            FROM goal_notes
            WHERE id = ?
        """, (note_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_goal_note(note_id: int, content: str = None, title: str = None,
                     note_type: str = None, mood: str = None) -> dict:
    """Update an existing goal note."""
    with get_db() as conn:
        cursor = conn.cursor()

        updates = []
        params = []

        if content is not None:
            updates.append("content = ?")
            params.append(content)
        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if note_type is not None:
            updates.append("note_type = ?")
            params.append(note_type)
        if mood is not None:
            updates.append("mood = ?")
            params.append(mood)

        if not updates:
            return {'success': False, 'error': 'No fields to update'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(note_id)

        cursor.execute(f"""
            UPDATE goal_notes SET {', '.join(updates)}
            WHERE id = ?
        """, params)

        return {'success': cursor.rowcount > 0}


def delete_goal_note(note_id: int) -> dict:
    """Delete a goal note."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM goal_notes WHERE id = ?", (note_id,))
        return {'success': cursor.rowcount > 0}


def get_goal_notes_timeline(investor_id: int, limit: int = 100) -> List[dict]:
    """
    Get a timeline of all notes across all goals for an investor.
    Useful for seeing overall investment thinking evolution.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                gn.id, gn.goal_id, gn.note_type, gn.title, gn.content, gn.mood,
                gn.created_at, gn.updated_at,
                g.name as goal_name
            FROM goal_notes gn
            JOIN goals g ON g.id = gn.goal_id
            WHERE g.investor_id = ?
            ORDER BY gn.created_at DESC
            LIMIT ?
        """, (investor_id, limit))
        return [dict(row) for row in cursor.fetchall()]
