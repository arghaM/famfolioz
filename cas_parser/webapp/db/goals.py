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
    "link_asset_to_goal",
    "unlink_asset_from_goal",
    "get_goal_linked_assets",
    "get_unlinked_assets_for_goal",
    "create_goal_note",
    "get_goal_notes",
    "get_goal_note_by_id",
    "update_goal_note",
    "delete_goal_note",
    "get_goal_notes_timeline",
    "get_goal_phases",
    "save_goal_phases",
    "delete_goal_phase",
    "get_goal_allocation_detail",
    "link_nps_to_goal",
    "unlink_nps_from_goal",
    "get_unlinked_nps_for_goal",
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
    """Delete a goal and its folio/asset/NPS links."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Delete folio links first
        cursor.execute("DELETE FROM goal_folios WHERE goal_id = ?", (goal_id,))

        # Delete asset links
        cursor.execute("DELETE FROM goal_assets WHERE goal_id = ?", (goal_id,))

        # Delete NPS links
        cursor.execute("DELETE FROM goal_nps WHERE goal_id = ?", (goal_id,))

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
    """Calculate current value and allocation for a goal based on linked folios and assets."""
    from cas_parser.webapp.db.manual_assets import _enrich_manual_asset

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

    # Get linked manual assets
    cursor.execute("""
        SELECT ma.*
        FROM goal_assets ga
        JOIN manual_assets ma ON ma.id = ga.asset_id
        WHERE ga.goal_id = ?
    """, (goal_id,))

    linked_assets = []
    for row in cursor.fetchall():
        asset = _enrich_manual_asset(dict(row))
        value = asset.get('calculated_value') or 0
        total_value += value

        # Use per-asset allocation percentages
        a_equity = asset.get('equity_pct') or 0
        a_debt = asset.get('debt_pct') or 0
        a_commodity = asset.get('commodity_pct') or 0
        a_cash = asset.get('cash_pct') or 0
        a_others = asset.get('others_pct') or 0
        alloc_sum = a_equity + a_debt + a_commodity + a_cash + a_others
        if alloc_sum >= 1:
            equity_value += value * a_equity / 100
            debt_value += value * a_debt / 100
            commodity_value += value * a_commodity / 100
            cash_value += value * a_cash / 100
            others_value += value * a_others / 100

        linked_assets.append({
            'asset_id': asset['id'],
            'name': asset['name'],
            'asset_type': asset['asset_type'],
            'value': value,
            'exclude_from_xirr': bool(asset.get('exclude_from_xirr')),
        })

    # Get linked NPS accounts
    cursor.execute("""
        SELECT ns.id, ns.pran, ns.name, ns.total_value
        FROM goal_nps gn
        JOIN nps_subscribers ns ON ns.id = gn.subscriber_id
        WHERE gn.goal_id = ?
    """, (goal_id,))

    linked_nps = []
    for row in cursor.fetchall():
        sub = dict(row)
        # Get scheme-level breakdown for allocation
        cursor.execute("""
            SELECT scheme_type, current_value
            FROM nps_schemes
            WHERE subscriber_id = ?
        """, (sub['id'],))

        nps_value = 0
        for scheme in cursor.fetchall():
            sv = scheme['current_value'] or 0
            nps_value += sv
            st = (scheme['scheme_type'] or '').upper()
            if st == 'E':
                equity_value += sv
            elif st in ('C', 'G'):
                debt_value += sv
            elif st == 'A':
                equity_value += sv * 0.75
                debt_value += sv * 0.25

        total_value += nps_value
        linked_nps.append({
            'subscriber_id': sub['id'],
            'pran': sub['pran'],
            'name': sub['name'],
            'value': nps_value,
        })

    return {
        'linked_folios': linked_folios,
        'linked_assets': linked_assets,
        'linked_nps': linked_nps,
        'linked_count': len(linked_folios) + len(linked_assets) + len(linked_nps),
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


# ==================== Goal-Asset Linking Functions ====================

def link_asset_to_goal(goal_id: int, asset_id: int) -> dict:
    """Link a manual asset to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO goal_assets (goal_id, asset_id) VALUES (?, ?)
            """, (goal_id, asset_id))
            return {'success': True}
        except sqlite3.IntegrityError:
            return {'success': False, 'error': 'Asset already linked to this goal'}


def unlink_asset_from_goal(goal_id: int, asset_id: int) -> dict:
    """Unlink a manual asset from a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM goal_assets WHERE goal_id = ? AND asset_id = ?
        """, (goal_id, asset_id))
        return {'success': cursor.rowcount > 0}


def get_goal_linked_assets(goal_id: int) -> list:
    """Get manual assets linked to a goal, enriched with calculated values."""
    from cas_parser.webapp.db.manual_assets import _enrich_manual_asset

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*
            FROM goal_assets ga
            JOIN manual_assets ma ON ma.id = ga.asset_id
            WHERE ga.goal_id = ?
            ORDER BY ma.name
        """, (goal_id,))
        return [_enrich_manual_asset(dict(row)) for row in cursor.fetchall()]


def get_unlinked_assets_for_goal(goal_id: int, investor_id: int) -> list:
    """Get manual assets not yet linked to a goal."""
    from cas_parser.webapp.db.manual_assets import _enrich_manual_asset

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*
            FROM manual_assets ma
            WHERE ma.investor_id = ?
              AND ma.is_active = 1
              AND ma.id NOT IN (SELECT asset_id FROM goal_assets WHERE goal_id = ?)
            ORDER BY ma.name
        """, (investor_id, goal_id))
        return [_enrich_manual_asset(dict(row)) for row in cursor.fetchall()]


# ==================== Goal-NPS Linking Functions ====================

def link_nps_to_goal(goal_id: int, subscriber_id: int) -> dict:
    """Link an NPS subscriber account to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO goal_nps (goal_id, subscriber_id) VALUES (?, ?)
            """, (goal_id, subscriber_id))
            return {'success': True}
        except sqlite3.IntegrityError:
            return {'success': False, 'error': 'NPS account already linked to this goal'}


def unlink_nps_from_goal(goal_id: int, subscriber_id: int) -> dict:
    """Unlink an NPS subscriber account from a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM goal_nps WHERE goal_id = ? AND subscriber_id = ?
        """, (goal_id, subscriber_id))
        return {'success': cursor.rowcount > 0}


def get_unlinked_nps_for_goal(goal_id: int, investor_id: int) -> list:
    """Get NPS subscriber accounts not yet linked to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, pran, name, total_value
            FROM nps_subscribers
            WHERE investor_id = ?
              AND id NOT IN (SELECT subscriber_id FROM goal_nps WHERE goal_id = ?)
            ORDER BY name
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


def get_goal_phases(goal_id: int) -> List[dict]:
    """Get all phases for a goal with their equity sub-allocations."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, phase_name, start_date, end_date,
                   equity_pct, debt_pct, commodity_pct, sort_order
            FROM goal_phases
            WHERE goal_id = ?
            ORDER BY sort_order, start_date
        """, (goal_id,))
        phases = [dict(row) for row in cursor.fetchall()]

        for phase in phases:
            cursor.execute("""
                SELECT india_large_cap_pct, india_mid_small_pct, india_flexi_pct,
                       intl_us_global_pct, intl_emerging_pct, sectoral_thematic_pct
                FROM goal_phase_equity_sub
                WHERE phase_id = ?
            """, (phase['id'],))
            sub = cursor.fetchone()
            phase['equity_sub'] = dict(sub) if sub else {
                'india_large_cap_pct': 0, 'india_mid_small_pct': 0,
                'india_flexi_pct': 0, 'intl_us_global_pct': 0,
                'intl_emerging_pct': 0, 'sectoral_thematic_pct': 0
            }

        return phases


def save_goal_phases(goal_id: int, phases_data: List[dict]) -> dict:
    """Replace all phases for a goal (delete + re-insert)."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Delete existing phases (CASCADE deletes equity sub)
        cursor.execute("DELETE FROM goal_phases WHERE goal_id = ?", (goal_id,))

        for i, phase in enumerate(phases_data):
            cursor.execute("""
                INSERT INTO goal_phases
                (goal_id, phase_name, start_date, end_date,
                 equity_pct, debt_pct, commodity_pct, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                goal_id,
                phase.get('phase_name', f'Phase {i + 1}'),
                phase.get('start_date'),
                phase.get('end_date'),
                float(phase.get('equity_pct', 0) or 0),
                float(phase.get('debt_pct', 0) or 0),
                float(phase.get('commodity_pct', 0) or 0),
                i
            ))
            phase_id = cursor.lastrowid

            # Insert equity sub-allocation if provided
            equity_sub = phase.get('equity_sub', {})
            if equity_sub:
                cursor.execute("""
                    INSERT INTO goal_phase_equity_sub
                    (phase_id, india_large_cap_pct, india_mid_small_pct,
                     india_flexi_pct, intl_us_global_pct, intl_emerging_pct,
                     sectoral_thematic_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    phase_id,
                    float(equity_sub.get('india_large_cap_pct', 0) or 0),
                    float(equity_sub.get('india_mid_small_pct', 0) or 0),
                    float(equity_sub.get('india_flexi_pct', 0) or 0),
                    float(equity_sub.get('intl_us_global_pct', 0) or 0),
                    float(equity_sub.get('intl_emerging_pct', 0) or 0),
                    float(equity_sub.get('sectoral_thematic_pct', 0) or 0),
                ))

        return {'success': True, 'phases_saved': len(phases_data)}


def delete_goal_phase(phase_id: int) -> dict:
    """Delete a single phase (CASCADE deletes equity sub)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM goal_phases WHERE id = ?", (phase_id,))
        return {'success': cursor.rowcount > 0}


def get_goal_allocation_detail(goal_id: int) -> dict:
    """Return detailed per-fund allocation breakdown for a goal.

    Groups funds by asset class and equity sub-category, showing each
    fund's contribution with its value, allocation percentages, and
    market cap split — so the user can see exactly how each number is derived.
    Includes manual assets linked to the goal.
    """
    from cas_parser.webapp.db.manual_assets import _enrich_manual_asset

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                gf.folio_id,
                f.scheme_name,
                f.folio_number,
                f.isin,
                h.units,
                COALESCE(mf.current_nav, h.nav) as nav,
                h.units * COALESCE(mf.current_nav, h.nav) as value,
                COALESCE(mf.equity_pct, 0) as equity_pct,
                COALESCE(mf.debt_pct, 0) as debt_pct,
                COALESCE(mf.commodity_pct, 0) as commodity_pct,
                COALESCE(mf.cash_pct, 0) as cash_pct,
                COALESCE(mf.others_pct, 0) as others_pct,
                COALESCE(mf.large_cap_pct, 0) as large_cap_pct,
                COALESCE(mf.mid_cap_pct, 0) as mid_cap_pct,
                COALESCE(mf.small_cap_pct, 0) as small_cap_pct,
                mf.fund_category,
                mf.geography,
                mf.equity_sub_category,
                mf.id as mf_id
            FROM goal_folios gf
            JOIN folios f ON f.id = gf.folio_id
            LEFT JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE gf.goal_id = ?
        """, (goal_id,))

        total_value = 0
        funds = []

        for row in cursor.fetchall():
            fund = dict(row)
            value = fund['value'] or 0
            total_value += value

            alloc_sum = fund['equity_pct'] + fund['debt_pct'] + fund['commodity_pct'] + fund['cash_pct'] + fund['others_pct']
            has_alloc = alloc_sum >= 1

            # Calculate each fund's contribution to asset classes (in absolute ₹)
            fund['equity_value'] = value * fund['equity_pct'] / 100 if has_alloc else 0
            fund['debt_value'] = value * fund['debt_pct'] / 100 if has_alloc else 0
            fund['commodity_value'] = value * fund['commodity_pct'] / 100 if has_alloc else 0
            fund['cash_value'] = value * fund['cash_pct'] / 100 if has_alloc else 0
            fund['others_value'] = value * fund['others_pct'] / 100 if has_alloc else 0

            funds.append(fund)

        # Get linked manual assets and add to the funds list
        cursor.execute("""
            SELECT ma.*
            FROM goal_assets ga
            JOIN manual_assets ma ON ma.id = ga.asset_id
            WHERE ga.goal_id = ?
        """, (goal_id,))

        manual_asset_entries = []
        for row in cursor.fetchall():
            asset = _enrich_manual_asset(dict(row))
            value = asset.get('calculated_value') or 0
            total_value += value

            a_equity = asset.get('equity_pct') or 0
            a_debt = asset.get('debt_pct') or 0
            a_commodity = asset.get('commodity_pct') or 0
            a_cash = asset.get('cash_pct') or 0
            a_others = asset.get('others_pct') or 0
            alloc_sum = a_equity + a_debt + a_commodity + a_cash + a_others
            has_alloc = alloc_sum >= 1

            type_label = (asset.get('asset_type') or 'other').upper()
            entry = {
                'scheme_name': f"[{type_label}] {asset['name']}",
                'folio_number': None,
                'folio_id': None,
                'mf_id': None,
                'asset_id': asset['id'],
                'is_manual_asset': True,
                'asset_type': asset.get('asset_type'),
                'value': value,
                'equity_pct': a_equity,
                'debt_pct': a_debt,
                'commodity_pct': a_commodity,
                'cash_pct': a_cash,
                'others_pct': a_others,
                'equity_value': value * a_equity / 100 if has_alloc else 0,
                'debt_value': value * a_debt / 100 if has_alloc else 0,
                'commodity_value': value * a_commodity / 100 if has_alloc else 0,
                'cash_value': value * a_cash / 100 if has_alloc else 0,
                'others_value': value * a_others / 100 if has_alloc else 0,
                'large_cap_pct': 0,
                'mid_cap_pct': 0,
                'small_cap_pct': 0,
                'fund_category': None,
                'geography': None,
                'equity_sub_category': None,
                'exclude_from_xirr': bool(asset.get('exclude_from_xirr')),
            }
            manual_asset_entries.append(entry)

        # Get linked NPS accounts and their schemes
        cursor.execute("""
            SELECT ns.id, ns.pran, ns.name
            FROM goal_nps gn
            JOIN nps_subscribers ns ON ns.id = gn.subscriber_id
            WHERE gn.goal_id = ?
        """, (goal_id,))

        nps_entries = []
        for sub_row in cursor.fetchall():
            sub = dict(sub_row)
            cursor.execute("""
                SELECT scheme_type, current_value
                FROM nps_schemes
                WHERE subscriber_id = ?
            """, (sub['id'],))

            for scheme in cursor.fetchall():
                sv = scheme['current_value'] or 0
                if sv <= 0:
                    continue
                st = (scheme['scheme_type'] or '').upper()
                total_value += sv

                # Derive allocation from scheme type
                if st == 'E':
                    eq_pct, dt_pct = 100, 0
                elif st in ('C', 'G'):
                    eq_pct, dt_pct = 0, 100
                elif st == 'A':
                    eq_pct, dt_pct = 75, 25
                else:
                    eq_pct, dt_pct = 0, 0

                label = f"[NPS-{st}] {sub['name']} ({sub['pran']})"
                entry = {
                    'scheme_name': label,
                    'folio_number': None,
                    'folio_id': None,
                    'mf_id': None,
                    'asset_id': None,
                    'is_manual_asset': False,
                    'is_nps': True,
                    'subscriber_id': sub['id'],
                    'value': sv,
                    'equity_pct': eq_pct,
                    'debt_pct': dt_pct,
                    'commodity_pct': 0,
                    'cash_pct': 0,
                    'others_pct': 0,
                    'equity_value': sv * eq_pct / 100,
                    'debt_value': sv * dt_pct / 100,
                    'commodity_value': 0,
                    'cash_value': 0,
                    'others_value': 0,
                    'large_cap_pct': 0,
                    'mid_cap_pct': 0,
                    'small_cap_pct': 0,
                    'fund_category': None,
                    'geography': None,
                    'equity_sub_category': None,
                }
                nps_entries.append(entry)

        # Combine funds, manual assets, and NPS for unified allocation calculation
        all_entries = funds + manual_asset_entries + nps_entries

        # Build per-category summaries
        # 1. Asset class breakdown (equity / debt / commodity / cash / others)
        asset_classes = {}
        for ac in ['equity', 'debt', 'commodity', 'cash', 'others']:
            ac_funds = []
            ac_total = 0
            for f in all_entries:
                contrib = f[f'{ac}_value']
                if contrib > 0.01:
                    ac_total += contrib
                    ac_funds.append({
                        'scheme_name': f['scheme_name'],
                        'folio_number': f.get('folio_number'),
                        'folio_id': f.get('folio_id'),
                        'mf_id': f.get('mf_id'),
                        'asset_id': f.get('asset_id'),
                        'is_manual_asset': f.get('is_manual_asset', False),
                        'total_value': f['value'] or 0,
                        'contribution': contrib,
                        'fund_alloc_pct': f[f'{ac}_pct'],
                        'fund_category': f.get('fund_category'),
                        'geography': f.get('geography'),
                        'equity_sub_category': f.get('equity_sub_category'),
                    })
            asset_classes[ac] = {
                'total': ac_total,
                'pct': (ac_total / total_value * 100) if total_value > 0 else 0,
                'funds': sorted(ac_funds, key=lambda x: x['contribution'], reverse=True)
            }

        # 2. Equity sub-category breakdown
        sub_categories = {}
        for sub_cat in ['india_large_cap', 'india_mid_small', 'india_flexi',
                        'intl_us_global', 'intl_emerging', 'sectoral_thematic']:
            sub_categories[sub_cat] = {'total': 0, 'pct_of_equity': 0, 'pct_of_total': 0, 'funds': []}

        total_equity = asset_classes['equity']['total']

        for f in funds:
            eq_val = f['equity_value']
            if eq_val <= 0.01:
                continue

            sub_cat = f['equity_sub_category']
            if not sub_cat or sub_cat not in sub_categories:
                sub_cat = 'india_flexi'  # fallback

            sub_categories[sub_cat]['total'] += eq_val
            sub_categories[sub_cat]['funds'].append({
                'scheme_name': f['scheme_name'],
                'folio_number': f['folio_number'],
                'folio_id': f['folio_id'],
                'mf_id': f['mf_id'],
                'total_value': f['value'] or 0,
                'equity_value': eq_val,
                'equity_pct': f['equity_pct'],
                'large_cap_pct': f['large_cap_pct'],
                'mid_cap_pct': f['mid_cap_pct'],
                'small_cap_pct': f['small_cap_pct'],
                'fund_category': f['fund_category'],
                'geography': f['geography'],
                'equity_sub_category': f['equity_sub_category'],
            })

        # Calculate percentages
        for sub_cat, data in sub_categories.items():
            data['pct_of_equity'] = (data['total'] / total_equity * 100) if total_equity > 0 else 0
            data['pct_of_total'] = (data['total'] / total_value * 100) if total_value > 0 else 0
            data['funds'] = sorted(data['funds'], key=lambda x: x['equity_value'], reverse=True)

        # 3. Market cap breakdown — actual large/mid/small exposure across all funds
        cap_tiers = {
            'large_cap': {'total': 0, 'funds': []},
            'mid_cap': {'total': 0, 'funds': []},
            'small_cap': {'total': 0, 'funds': []},
        }
        india_equity_total = 0
        intl_equity_total = 0
        india_equity_funds = []
        intl_equity_funds = []

        for f in funds:
            eq_val = f['equity_value']
            if eq_val <= 0.01:
                continue

            fund_info = {
                'scheme_name': f['scheme_name'],
                'folio_number': f['folio_number'],
                'folio_id': f['folio_id'],
                'mf_id': f['mf_id'],
                'equity_value': eq_val,
                'total_value': f['value'] or 0,
                'equity_pct': f['equity_pct'],
                'large_cap_pct': f['large_cap_pct'],
                'mid_cap_pct': f['mid_cap_pct'],
                'small_cap_pct': f['small_cap_pct'],
                'fund_category': f['fund_category'],
                'geography': f['geography'],
                'equity_sub_category': f['equity_sub_category'],
            }

            lc_val = eq_val * f['large_cap_pct'] / 100
            mc_val = eq_val * f['mid_cap_pct'] / 100
            sc_val = eq_val * f['small_cap_pct'] / 100
            fund_info['large_cap_value'] = lc_val
            fund_info['mid_cap_value'] = mc_val
            fund_info['small_cap_value'] = sc_val

            if f['geography'] == 'international':
                intl_equity_total += eq_val
                intl_equity_funds.append(fund_info)
            else:
                india_equity_total += eq_val
                india_equity_funds.append(fund_info)
                cap_tiers['large_cap']['total'] += lc_val
                cap_tiers['mid_cap']['total'] += mc_val
                cap_tiers['small_cap']['total'] += sc_val

                if lc_val > 0:
                    cap_tiers['large_cap']['funds'].append({
                        **fund_info, 'contribution': lc_val})
                if mc_val > 0:
                    cap_tiers['mid_cap']['funds'].append({
                        **fund_info, 'contribution': mc_val})
                if sc_val > 0:
                    cap_tiers['small_cap']['funds'].append({
                        **fund_info, 'contribution': sc_val})

        # Percentages for cap tiers
        for tier_data in cap_tiers.values():
            tier_data['pct_of_equity'] = (tier_data['total'] / total_equity * 100) if total_equity > 0 else 0
            tier_data['pct_of_india_equity'] = (tier_data['total'] / india_equity_total * 100) if india_equity_total > 0 else 0
            tier_data['funds'] = sorted(tier_data['funds'], key=lambda x: x['contribution'], reverse=True)

        # Sort fund lists
        india_equity_funds.sort(key=lambda x: x['equity_value'], reverse=True)
        intl_equity_funds.sort(key=lambda x: x['equity_value'], reverse=True)

        return {
            'total_value': total_value,
            'total_equity': total_equity,
            'india_equity_total': india_equity_total,
            'intl_equity_total': intl_equity_total,
            'asset_classes': asset_classes,
            'equity_sub_categories': sub_categories,
            'cap_breakdown': cap_tiers,
            'india_equity_funds': india_equity_funds,
            'intl_equity_funds': intl_equity_funds,
        }
