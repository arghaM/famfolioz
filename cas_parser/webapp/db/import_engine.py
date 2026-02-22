"""CAS PDF import orchestrator — parses, deduplicates, and stores transaction data."""

import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime
from itertools import combinations
from typing import List, Optional, Tuple

from cas_parser.webapp.db.connection import get_db
from cas_parser.webapp.db.investors import get_investor_by_pan, update_investor
from cas_parser.webapp.db.folios import get_folio_by_number_and_isin, create_folio, get_unmapped_folios
from cas_parser.webapp.db.holdings import upsert_holding
from cas_parser.webapp.db.transactions import (
    generate_tx_hash, _compute_sequence_numbers, insert_transaction,
    get_pending_conflict_groups, get_conflict_group_transactions,
    resolve_conflict, get_conflict_stats
)
from cas_parser.webapp.db.mutual_funds import add_to_mutual_fund_master
from cas_parser.webapp.db.validation import (
    validate_folio_units, run_post_import_validation, add_to_quarantine
)

logger = logging.getLogger(__name__)

__all__ = [
    '_validate_balance_continuity',
    '_detect_reversal_pairs',
    '_find_excess_transactions',
    '_stage_and_analyze_transactions',
    '_validate_transaction_for_insert',
    'import_parsed_data',
]

_BUY_TYPES = {'purchase', 'sip', 'switch_in'}
_SELL_TYPES = {'redemption', 'switch_out'}


def _validate_balance_continuity(transactions: List[dict]) -> Tuple[List[dict], int]:
    """
    Two-phase transaction validation and repair:

    Phase 1 — Per-record cross-check: |amount| ≈ |units| × nav.
      If the identity fails, try reassigning the 4 raw values (amount, units,
      nav, balance) into the correct columns.

    Phase 2 — Balance-units continuity: balance[i] = balance[i-1] + units[i].
      Uses pairwise consistency to find anchors, then repairs isolated suspects
      using surrounding anchors.

    Returns:
        (transactions, repair_count) — transactions list is modified in-place.
    """
    SKIP_TYPES = {'stt', 'stamp_duty', 'charges', 'misc'}
    repair_count = 0

    # ================================================================
    # PHASE 1: Per-record amount ≈ units × nav validation
    # ================================================================
    for tx in transactions:
        if tx.get('type', '') in SKIP_TYPES:
            continue

        amount = float(tx.get('amount', 0) or 0)
        units = float(tx.get('units', 0) or 0)
        nav = float(tx.get('nav', 0) or 0)
        balance = float(tx.get('balance_units', 0) or 0)

        # Skip if any essential value is zero (can't validate)
        if nav == 0 or units == 0:
            continue

        expected_amount = abs(units) * nav
        actual_amount = abs(amount)

        # Check if amount ≈ units × nav (1% tolerance)
        if expected_amount > 0 and actual_amount > 0:
            ratio = actual_amount / expected_amount
            if 0.99 <= ratio <= 1.01:
                continue  # consistent, no repair needed

        # Cross-check failed — try all permutations of the 4 raw values
        raw = [abs(amount), abs(units), nav, balance]
        best_fit = None
        best_error = float('inf')

        for i_amt in range(4):
            for i_units in range(4):
                if i_units == i_amt:
                    continue
                for i_nav in range(4):
                    if i_nav == i_amt or i_nav == i_units:
                        continue
                    c_amount = raw[i_amt]
                    c_units = raw[i_units]
                    c_nav = raw[i_nav]
                    # NAV must be in plausible range
                    if not (1 <= c_nav <= 100000):
                        continue
                    if c_units == 0:
                        continue
                    expected = c_units * c_nav
                    if expected == 0:
                        continue
                    error = abs(c_amount - expected) / expected
                    if error < best_error:
                        best_error = error
                        # The remaining index is balance
                        i_bal = 6 - i_amt - i_units - i_nav  # sum of 0,1,2,3 = 6
                        best_fit = (c_amount, c_units, c_nav, raw[i_bal],
                                    i_amt, i_units, i_nav, i_bal)

        if best_fit and best_error < 0.01:
            c_amount, c_units, c_nav, c_balance = best_fit[:4]

            # Preserve signs from original
            if amount < 0 or units < 0:
                c_amount = -c_amount
            if units < 0:
                c_units = -c_units

            old_vals = f"amt={amount}, units={units}, nav={nav}, bal={balance}"
            tx['amount'] = str(c_amount)
            tx['units'] = str(c_units)
            tx['nav'] = str(c_nav)
            tx['balance_units'] = str(c_balance)
            new_vals = f"amt={c_amount}, units={c_units}, nav={c_nav}, bal={c_balance}"
            repair_count += 1
            logger.warning(
                f"[per-record-validation] REPAIRED tx "
                f"date={tx.get('date', '?')}, "
                f"desc={tx.get('description', '?')[:50]}: "
                f"{old_vals} → {new_vals} (error={best_error:.6f})"
            )

    # ================================================================
    # PHASE 2: Balance-units continuity
    # ================================================================
    # Group by (folio, isin) — each group has independent balance track
    groups = defaultdict(list)
    for idx, tx in enumerate(transactions):
        folio = tx.get('folio', '')
        isin = tx.get('isin', '')
        if folio and isin:
            groups[(folio, isin)].append((idx, tx))

    for (folio, isin), group_txs in groups.items():
        # Sort by date, preserving original order for same date
        group_txs.sort(key=lambda x: x[1].get('date', ''))

        # Build list of verifiable txs (skip charges; balance=0 IS valid)
        verifiable = []
        for g_idx, (_, tx) in enumerate(group_txs):
            tx_type = tx.get('type', '')
            if tx_type in SKIP_TYPES:
                tx['_anchor'] = True
                continue
            # Only skip if balance_units is truly missing (None/empty)
            bal_raw = tx.get('balance_units')
            if bal_raw is None or bal_raw == '':
                continue
            verifiable.append((g_idx, tx))

        # Pairwise consistency check
        for v_idx, (g_idx, tx) in enumerate(verifiable):
            balance_i = float(tx.get('balance_units', 0) or 0)
            units_i = float(tx.get('units', 0) or 0)

            has_forward = False
            if v_idx > 0:
                prev_tx = verifiable[v_idx - 1][1]
                prev_balance = float(prev_tx.get('balance_units', 0) or 0)
                if abs(prev_balance + units_i - balance_i) < 0.01:
                    has_forward = True

            has_backward = False
            if v_idx < len(verifiable) - 1:
                next_tx = verifiable[v_idx + 1][1]
                next_balance = float(next_tx.get('balance_units', 0) or 0)
                next_units = float(next_tx.get('units', 0) or 0)
                if abs(balance_i + next_units - next_balance) < 0.01:
                    has_backward = True

            if v_idx == 0 or v_idx == len(verifiable) - 1:
                tx['_anchor'] = True
            elif has_forward or has_backward:
                tx['_anchor'] = True
            else:
                tx['_anchor'] = False
                logger.info(
                    f"[balance-continuity] SUSPECT tx in {folio}/{isin}: "
                    f"balance={balance_i}, units={units_i}, "
                    f"date={tx.get('date', '?')}"
                )

        # Repair isolated suspects using surrounding anchors
        for g_idx, (orig_idx, tx) in enumerate(group_txs):
            if tx.get('_anchor', True):
                continue

            # Find prev anchor balance
            prev_anchor_balance = 0.0
            for j in range(g_idx - 1, -1, -1):
                if group_txs[j][1].get('_anchor', False):
                    prev_anchor_balance = float(
                        group_txs[j][1].get('balance_units', 0) or 0)
                    break

            # Find next anchor
            next_anchor_idx = None
            for j in range(g_idx + 1, len(group_txs)):
                jtx = group_txs[j][1]
                if jtx.get('_anchor', False):
                    next_anchor_idx = j
                    break

            if next_anchor_idx is None:
                logger.warning(
                    f"[balance-continuity] No next anchor for suspect in "
                    f"{folio}/{isin} date={tx.get('date', '?')} — skip")
                continue

            # Check no consecutive suspects
            has_other_suspects = any(
                not group_txs[j][1].get('_anchor', True)
                for j in range(g_idx + 1, next_anchor_idx))
            if has_other_suspects:
                logger.warning(
                    f"[balance-continuity] Consecutive suspects in "
                    f"{folio}/{isin} — skip")
                continue

            next_anchor_balance = float(
                group_txs[next_anchor_idx][1].get('balance_units', 0) or 0)

            # Compute correct balance and units
            intervening_units = sum(
                float(group_txs[j][1].get('units', 0) or 0)
                for j in range(g_idx + 1, next_anchor_idx + 1)
                if group_txs[j][1].get('type', '') not in SKIP_TYPES)

            correct_balance = next_anchor_balance - intervening_units
            correct_units = correct_balance - prev_anchor_balance

            old_units = float(tx.get('units', 0) or 0)
            old_balance = float(tx.get('balance_units', 0) or 0)

            tx['units'] = str(correct_units)
            tx['balance_units'] = str(correct_balance)

            # Try to fix NAV/amount with the corrected units
            old_amount = float(tx.get('amount', 0) or 0)
            old_nav = float(tx.get('nav', 0) or 0)
            raw_values = [abs(old_amount), abs(old_units), old_nav, old_balance]

            nav_fixed = False
            if abs(correct_units) > 0.001:
                for c_nav in raw_values:
                    if not (1 <= c_nav <= 100000):
                        continue
                    exp_amt = abs(correct_units) * c_nav
                    for c_amt in raw_values:
                        if c_amt == c_nav:
                            continue
                        if exp_amt > 0 and abs(exp_amt - c_amt) / exp_amt < 0.01:
                            tx['nav'] = str(c_nav)
                            tx['amount'] = str(
                                -c_amt if old_amount < 0 else c_amt)
                            nav_fixed = True
                            break
                    if nav_fixed:
                        break

            repair_count += 1
            logger.warning(
                f"[balance-continuity] REPAIRED tx in {folio}/{isin} "
                f"date={tx.get('date', '?')}: "
                f"units: {old_units}→{correct_units:.4f}, "
                f"balance: {old_balance}→{correct_balance:.4f}")

    # Clean up
    for tx in transactions:
        tx.pop('_anchor', None)

    return transactions, repair_count


def _detect_reversal_pairs(txs: list) -> set:
    """Find same-day transaction pairs within a folio group that cancel each other out.

    Detects two kinds of reversal pairs:

    1. Cross-type: buy-type + sell-type with matching units/amounts
       (e.g. purchase + redemption on same day)

    2. Same-type compensating: same tx_type, one positive units + one negative units
       (e.g. SIP purchase + its cancellation due to "payment not received")
       The negative-units member must have matching magnitude.

    Returns set of local indices (into txs) that are reversal pair members.
    """
    reversal_indices = set()

    # Group by date
    date_groups = defaultdict(list)
    for i, tx in enumerate(txs):
        date_groups[tx.get('date', '')].append(i)

    for date_key, indices in date_groups.items():
        if len(indices) < 2:
            continue

        # --- Pass 1: Cross-type pairs (buy + sell) ---
        buys = []
        sells = []
        for i in indices:
            tx_type = txs[i].get('type', '')
            if tx_type in _BUY_TYPES:
                buys.append(i)
            elif tx_type in _SELL_TYPES:
                sells.append(i)

        if buys and sells:
            used_sells = set()
            for bi in buys:
                buy_units = abs(float(txs[bi].get('units', 0)))
                buy_amount = abs(float(txs[bi].get('amount', 0) or 0))

                for si in sells:
                    if si in used_sells:
                        continue
                    sell_units = abs(float(txs[si].get('units', 0)))
                    sell_amount = abs(float(txs[si].get('amount', 0) or 0))

                    if abs(buy_units - sell_units) > 0.01:
                        continue

                    amount_diff = abs(buy_amount - sell_amount)
                    max_amount = max(buy_amount, sell_amount, 0.01)
                    if amount_diff > 1.0 and (amount_diff / max_amount) > 0.01:
                        continue

                    reversal_indices.add(bi)
                    reversal_indices.add(si)
                    used_sells.add(si)
                    break

        # --- Pass 2: Same-type compensating pairs (positive + negative units) ---
        # Groups by tx_type, then matches positive-unit txs with negative-unit txs
        # e.g. purchase +30 units paired with purchase -30 units ("payment not received")
        type_groups = defaultdict(list)
        for i in indices:
            if i in reversal_indices:
                continue  # already matched in pass 1
            type_groups[txs[i].get('type', '')].append(i)

        for tx_type, type_indices in type_groups.items():
            positives = [i for i in type_indices if float(txs[i].get('units', 0)) > 0]
            negatives = [i for i in type_indices if float(txs[i].get('units', 0)) < 0]

            if not positives or not negatives:
                continue

            used_negs = set()
            for pi in positives:
                pos_units = abs(float(txs[pi].get('units', 0)))
                pos_amount = abs(float(txs[pi].get('amount', 0) or 0))

                for ni in negatives:
                    if ni in used_negs:
                        continue
                    neg_units = abs(float(txs[ni].get('units', 0)))
                    neg_amount = abs(float(txs[ni].get('amount', 0) or 0))

                    if abs(pos_units - neg_units) > 0.01:
                        continue

                    amount_diff = abs(pos_amount - neg_amount)
                    max_amount = max(pos_amount, neg_amount, 0.01)
                    if amount_diff > 1.0 and (amount_diff / max_amount) > 0.01:
                        continue

                    reversal_indices.add(pi)
                    reversal_indices.add(ni)
                    used_negs.add(ni)
                    break

    return reversal_indices


def _find_excess_transactions(group_txs: list, excess: float) -> set:
    """Find the smallest subset of transactions whose units sum to excess.

    Used when the last transaction's balance_units doesn't match the CAS
    closing balance.  The excess = last_tx_balance - closing_balance, so
    we look for a subset whose unit sum ≈ excess (within 0.01 tolerance).

    Args:
        group_txs: List of (global_index, tx_dict) tuples for one folio group.
        excess: The unit difference to account for.

    Returns:
        Set of *local* indices into group_txs to exclude, or None if no
        solution found.
    """
    tol = 0.01

    # Build candidate list: (local_idx, units)
    candidates = []
    for local_idx, (_, tx) in enumerate(group_txs):
        units = float(tx.get('units', 0))
        candidates.append((local_idx, units))

    # Strategy 1: single transaction with units ≈ excess
    for local_idx, units in candidates:
        if abs(units - excess) < tol:
            return {local_idx}

    # Strategy 2: pair summing to ≈ excess
    for (i, u_i), (j, u_j) in combinations(candidates, 2):
        if abs((u_i + u_j) - excess) < tol:
            return {i, j}

    # Strategy 3: combos of size 3-4 (capped at 50 candidates to avoid explosion)
    if len(candidates) <= 50:
        for size in (3, 4):
            for combo in combinations(candidates, size):
                if abs(sum(u for _, u in combo) - excess) < tol:
                    return {idx for idx, _ in combo}

    return None


def _stage_and_analyze_transactions(transactions: list, holdings: list) -> dict:
    """Pre-analyze CAS transactions before insertion to determine per-folio strategy.

    Uses a **closing-balance-first** approach for each (folio, isin) group:

    1. If the last transaction's balance_units matches the CAS closing balance
       (within 0.01), all transactions are valid — no reversal detection needed.
       → strategy='closing_balance_match', balance_validated=True

    2. If there's an excess (last_tx_balance - closing != 0), try to find the
       smallest subset of transactions whose units sum to that excess and mark
       them as reversed.
       → strategy='excess_excluded', balance_validated=True

    3. If no closing balance is available from CAS holdings, fall back to
       pattern-based reversal detection (_detect_reversal_pairs).
       → strategy='pattern_fallback', balance_validated=False

    4. If closing balance exists but no subset solves the excess, mark as
       disputed for manual review.
       → strategy='disputed', balance_validated=False

    Returns dict keyed by (folio_number, isin) with analysis metadata.
    """
    # Build holdings lookup: {(folio_number, isin): closing_units}
    closing_balances = {}
    for h in holdings:
        key = (h.get('folio', ''), h.get('isin', ''))
        closing_balances[key] = float(h.get('units', 0) or 0)

    # Group transactions by (folio, isin), preserving original indices
    groups = defaultdict(list)
    for idx, tx in enumerate(transactions):
        key = (tx.get('folio', ''), tx.get('isin', ''))
        groups[key].append((idx, tx))

    # Analyze each group
    analysis = {}
    total_reversals = 0
    validated_count = 0

    for key, group_txs in groups.items():
        # Sort by date (preserve original order for same date via stable sort)
        group_txs.sort(key=lambda x: x[1].get('date', ''))

        closing = closing_balances.get(key)
        # Use the last transaction with non-zero balance_units (skip stamp_duty/stt
        # rows that have balance_units=0 and would break the comparison).
        last_balance = 0
        for _, tx in reversed(group_txs):
            bal = float(tx.get('balance_units', 0) or 0)
            if bal > 0:
                last_balance = bal
                break

        reversal_global = set()
        balance_validated = False
        strategy = 'unknown'

        if closing is not None:
            excess = last_balance - closing

            if abs(excess) < 0.01:
                # All transactions valid — closing balance matches
                strategy = 'closing_balance_match'
                balance_validated = True
            else:
                # Try to find a subset whose units sum to the excess
                exclude_local = _find_excess_transactions(group_txs, excess)
                if exclude_local is not None:
                    strategy = 'excess_excluded'
                    balance_validated = True
                    reversal_global = {group_txs[i][0] for i in exclude_local}
                    logger.info(
                        f"[staging] {key}: excess={excess:.4f}, "
                        f"excluded {len(exclude_local)} txs to match closing balance"
                    )
                else:
                    # Cannot reconcile — fall back to conflict detection
                    strategy = 'disputed'
                    balance_validated = False
                    logger.warning(
                        f"[staging] {key}: excess={excess:.4f}, "
                        f"no subset found — marking as disputed"
                    )
        else:
            # No closing balance available — use pattern-based detection
            strategy = 'pattern_fallback'
            just_txs = [tx for _, tx in group_txs]
            reversal_indices_local = _detect_reversal_pairs(just_txs)
            reversal_global = {group_txs[i][0] for i in reversal_indices_local}

        total_reversals += len(reversal_global)
        if balance_validated:
            validated_count += 1

        analysis[key] = {
            'reversal_indices': reversal_global,
            'balance_validated': balance_validated,
            'closing_balance': closing,
            'last_tx_balance': last_balance,
            'strategy': strategy,
        }

    logger.info(
        f"[staging] {len(groups)} folio groups analyzed: "
        f"{validated_count} balance-validated, "
        f"{total_reversals} reversal pair members detected"
    )

    return analysis


def _validate_transaction_for_insert(
    amount: float, units: float, nav: float
) -> tuple:
    """
    Cross-validate amount, units, and NAV before persisting.

    Uses the identity: amount = |units| × nav to detect and fix
    a single corrupt value when the other two are consistent.

    Returns:
        Tuple of (amount, units, nav) — corrected if needed.
    """
    abs_units = abs(units)
    abs_amount = abs(amount)

    # Step 1: NAV range check
    if nav <= 0 or nav > 100000:
        if abs_amount > 0 and abs_units > 0:
            recomputed_nav = abs_amount / abs_units
            if 1 <= recomputed_nav <= 100000:
                logger.warning(
                    f"[persistence] Correcting NAV from {nav} to {recomputed_nav:.4f} "
                    f"(amount={amount}, units={units})"
                )
                nav = recomputed_nav
            else:
                logger.warning(
                    f"[persistence] NAV={nav} out of range, recomputed={recomputed_nav:.4f} "
                    f"also invalid — leaving as-is"
                )

    # Step 2: Cross-validate amount vs units × nav
    if nav > 0 and abs_units > 0:
        expected = abs_units * nav
        if expected > 0:
            ratio = abs_amount / expected
            if ratio >= 100:
                corrected_amount = expected
                if amount < 0:
                    corrected_amount = -corrected_amount
                logger.warning(
                    f"[persistence] Correcting amount from {amount} to {corrected_amount:.2f} "
                    f"(units={units}, nav={nav}, ratio={ratio:.1f})"
                )
                amount = corrected_amount
            elif ratio <= 0.01:
                corrected_units = abs_amount / nav
                if units < 0:
                    corrected_units = -corrected_units
                logger.warning(
                    f"[persistence] Correcting units from {units} to {corrected_units:.4f} "
                    f"(amount={amount}, nav={nav}, ratio={ratio:.6f})"
                )
                units = corrected_units

    return amount, units, nav


def import_parsed_data(parsed_data: dict, source_filename: str = None) -> dict:
    """
    Import parsed CAS data into the database.

    Args:
        parsed_data: Parsed CAS data dict
        source_filename: Original PDF filename (for quarantine tracking)

    Returns a summary of what was imported and what needs mapping.
    """
    result = {
        'new_folios': [],
        'existing_folios': [],
        'unmapped_folios': [],
        'new_transactions': 0,
        'duplicate_transactions': 0,
        'skipped_discarded': 0,
        'conflict_transactions': 0,
        'reversed_transactions': 0,
        'repaired_transactions': 0,
        'conflict_stats': {},
        'investor_id': None,
        'investor_found': False,
    }

    # Check if investor exists by PAN
    investor_data = parsed_data.get('investor', {})
    pan = investor_data.get('pan')

    # Get statement period from validation or parsed data
    validation = parsed_data.get('validation', {})
    statement_from = validation.get('statement_from') or parsed_data.get('statement_from')
    statement_to = validation.get('statement_to') or parsed_data.get('statement_to')

    if pan:
        existing_investor = get_investor_by_pan(pan)
        if existing_investor:
            result['investor_id'] = existing_investor['id']
            result['investor_found'] = True
            # Only update email/mobile if not already set - NEVER overwrite name
            # User may have set a custom name they want to keep
            # Always update CAS upload tracking
            update_investor(
                existing_investor['id'],
                name=None,  # Don't overwrite existing name
                email=investor_data.get('email') if not existing_investor.get('email') else None,
                mobile=investor_data.get('mobile') if not existing_investor.get('mobile') else None,
                last_cas_upload=datetime.now().isoformat(),
                statement_from_date=statement_from,
                statement_to_date=statement_to
            )
        # NOTE: Do NOT auto-create investor on first import
        # Admin must manually create and map investors via the Map Folios page
        # This gives admin control over investor names and prevents duplicate investors

    # Process holdings and create folios
    for holding in parsed_data.get('holdings', []):
        folio_number = holding.get('folio', '')
        isin = holding.get('isin', '')

        if not folio_number or not isin:
            continue

        # Add to mutual fund master
        add_to_mutual_fund_master(
            scheme_name=holding.get('scheme_name', ''),
            isin=isin,
            amc=holding.get('amc', '')
        )

        # Check if folio exists
        existing_folio = get_folio_by_number_and_isin(folio_number, isin)

        if existing_folio:
            folio_id = existing_folio['id']
            result['existing_folios'].append({
                'id': folio_id,
                'folio_number': folio_number,
                'scheme_name': holding.get('scheme_name', ''),
                'investor_id': existing_folio.get('investor_id')
            })
        else:
            # Create new folio
            folio_id = create_folio(
                folio_number=folio_number,
                scheme_name=holding.get('scheme_name', ''),
                isin=isin,
                amc=holding.get('amc'),
                registrar=holding.get('registrar'),
                investor_id=result['investor_id']  # May be None
            )
            result['new_folios'].append({
                'id': folio_id,
                'folio_number': folio_number,
                'scheme_name': holding.get('scheme_name', ''),
                'isin': isin,
                'amc': holding.get('amc')
            })

        # Update holding
        upsert_holding(
            folio_id=folio_id,
            units=float(holding.get('units', 0)),
            nav=float(holding.get('nav', 0)),
            nav_date=holding.get('nav_date', ''),
            current_value=float(holding.get('current_value', 0))
        )

    # Process transactions
    folio_cache = {}  # Cache folio lookups

    # Balance-units continuity validation and repair
    transactions = parsed_data.get('transactions', [])
    transactions, repair_count = _validate_balance_continuity(transactions)
    result['repaired_transactions'] = repair_count

    # Stage and analyze transactions: detect reversal pairs and validate closing balances
    analysis = _stage_and_analyze_transactions(transactions, parsed_data.get('holdings', []))

    # Compute sequence numbers so duplicate-fingerprint txs get distinct hashes
    sequence_map = _compute_sequence_numbers(transactions)

    for idx, tx in enumerate(transactions):
        folio_number = tx.get('folio', '')
        isin = tx.get('isin', '')

        if not folio_number:
            continue

        # Get or create folio
        cache_key = f"{folio_number}|{isin}"
        if cache_key in folio_cache:
            folio_id = folio_cache[cache_key]
        else:
            folio = get_folio_by_number_and_isin(folio_number, isin)
            if folio:
                folio_id = folio['id']
            else:
                # Create folio from transaction
                folio_id = create_folio(
                    folio_number=folio_number,
                    scheme_name=tx.get('scheme_name', ''),
                    isin=isin,
                    investor_id=result['investor_id']
                )
            folio_cache[cache_key] = folio_id

        # Extract and cross-validate values before persisting
        amount = float(tx.get('amount', 0) or 0)
        units = float(tx.get('units', 0))
        nav = float(tx.get('nav', 0) or 0)
        amount, units, nav = _validate_transaction_for_insert(amount, units, nav)

        # Determine insertion strategy from staging analysis
        folio_key = (folio_number, isin)
        folio_analysis = analysis.get(folio_key, {})
        is_reversal = idx in folio_analysis.get('reversal_indices', set())
        skip_conflicts = folio_analysis.get('balance_validated', False)
        tx_sequence = sequence_map.get(idx, 0)

        if is_reversal:
            # Reversal pair member — insert as reversed for audit trail
            _, status = insert_transaction(
                folio_id=folio_id,
                tx_date=tx.get('date', ''),
                tx_type=tx.get('type', 'unknown'),
                description=tx.get('description', ''),
                amount=amount,
                units=units,
                nav=nav,
                balance_units=float(tx.get('balance_units', 0)),
                folio_number=folio_number,
                detect_conflicts=False,
                force_status='reversed',
                sequence=tx_sequence
            )
        elif skip_conflicts:
            # Balance validated — trust the CAS, no conflict detection
            _, status = insert_transaction(
                folio_id=folio_id,
                tx_date=tx.get('date', ''),
                tx_type=tx.get('type', 'unknown'),
                description=tx.get('description', ''),
                amount=amount,
                units=units,
                nav=nav,
                balance_units=float(tx.get('balance_units', 0)),
                folio_number=folio_number,
                detect_conflicts=False,
                sequence=tx_sequence
            )
        else:
            # Fallback — existing conflict detection behavior
            _, status = insert_transaction(
                folio_id=folio_id,
                tx_date=tx.get('date', ''),
                tx_type=tx.get('type', 'unknown'),
                description=tx.get('description', ''),
                amount=amount,
                units=units,
                nav=nav,
                balance_units=float(tx.get('balance_units', 0)),
                folio_number=folio_number,
                detect_conflicts=True,
                sequence=tx_sequence
            )

        if status == 'inserted':
            result['new_transactions'] += 1
        elif status == 'duplicate':
            result['duplicate_transactions'] += 1
        elif status == 'discarded':
            result['skipped_discarded'] += 1
        elif status == 'conflict':
            result['conflict_transactions'] += 1
        elif status == 'reversed':
            result['reversed_transactions'] += 1

    # Add staging analysis summary to result
    reversal_count = sum(len(a.get('reversal_indices', set())) for a in analysis.values())
    validated_folio_count = sum(1 for a in analysis.values() if a.get('balance_validated'))
    result['reversal_pairs_detected'] = reversal_count
    result['balance_validated_folios'] = validated_folio_count

    # Auto-resolve conflict groups where accepting all pending transactions
    # produces the correct unit balance (i.e. "Accept All Recommended" scenarios)
    result['auto_resolved_conflicts'] = 0
    conflict_groups = get_pending_conflict_groups()
    for group in conflict_groups:
        group_id = group['conflict_group_id']
        group_txs = get_conflict_group_transactions(group_id)
        if not group_txs:
            continue

        folio_id = group_txs[0]['folio_id']
        validation = validate_folio_units(folio_id)

        # Auto-resolve only when accepting ALL pending txs matches expected units
        if validation.get('issue_type') == 'pending_conflicts':
            all_hashes = [tx['tx_hash'] for tx in group_txs]
            res = resolve_conflict(group_id, all_hashes)
            result['auto_resolved_conflicts'] += res.get('activated', 0)
            # Reclassify: these were conflicts, now they're active inserts
            result['conflict_transactions'] -= res.get('activated', 0)
            result['new_transactions'] += res.get('activated', 0)
            logger.info(
                f"[auto-resolve] Conflict group {group_id}: "
                f"accepted {res['activated']} transactions "
                f"(unit balance matches expected)"
            )

    # Reconcile holding units with final transaction balance.
    # The CAS holdings section can be stale (e.g. not reflecting rejected purchases),
    # while transactions have the correct final balance_units.
    # NOTE: We use tx_date (not MAX(id)) to find the latest transaction because
    # CAS PDFs may be imported out of order, giving older transactions higher IDs.
    result['holdings_reconciled'] = 0
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT h.folio_id, h.units as holding_units, h.nav,
                   latest_tx.balance_units as tx_balance
            FROM holdings h
            JOIN (
                SELECT t.folio_id, t.balance_units
                FROM transactions t
                INNER JOIN (
                    SELECT folio_id, MAX(tx_date) as max_date
                    FROM transactions
                    WHERE status = 'active'
                      AND tx_type IN ('purchase', 'sip', 'switch_in', 'redemption', 'switch_out')
                      AND balance_units > 0
                    GROUP BY folio_id
                ) latest ON t.folio_id = latest.folio_id AND t.tx_date = latest.max_date
                WHERE t.status = 'active'
                  AND t.tx_type IN ('purchase', 'sip', 'switch_in', 'redemption', 'switch_out')
                  AND t.balance_units > 0
                GROUP BY t.folio_id
                HAVING t.id = MAX(t.id)
            ) latest_tx ON latest_tx.folio_id = h.folio_id
            WHERE ABS(h.units - latest_tx.balance_units) / MAX(h.units, 0.01) > 0.001
        """)
        mismatches = cursor.fetchall()

        for row in mismatches:
            fid = row['folio_id']
            old_units = row['holding_units']
            new_units = row['tx_balance']
            nav = row['nav'] or 0
            new_value = new_units * nav
            cursor.execute(
                "UPDATE holdings SET units = ?, current_value = ?, updated_at = CURRENT_TIMESTAMP WHERE folio_id = ?",
                (new_units, new_value, fid)
            )
            result['holdings_reconciled'] += 1
            logger.info(f"Reconciled folio {fid}: units {old_units} -> {new_units} (from final transaction balance)")

    # Get unmapped folios and conflict stats
    result['unmapped_folios'] = get_unmapped_folios()
    result['conflict_stats'] = get_conflict_stats()

    # Process quarantined items (items with broken ISINs)
    quarantine = parsed_data.get('quarantine', [])
    result['quarantined'] = 0
    if quarantine:
        import uuid
        import_batch_id = str(uuid.uuid4())[:8]
        for item in quarantine:
            add_to_quarantine(
                partial_isin=item.get('partial_isin', ''),
                scheme_name=item.get('scheme_name', ''),
                amc=item.get('amc', ''),
                folio_number=item.get('folio_number', ''),
                data_type=item.get('data_type', ''),
                data=item.get('data', {}),
                import_batch_id=import_batch_id,
                source_filename=source_filename
            )
            result['quarantined'] += 1
        logger.warning(f"Quarantined {result['quarantined']} items with broken ISINs (batch: {import_batch_id})")

    # Run post-import validation to check if transaction units match holdings
    validation_result = run_post_import_validation(result.get('investor_id'))
    result['validation'] = validation_result

    return result
