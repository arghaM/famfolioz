"""
ISIN Resolver for handling truncated/missing ISINs in CAS statements.

This module provides multiple strategies for recovering full ISINs:
1. AMFI database lookup (official source)
2. Local cache of known mappings
3. Fuzzy matching on scheme names
"""

import json
import logging
import re
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

import requests

logger = logging.getLogger(__name__)

# Cache directory for AMFI data
CACHE_DIR = Path(__file__).parent / "data"
AMFI_CACHE_FILE = CACHE_DIR / "amfi_schemes.json"
MANUAL_MAPPINGS_FILE = CACHE_DIR / "manual_isin_mappings.json"

# AMFI NAV API endpoint (contains scheme names and ISINs)
AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"


class ISINResolver:
    """
    Resolves truncated or missing ISINs using multiple strategies.
    """

    def __init__(self):
        """Initialize the ISIN resolver."""
        self._amfi_data: Dict[str, Dict] = {}  # ISIN -> scheme info
        self._scheme_name_index: Dict[str, str] = {}  # normalized_name -> ISIN
        self._manual_mappings: Dict[str, str] = {}  # scheme_pattern -> ISIN
        self._load_caches()

    def _load_caches(self) -> None:
        """Load cached data from files."""
        CACHE_DIR.mkdir(exist_ok=True)

        # Load AMFI cache
        if AMFI_CACHE_FILE.exists():
            try:
                with open(AMFI_CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    self._amfi_data = data.get('schemes', {})
                    self._build_name_index()
                    logger.info(f"Loaded {len(self._amfi_data)} schemes from AMFI cache")
            except Exception as e:
                logger.warning(f"Failed to load AMFI cache: {e}")

        # Load manual mappings
        if MANUAL_MAPPINGS_FILE.exists():
            try:
                with open(MANUAL_MAPPINGS_FILE, 'r') as f:
                    self._manual_mappings = json.load(f)
                    logger.info(f"Loaded {len(self._manual_mappings)} manual ISIN mappings")
            except Exception as e:
                logger.warning(f"Failed to load manual mappings: {e}")

    def _build_name_index(self) -> None:
        """Build an index of normalized scheme names for fast lookup."""
        self._scheme_name_index = {}
        for isin, info in self._amfi_data.items():
            name = info.get('scheme_name', '')
            normalized = self._normalize_scheme_name(name)
            if normalized:
                self._scheme_name_index[normalized] = isin

    def _normalize_scheme_name(self, name: str) -> str:
        """Normalize a scheme name for matching."""
        if not name:
            return ""
        # Convert to lowercase
        name = name.lower()
        # Remove common suffixes/prefixes
        name = re.sub(r'\s*-\s*(direct|regular)\s*(plan|growth|dividend|idcw).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*\(.*?\)', '', name)  # Remove parenthetical content
        name = re.sub(r'\s*(fund|scheme)\s*$', '', name, flags=re.IGNORECASE)
        # Normalize whitespace
        name = ' '.join(name.split())
        return name.strip()

    def refresh_amfi_data(self) -> bool:
        """
        Refresh AMFI scheme data from the official source.

        Returns:
            True if successful, False otherwise.
        """
        logger.info("Fetching AMFI scheme data...")
        try:
            response = requests.get(AMFI_NAV_URL, timeout=30)
            response.raise_for_status()

            schemes = self._parse_amfi_nav_data(response.text)

            if schemes:
                self._amfi_data = schemes
                self._build_name_index()

                # Save to cache
                CACHE_DIR.mkdir(exist_ok=True)
                with open(AMFI_CACHE_FILE, 'w') as f:
                    json.dump({
                        'schemes': schemes,
                        'count': len(schemes)
                    }, f, indent=2)

                logger.info(f"Cached {len(schemes)} schemes from AMFI")
                return True
            else:
                logger.warning("No schemes parsed from AMFI data")
                return False

        except Exception as e:
            logger.error(f"Failed to fetch AMFI data: {e}")
            return False

    def _parse_amfi_nav_data(self, text: str) -> Dict[str, Dict]:
        """
        Parse AMFI NAV text file format.

        Format:
        Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
        """
        schemes = {}
        current_amc = ""

        for line in text.strip().split('\n'):
            line = line.strip()
            if not line:
                continue

            # AMC header line (no semicolons, not a data line)
            if ';' not in line and line and not line[0].isdigit():
                current_amc = line
                continue

            parts = line.split(';')
            if len(parts) >= 4:
                scheme_code = parts[0].strip()
                isin_payout = parts[1].strip() if len(parts) > 1 else ""
                isin_growth = parts[2].strip() if len(parts) > 2 else ""
                scheme_name = parts[3].strip() if len(parts) > 3 else ""
                nav = parts[4].strip() if len(parts) > 4 else ""

                # Use growth ISIN if available, otherwise payout
                isin = isin_growth if isin_growth.startswith('INF') else isin_payout

                if isin and isin.startswith('INF') and len(isin) == 12:
                    schemes[isin] = {
                        'scheme_code': scheme_code,
                        'scheme_name': scheme_name,
                        'amc': current_amc,
                        'nav': nav
                    }

        return schemes

    def resolve_isin(
        self,
        partial_isin: str,
        scheme_name: str,
        amc: Optional[str] = None
    ) -> Optional[str]:
        """
        Try to resolve a full ISIN from partial ISIN and scheme name.

        Args:
            partial_isin: Partial/truncated ISIN (e.g., "INF109")
            scheme_name: Scheme name from CAS
            amc: Optional AMC name for additional matching

        Returns:
            Full 12-character ISIN if found, None otherwise.
        """
        # Strategy 1: Check manual mappings first
        isin = self._check_manual_mappings(partial_isin, scheme_name)
        if isin:
            logger.info(f"Resolved ISIN from manual mapping: {isin}")
            return isin

        # Strategy 2: Try AMFI database lookup
        isin = self._lookup_in_amfi(partial_isin, scheme_name, amc)
        if isin:
            logger.info(f"Resolved ISIN from AMFI database: {isin}")
            return isin

        # Strategy 3: Fuzzy match on scheme name
        isin = self._fuzzy_match_scheme(scheme_name, partial_isin)
        if isin:
            logger.info(f"Resolved ISIN from fuzzy matching: {isin}")
            return isin

        return None

    def _check_manual_mappings(self, partial_isin: str, scheme_name: str) -> Optional[str]:
        """Check manual mappings for a match."""
        scheme_lower = scheme_name.lower()

        for pattern, isin in self._manual_mappings.items():
            if pattern.lower() in scheme_lower:
                # Verify partial ISIN matches if provided
                if partial_isin and not isin.startswith(partial_isin):
                    continue
                return isin

        return None

    def _lookup_in_amfi(
        self,
        partial_isin: str,
        scheme_name: str,
        amc: Optional[str] = None
    ) -> Optional[str]:
        """Look up ISIN in AMFI database."""
        if not self._amfi_data:
            return None

        candidates = []

        for isin, info in self._amfi_data.items():
            # Check if partial ISIN matches
            if partial_isin and not isin.startswith(partial_isin):
                continue

            # Calculate scheme name similarity
            amfi_name = info.get('scheme_name', '')
            similarity = self._calculate_similarity(scheme_name, amfi_name)

            if similarity > 0.6:  # 60% similarity threshold
                candidates.append((isin, similarity, info))

        if candidates:
            # Sort by similarity descending
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_match = candidates[0]
            logger.debug(f"Best AMFI match: {best_match[2]['scheme_name']} (similarity: {best_match[1]:.2f})")
            return best_match[0]

        return None

    def _fuzzy_match_scheme(self, scheme_name: str, partial_isin: str) -> Optional[str]:
        """Fuzzy match scheme name against index."""
        if not self._scheme_name_index:
            return None

        normalized = self._normalize_scheme_name(scheme_name)
        if not normalized:
            return None

        best_match = None
        best_score = 0.0

        for indexed_name, isin in self._scheme_name_index.items():
            # Skip if partial ISIN doesn't match
            if partial_isin and not isin.startswith(partial_isin):
                continue

            score = SequenceMatcher(None, normalized, indexed_name).ratio()
            if score > best_score and score > 0.7:  # 70% threshold for fuzzy match
                best_score = score
                best_match = isin

        return best_match

    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two scheme names."""
        # Normalize both names
        n1 = self._normalize_scheme_name(name1)
        n2 = self._normalize_scheme_name(name2)

        if not n1 or not n2:
            return 0.0

        return SequenceMatcher(None, n1, n2).ratio()

    def add_manual_mapping(self, scheme_pattern: str, isin: str) -> bool:
        """
        Add a manual mapping for ISIN resolution.

        Args:
            scheme_pattern: Pattern to match in scheme name (case-insensitive)
            isin: Full 12-character ISIN

        Returns:
            True if added successfully.
        """
        if not isin or len(isin) != 12 or not isin.startswith('INF'):
            logger.error(f"Invalid ISIN format: {isin}")
            return False

        self._manual_mappings[scheme_pattern] = isin

        # Save to file
        try:
            CACHE_DIR.mkdir(exist_ok=True)
            with open(MANUAL_MAPPINGS_FILE, 'w') as f:
                json.dump(self._manual_mappings, f, indent=2)
            logger.info(f"Added manual mapping: '{scheme_pattern}' -> {isin}")
            return True
        except Exception as e:
            logger.error(f"Failed to save manual mapping: {e}")
            return False

    def remove_manual_mapping(self, scheme_pattern: str) -> bool:
        """Remove a manual mapping."""
        if scheme_pattern in self._manual_mappings:
            del self._manual_mappings[scheme_pattern]
            try:
                with open(MANUAL_MAPPINGS_FILE, 'w') as f:
                    json.dump(self._manual_mappings, f, indent=2)
                return True
            except Exception as e:
                logger.error(f"Failed to save manual mappings: {e}")
        return False

    def get_manual_mappings(self) -> Dict[str, str]:
        """Get all manual mappings."""
        return self._manual_mappings.copy()

    def get_amfi_scheme_count(self) -> int:
        """Get count of schemes in AMFI cache."""
        return len(self._amfi_data)


# Global resolver instance
_resolver: Optional[ISINResolver] = None


def get_isin_resolver() -> ISINResolver:
    """Get the global ISIN resolver instance."""
    global _resolver
    if _resolver is None:
        _resolver = ISINResolver()
    return _resolver


def resolve_isin(partial_isin: str, scheme_name: str, amc: Optional[str] = None) -> Optional[str]:
    """Convenience function to resolve an ISIN."""
    return get_isin_resolver().resolve_isin(partial_isin, scheme_name, amc)


def refresh_amfi_data() -> bool:
    """Convenience function to refresh AMFI data."""
    return get_isin_resolver().refresh_amfi_data()


def add_manual_isin_mapping(scheme_pattern: str, isin: str) -> bool:
    """Convenience function to add a manual ISIN mapping."""
    return get_isin_resolver().add_manual_mapping(scheme_pattern, isin)
