"""
Finite State Machine for CAS section detection.

This module implements a state machine that identifies different sections
of a CDSL CAS PDF using semantic markers rather than fixed positions.
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SectionState(Enum):
    """
    States of the CAS section detection FSM.

    The parser transitions through these states as it encounters
    different sections of the CAS document.
    """
    INITIAL = auto()
    INVESTOR_INFO = auto()
    HOLDINGS_SUMMARY = auto()
    TRANSACTION_DETAILS = auto()
    END = auto()


@dataclass
class Section:
    """
    Represents a detected section in the CAS document.

    Attributes:
        section_type: Type of section (from SectionState)
        start_line: Index of the first line of this section
        end_line: Index of the last line of this section (exclusive)
        lines: Actual text lines in this section
    """
    section_type: SectionState
    start_line: int
    end_line: int
    lines: List[str] = field(default_factory=list)


class SectionPatterns:
    """
    Regex patterns for detecting different CAS sections.

    These patterns use semantic markers to identify sections,
    allowing the parser to handle format drift between CAS versions.
    """

    # Investor Information Section markers
    INVESTOR_MARKERS = [
        r"(?i)personal\s*information",
        r"(?i)investor\s*details",
        r"(?i)account\s*holder\s*details",
        r"(?i)statement\s*for\s*the\s*period",
        r"(?i)consolidated\s*account\s*statement",
    ]

    # Holdings Summary Section markers
    HOLDINGS_MARKERS = [
        r"(?i)mutual\s*fund.*summary",
        r"(?i)summary\s*of\s*mutual\s*fund",
        r"(?i)scheme\s*name.*ISIN",
        r"(?i)ISIN.*NAV",
        r"(?i)folio\s*no.*units.*nav",
        r"(?i)market\s*value\s*of.*holdings",
        r"(?i)portfolio\s*summary",
        r"(?i)folio\s*no\s*:",
    ]

    # Transaction Details Section markers
    TRANSACTION_MARKERS = [
        r"(?i)transaction\s*statement",
        r"(?i)statement\s*of\s*transactions",
        r"(?i)transaction\s*details",
        r"(?i)details\s*of\s*transactions",
        r"(?i)transaction\s*history",
    ]

    # PAN pattern (appears in investor info)
    PAN_PATTERN = r"\b[A-Z]{5}[0-9]{4}[A-Z]\b"

    # ISIN pattern (appears in holdings and transactions)
    ISIN_PATTERN = r"\bINF[A-Z0-9]{9}\b"

    # Date pattern (common in transactions)
    DATE_PATTERN = r"\b\d{2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}\b"

    # Folio pattern
    FOLIO_PATTERN = r"(?i)folio\s*(?:no\.?|number)?\s*:?\s*\d+"

    # NAV pattern
    NAV_PATTERN = r"(?i)nav\s*:?\s*[\d,]+\.\d+"

    # End markers
    END_MARKERS = [
        r"(?i)this\s*is\s*a\s*computer\s*generated",
        r"(?i)statement\s*generated\s*on",
        r"(?i)end\s*of\s*statement",
        r"(?i)^\s*page\s*\d+\s*of\s*\d+\s*$",
    ]


class SectionDetector:
    """
    Finite State Machine for detecting sections in CDSL CAS documents.

    The detector transitions through states based on semantic markers
    found in the text, identifying the boundaries of each section.
    """

    def __init__(self):
        """Initialize the section detector with default state."""
        self.current_state = SectionState.INITIAL
        self.patterns = SectionPatterns()
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compile regex patterns for efficiency."""
        self.investor_re = [re.compile(p) for p in self.patterns.INVESTOR_MARKERS]
        self.holdings_re = [re.compile(p) for p in self.patterns.HOLDINGS_MARKERS]
        self.transaction_re = [re.compile(p) for p in self.patterns.TRANSACTION_MARKERS]
        self.end_re = [re.compile(p) for p in self.patterns.END_MARKERS]
        self.pan_re = re.compile(self.patterns.PAN_PATTERN)
        self.isin_re = re.compile(self.patterns.ISIN_PATTERN)
        self.date_re = re.compile(self.patterns.DATE_PATTERN)
        self.folio_re = re.compile(self.patterns.FOLIO_PATTERN)
        self.nav_re = re.compile(self.patterns.NAV_PATTERN)

    def detect_sections(self, lines: List[str]) -> List[Section]:
        """
        Detect all sections in the given text lines.

        Args:
            lines: List of text lines from the PDF.

        Returns:
            List of Section objects with detected boundaries.
        """
        self.current_state = SectionState.INITIAL
        sections: List[Section] = []
        current_section_start: Optional[int] = None
        current_section_type: Optional[SectionState] = None

        logger.info(f"Detecting sections in {len(lines)} lines")

        for i, line in enumerate(lines):
            new_state = self._check_transition(line, i, lines)

            if new_state != self.current_state:
                # Close previous section
                if current_section_type is not None and current_section_start is not None:
                    section = Section(
                        section_type=current_section_type,
                        start_line=current_section_start,
                        end_line=i,
                        lines=lines[current_section_start:i],
                    )
                    sections.append(section)
                    logger.debug(
                        f"Closed {current_section_type.name} section: "
                        f"lines {current_section_start}-{i}"
                    )

                # Start new section
                if new_state != SectionState.END:
                    current_section_start = i
                    current_section_type = new_state
                    logger.debug(f"Started {new_state.name} section at line {i}")
                else:
                    current_section_start = None
                    current_section_type = None

                self.current_state = new_state

        # Close final section
        if current_section_type is not None and current_section_start is not None:
            section = Section(
                section_type=current_section_type,
                start_line=current_section_start,
                end_line=len(lines),
                lines=lines[current_section_start:],
            )
            sections.append(section)
            logger.debug(
                f"Closed final {current_section_type.name} section: "
                f"lines {current_section_start}-{len(lines)}"
            )

        logger.info(f"Detected {len(sections)} sections")
        return sections

    def _check_transition(
        self, line: str, line_index: int, all_lines: List[str]
    ) -> SectionState:
        """
        Check if the current line triggers a state transition.

        Args:
            line: Current text line.
            line_index: Index of the current line.
            all_lines: All lines for lookahead context.

        Returns:
            New state if transition occurs, otherwise current state.
        """
        # Check for end markers first
        if self._matches_any(line, self.end_re):
            return SectionState.END

        # State-specific transitions
        if self.current_state == SectionState.INITIAL:
            return self._transition_from_initial(line, line_index, all_lines)

        elif self.current_state == SectionState.INVESTOR_INFO:
            return self._transition_from_investor(line, line_index, all_lines)

        elif self.current_state == SectionState.HOLDINGS_SUMMARY:
            return self._transition_from_holdings(line, line_index, all_lines)

        elif self.current_state == SectionState.TRANSACTION_DETAILS:
            return self._transition_from_transactions(line, line_index, all_lines)

        return self.current_state

    def _transition_from_initial(
        self, line: str, line_index: int, all_lines: List[str]
    ) -> SectionState:
        """Handle transitions from INITIAL state."""
        # Look for investor info markers
        if self._matches_any(line, self.investor_re):
            return SectionState.INVESTOR_INFO

        # PAN often appears early in investor section
        if self.pan_re.search(line):
            return SectionState.INVESTOR_INFO

        # Check for direct holdings section (some formats skip explicit investor header)
        if self._matches_any(line, self.holdings_re):
            return SectionState.HOLDINGS_SUMMARY

        return SectionState.INITIAL

    def _transition_from_investor(
        self, line: str, line_index: int, all_lines: List[str]
    ) -> SectionState:
        """Handle transitions from INVESTOR_INFO state."""
        # Look for holdings section markers
        if self._matches_any(line, self.holdings_re):
            return SectionState.HOLDINGS_SUMMARY

        # ISIN with NAV patterns indicate holdings
        if self.isin_re.search(line) and self._has_nearby_nav(line_index, all_lines):
            return SectionState.HOLDINGS_SUMMARY

        # Transaction section markers
        if self._matches_any(line, self.transaction_re):
            return SectionState.TRANSACTION_DETAILS

        return SectionState.INVESTOR_INFO

    def _transition_from_holdings(
        self, line: str, line_index: int, all_lines: List[str]
    ) -> SectionState:
        """Handle transitions from HOLDINGS_SUMMARY state."""
        # Transaction section markers
        if self._matches_any(line, self.transaction_re):
            return SectionState.TRANSACTION_DETAILS

        # Date pattern with transaction-like structure
        if self.date_re.search(line) and self._looks_like_transaction(line):
            return SectionState.TRANSACTION_DETAILS

        return SectionState.HOLDINGS_SUMMARY

    def _transition_from_transactions(
        self, line: str, line_index: int, all_lines: List[str]
    ) -> SectionState:
        """Handle transitions from TRANSACTION_DETAILS state."""
        # Transaction section usually goes until end
        # Could transition back to holdings for multi-fund statements
        if self._matches_any(line, self.holdings_re):
            # Check if this is a new holdings section or continuation
            if self._is_new_holdings_section(line_index, all_lines):
                return SectionState.HOLDINGS_SUMMARY

        return SectionState.TRANSACTION_DETAILS

    def _matches_any(self, line: str, patterns: List[re.Pattern]) -> bool:
        """Check if line matches any of the given patterns."""
        return any(p.search(line) for p in patterns)

    def _has_nearby_nav(
        self, line_index: int, all_lines: List[str], window: int = 3
    ) -> bool:
        """Check if NAV pattern appears near the given line."""
        start = max(0, line_index - window)
        end = min(len(all_lines), line_index + window + 1)
        for i in range(start, end):
            if self.nav_re.search(all_lines[i]):
                return True
        return False

    def _looks_like_transaction(self, line: str) -> bool:
        """Check if line looks like a transaction entry."""
        # Transaction typically has: date, description, amount/units
        has_date = self.date_re.search(line) is not None
        has_number = re.search(r"\d+\.\d{2,4}", line) is not None
        return has_date and has_number

    def _is_new_holdings_section(
        self, line_index: int, all_lines: List[str]
    ) -> bool:
        """
        Determine if this is a genuinely new holdings section.

        Some CAS formats intersperse holdings and transactions.
        """
        # Look for clear section header patterns
        if line_index < len(all_lines):
            line = all_lines[line_index]
            # Check for explicit section headers
            if re.search(r"(?i)summary\s*of\s*holdings", line):
                return True
            if re.search(r"(?i)mutual\s*fund\s*summary", line):
                return True
        return False


def detect_sections(lines: List[str]) -> List[Section]:
    """
    Convenience function to detect sections in CAS text lines.

    Args:
        lines: List of text lines from the PDF.

    Returns:
        List of Section objects with detected boundaries.
    """
    detector = SectionDetector()
    return detector.detect_sections(lines)


def get_section_by_type(
    sections: List[Section], section_type: SectionState
) -> Optional[Section]:
    """
    Get the first section of a specific type.

    Args:
        sections: List of detected sections.
        section_type: Type of section to find.

    Returns:
        First matching section or None.
    """
    for section in sections:
        if section.section_type == section_type:
            return section
    return None


def get_all_sections_by_type(
    sections: List[Section], section_type: SectionState
) -> List[Section]:
    """
    Get all sections of a specific type.

    Args:
        sections: List of detected sections.
        section_type: Type of section to find.

    Returns:
        List of matching sections.
    """
    return [s for s in sections if s.section_type == section_type]
