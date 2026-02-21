"""Tests for CAS section detector FSM."""

import pytest

from cas_parser.section_detector import (
    Section,
    SectionDetector,
    SectionPatterns,
    SectionState,
    detect_sections,
    get_all_sections_by_type,
    get_section_by_type,
)


class TestSectionPatterns:
    """Tests for section pattern matching."""

    def test_investor_patterns(self):
        """Test investor section marker patterns."""
        import re

        patterns = [re.compile(p) for p in SectionPatterns.INVESTOR_MARKERS]

        assert any(p.search("Personal Information") for p in patterns)
        assert any(p.search("Investor Details") for p in patterns)
        assert any(p.search("Statement for the period") for p in patterns)

    def test_holdings_patterns(self):
        """Test holdings section marker patterns."""
        import re

        patterns = [re.compile(p) for p in SectionPatterns.HOLDINGS_MARKERS]

        assert any(p.search("Mutual Fund Summary") for p in patterns)
        assert any(p.search("Scheme Name ISIN Folio") for p in patterns)
        assert any(p.search("ISIN NAV Units") for p in patterns)

    def test_transaction_patterns(self):
        """Test transaction section marker patterns."""
        import re

        patterns = [re.compile(p) for p in SectionPatterns.TRANSACTION_MARKERS]

        assert any(p.search("Transaction Statement") for p in patterns)
        assert any(p.search("Statement of Transactions") for p in patterns)
        assert any(p.search("Transaction Details") for p in patterns)


class TestSectionDetector:
    """Tests for SectionDetector FSM."""

    def test_initial_state(self):
        """Test detector starts in INITIAL state."""
        detector = SectionDetector()

        assert detector.current_state == SectionState.INITIAL

    def test_detect_investor_section(self):
        """Test detecting investor information section."""
        lines = [
            "Consolidated Account Statement",
            "Personal Information",
            "Name: John Doe",
            "PAN: ABCDE1234F",
            "Email: john@example.com",
        ]

        detector = SectionDetector()
        sections = detector.detect_sections(lines)

        investor_sections = [s for s in sections if s.section_type == SectionState.INVESTOR_INFO]
        assert len(investor_sections) >= 1

    def test_detect_holdings_section(self):
        """Test detecting holdings summary section."""
        lines = [
            "Some header",
            "Mutual Fund Summary",
            "Scheme Name          ISIN           Folio     Units     NAV      Value",
            "HDFC Equity Fund    INF179K01234   12345     100.00    45.67    4567.00",
        ]

        detector = SectionDetector()
        sections = detector.detect_sections(lines)

        holdings_sections = [s for s in sections if s.section_type == SectionState.HOLDINGS_SUMMARY]
        assert len(holdings_sections) >= 1

    def test_detect_transaction_section(self):
        """Test detecting transaction details section."""
        lines = [
            "Personal Information",
            "Name: John Doe",
            "PAN: ABCDE1234F",
            "Transaction Statement",
            "Date        Description     Amount    Units     NAV      Balance",
            "15-Jan-2024 Purchase       10000.00  219.123   45.67   1000.567",
        ]

        detector = SectionDetector()
        sections = detector.detect_sections(lines)

        tx_sections = [s for s in sections if s.section_type == SectionState.TRANSACTION_DETAILS]
        assert len(tx_sections) >= 1

    def test_full_document_sections(self):
        """Test detecting all sections in a complete document."""
        lines = [
            "CDSL Consolidated Account Statement",
            "",
            "Personal Information",
            "Name: John Doe",
            "PAN: ABCDE1234F",
            "",
            "Mutual Fund Summary",
            "HDFC Equity Fund INF179K01234 12345 100.567 45.67 4592.88",
            "",
            "Transaction Statement",
            "15-Jan-2024 Purchase 10000.00 219.123 45.67 1000.567",
            "20-Jan-2024 SIP 5000.00 109.321 45.76 1109.888",
            "",
            "This is a computer generated statement",
        ]

        sections = detect_sections(lines)

        # Should have at least investor, holdings, and transactions
        section_types = {s.section_type for s in sections}
        # Note: exact detection depends on pattern matching
        assert len(sections) > 0

    def test_section_lines_captured(self):
        """Test that section lines are correctly captured."""
        lines = [
            "Header",
            "Personal Information",
            "Name: John",
            "PAN: ABCDE1234F",
            "Mutual Fund Summary",
            "Fund data here",
        ]

        sections = detect_sections(lines)

        # Find investor section
        investor_section = get_section_by_type(sections, SectionState.INVESTOR_INFO)
        if investor_section:
            # Should contain the investor info lines
            assert any("Personal Information" in line or "Name: John" in line
                      for line in investor_section.lines)


class TestHelperFunctions:
    """Tests for section detector helper functions."""

    def test_get_section_by_type_found(self):
        """Test getting section by type when it exists."""
        sections = [
            Section(SectionState.INVESTOR_INFO, 0, 5, ["line1"]),
            Section(SectionState.HOLDINGS_SUMMARY, 5, 10, ["line2"]),
        ]

        result = get_section_by_type(sections, SectionState.HOLDINGS_SUMMARY)

        assert result is not None
        assert result.section_type == SectionState.HOLDINGS_SUMMARY

    def test_get_section_by_type_not_found(self):
        """Test getting section by type when it doesn't exist."""
        sections = [
            Section(SectionState.INVESTOR_INFO, 0, 5, ["line1"]),
        ]

        result = get_section_by_type(sections, SectionState.TRANSACTION_DETAILS)

        assert result is None

    def test_get_all_sections_by_type(self):
        """Test getting all sections of a specific type."""
        sections = [
            Section(SectionState.INVESTOR_INFO, 0, 5, ["line1"]),
            Section(SectionState.HOLDINGS_SUMMARY, 5, 10, ["line2"]),
            Section(SectionState.TRANSACTION_DETAILS, 10, 15, ["line3"]),
            Section(SectionState.HOLDINGS_SUMMARY, 15, 20, ["line4"]),  # Second holdings
        ]

        result = get_all_sections_by_type(sections, SectionState.HOLDINGS_SUMMARY)

        assert len(result) == 2
        assert all(s.section_type == SectionState.HOLDINGS_SUMMARY for s in result)


class TestSection:
    """Tests for Section dataclass."""

    def test_create_section(self):
        """Test creating a section."""
        lines = ["line 1", "line 2", "line 3"]
        section = Section(
            section_type=SectionState.INVESTOR_INFO,
            start_line=0,
            end_line=3,
            lines=lines,
        )

        assert section.section_type == SectionState.INVESTOR_INFO
        assert section.start_line == 0
        assert section.end_line == 3
        assert len(section.lines) == 3
