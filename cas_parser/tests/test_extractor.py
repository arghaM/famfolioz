"""Tests for PDF extractor."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cas_parser.extractor import (
    ExtractedDocument,
    PageContent,
    PDFExtractor,
    extract_text_from_pdf,
)


class TestPageContent:
    """Tests for PageContent dataclass."""

    def test_create_page_content(self):
        """Test creating page content."""
        content = PageContent(
            page_number=1,
            lines=["Line 1", "Line 2", "Line 3"],
            raw_text="Line 1\nLine 2\nLine 3",
        )

        assert content.page_number == 1
        assert len(content.lines) == 3
        assert "Line 1" in content.raw_text


class TestExtractedDocument:
    """Tests for ExtractedDocument dataclass."""

    def test_create_empty_document(self):
        """Test creating an empty document."""
        doc = ExtractedDocument()

        assert len(doc.pages) == 0
        assert doc.total_pages == 0
        assert doc.source_path is None

    def test_get_all_lines(self):
        """Test getting all lines from document."""
        page1 = PageContent(page_number=1, lines=["A", "B"], raw_text="A\nB")
        page2 = PageContent(page_number=2, lines=["C", "D"], raw_text="C\nD")

        doc = ExtractedDocument(pages=[page1, page2], total_pages=2)

        lines = doc.get_all_lines()

        assert len(lines) == 4
        assert lines == ["A", "B", "C", "D"]

    def test_get_all_text(self):
        """Test getting all text from document."""
        page1 = PageContent(page_number=1, lines=["A"], raw_text="Page 1 text")
        page2 = PageContent(page_number=2, lines=["B"], raw_text="Page 2 text")

        doc = ExtractedDocument(pages=[page1, page2], total_pages=2)

        text = doc.get_all_text()

        assert "Page 1 text" in text
        assert "Page 2 text" in text


class TestPDFExtractor:
    """Tests for PDFExtractor class."""

    def test_extractor_init(self):
        """Test initializing extractor."""
        extractor = PDFExtractor()
        assert extractor.password is None

        extractor_with_pass = PDFExtractor(password="secret")
        assert extractor_with_pass.password == "secret"

    def test_extract_file_not_found(self):
        """Test extracting non-existent file raises error."""
        extractor = PDFExtractor()

        with pytest.raises(FileNotFoundError):
            extractor.extract("/nonexistent/path/file.pdf")

    def test_extract_invalid_extension(self, tmp_path):
        """Test extracting non-PDF file raises error."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Not a PDF")

        extractor = PDFExtractor()

        with pytest.raises(ValueError, match="not a PDF"):
            extractor.extract(str(txt_file))

    @patch("cas_parser.extractor.pdfplumber")
    def test_extract_with_mock(self, mock_pdfplumber, tmp_path):
        """Test extraction with mocked pdfplumber."""
        # Create a dummy PDF file
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 dummy content")

        # Mock pdfplumber
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Line 1\nLine 2\nLine 3"

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)

        mock_pdfplumber.open.return_value = mock_pdf

        extractor = PDFExtractor()
        doc = extractor.extract(str(pdf_file))

        assert doc.total_pages == 1
        assert len(doc.pages) == 1
        assert len(doc.pages[0].lines) > 0

    def test_clean_lines(self):
        """Test line cleaning."""
        extractor = PDFExtractor()

        lines = [
            "  Line with   spaces  ",
            "",
            "Normal line",
            "   ",
            "Another line",
        ]

        cleaned = extractor._clean_lines(lines)

        assert "Line with spaces" in cleaned
        assert "Normal line" in cleaned
        assert "Another line" in cleaned
        assert "" not in cleaned  # Empty lines removed


class TestConvenienceFunction:
    """Tests for extract_text_from_pdf convenience function."""

    def test_extract_file_not_found(self):
        """Test that convenience function raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            extract_text_from_pdf("/nonexistent/file.pdf")
