"""
PDF text extraction module for CDSL CAS Parser.

This module handles raw text extraction from PDF files using pdfplumber,
preserving the line structure needed for subsequent parsing.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

import pdfplumber

logger = logging.getLogger(__name__)


@dataclass
class PageContent:
    """
    Represents extracted content from a single PDF page.

    Attributes:
        page_number: 1-indexed page number
        lines: List of text lines extracted from the page
        raw_text: Complete raw text of the page
    """
    page_number: int
    lines: List[str] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class ExtractedDocument:
    """
    Represents the complete extracted content from a PDF document.

    Attributes:
        pages: List of page contents
        total_pages: Total number of pages in the document
        source_path: Path to the source PDF file
    """
    pages: List[PageContent] = field(default_factory=list)
    total_pages: int = 0
    source_path: Optional[str] = None

    def get_all_lines(self) -> List[str]:
        """
        Get all lines from all pages as a flat list.

        Returns:
            List of all text lines across all pages.
        """
        all_lines = []
        for page in self.pages:
            all_lines.extend(page.lines)
        return all_lines

    def get_all_text(self) -> str:
        """
        Get complete text from all pages.

        Returns:
            Complete text content of the document.
        """
        return "\n".join(page.raw_text for page in self.pages)


class PDFExtractor:
    """
    Extracts text content from CDSL CAS PDF files.

    This class uses pdfplumber for text extraction, handling multi-page
    documents and preserving line structure for parsing.
    """

    def __init__(self, password: Optional[str] = None):
        """
        Initialize the PDF extractor.

        Args:
            password: Optional password for encrypted PDFs.
        """
        self.password = password

    def extract(self, pdf_path: Union[str, Path]) -> ExtractedDocument:
        """
        Extract text content from a PDF file.

        Args:
            pdf_path: Path to the PDF file.

        Returns:
            ExtractedDocument containing all extracted text.

        Raises:
            FileNotFoundError: If the PDF file does not exist.
            ValueError: If the file is not a valid PDF.
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        if not pdf_path.suffix.lower() == ".pdf":
            raise ValueError(f"File is not a PDF: {pdf_path}")

        logger.info(f"Extracting text from PDF: {pdf_path}")

        document = ExtractedDocument(source_path=str(pdf_path))

        try:
            with pdfplumber.open(pdf_path, password=self.password) as pdf:
                document.total_pages = len(pdf.pages)
                logger.info(f"PDF has {document.total_pages} pages")

                for page_num, page in enumerate(pdf.pages, start=1):
                    page_content = self._extract_page(page, page_num)
                    document.pages.append(page_content)
                    logger.debug(
                        f"Page {page_num}: extracted {len(page_content.lines)} lines"
                    )

        except Exception as e:
            logger.error(f"Failed to extract PDF: {e}")
            raise

        return document

    def _extract_page(self, page: pdfplumber.page.Page, page_number: int) -> PageContent:
        """
        Extract text from a single PDF page.

        Args:
            page: pdfplumber page object.
            page_number: 1-indexed page number.

        Returns:
            PageContent with extracted lines and raw text.
        """
        content = PageContent(page_number=page_number)

        # Try extraction without layout first (often preserves text better)
        # Then fall back to layout mode if needed
        raw_text = None

        # Method 1: Simple extraction (preserves character sequences better)
        try:
            raw_text = page.extract_text(
                x_tolerance=2,
                y_tolerance=2,
            )
        except Exception as e:
            logger.debug(f"Simple extraction failed: {e}")

        # Method 2: Layout-aware extraction as fallback
        if not raw_text or len(raw_text.strip()) < 100:
            try:
                raw_text_layout = page.extract_text(
                    layout=True,
                    x_tolerance=3,
                    y_tolerance=3,
                )
                if raw_text_layout and len(raw_text_layout) > len(raw_text or ""):
                    raw_text = raw_text_layout
            except Exception as e:
                logger.debug(f"Layout extraction failed: {e}")

        if raw_text:
            content.raw_text = raw_text
            # Split into lines and clean up
            content.lines = self._clean_lines(raw_text.split("\n"))
        else:
            logger.warning(f"No text extracted from page {page_number}")
            content.raw_text = ""
            content.lines = []

        return content

    def _clean_lines(self, lines: List[str]) -> List[str]:
        """
        Clean and normalize extracted text lines.

        Args:
            lines: Raw lines from PDF extraction.

        Returns:
            Cleaned lines with normalized whitespace.
        """
        cleaned = []
        for line in lines:
            # Normalize whitespace but preserve structure
            # Replace multiple spaces with single space, but keep leading/trailing
            normalized = " ".join(line.split())
            if normalized:  # Skip completely empty lines
                cleaned.append(normalized)
        return cleaned


def extract_text_from_pdf(
    pdf_path: Union[str, Path],
    password: Optional[str] = None,
) -> ExtractedDocument:
    """
    Convenience function to extract text from a PDF file.

    Args:
        pdf_path: Path to the PDF file.
        password: Optional password for encrypted PDFs.

    Returns:
        ExtractedDocument containing all extracted text.
    """
    extractor = PDFExtractor(password=password)
    return extractor.extract(pdf_path)
