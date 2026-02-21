# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CDSL CAS (Consolidated Account Statement) PDF Parser - A production-grade Python library for extracting investor details, mutual fund holdings, and transaction history from CDSL CAS PDFs.

## Build and Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install dev dependencies
pip install -r requirements-dev.txt

# Install in development mode
pip install -e .

# Run all tests
pytest cas_parser/tests/

# Run specific test file
pytest cas_parser/tests/test_validator.py

# Run specific test
pytest cas_parser/tests/test_validator.py::TestValidateISIN::test_valid_isin

# Run with coverage
pytest cas_parser/tests/ --cov=cas_parser --cov-report=html

# Lint
flake8 cas_parser/
black --check cas_parser/
isort --check cas_parser/

# Type check
mypy cas_parser/
```

## CLI Usage

```bash
# Parse a CAS PDF
python -m cas_parser.main statement.pdf

# Output to JSON file
python -m cas_parser.main statement.pdf -o output.json

# With password for encrypted PDF
python -m cas_parser.main statement.pdf -p mypassword

# Validation only
python -m cas_parser.main statement.pdf --validate-only

# Verbose output
python -m cas_parser.main statement.pdf -v
```

## Architecture

The parser uses a **Finite State Machine (FSM)** for section detection:

```
INITIAL → INVESTOR_INFO → HOLDINGS_SUMMARY → TRANSACTION_DETAILS → END
```

Key modules:
- `models.py` - Dataclasses: `Investor`, `Holding`, `Transaction`, `CASStatement`
- `extractor.py` - PDF text extraction using pdfplumber
- `section_detector.py` - FSM for detecting CAS sections using semantic markers
- `holdings_parser.py` - Parse mutual fund holdings (handles multi-line scheme names)
- `transactions_parser.py` - Parse transaction history with type detection
- `validator.py` - Validation rules (value calculations, ISIN/PAN format, consistency)
- `main.py` - CLI interface and orchestration

## Key Design Decisions

1. **Semantic Markers**: Uses regex patterns to detect sections (not fixed positions) for format drift tolerance
2. **Decimal for Money**: All monetary values use `Decimal` to avoid floating-point errors
3. **Transaction Type Detection**: `TransactionTypeDetector` class with keyword pattern matching
4. **Validation Tolerance**: 1% tolerance for value calculations (units × NAV ≈ current_value)

## Edge Cases Handled

- Segregated portfolio entries (detected by "Segregated" keyword)
- Switch in/out transactions (detect and pair)
- Dividend reinvestment (units positive, not redemption)
- STT and charges (do not reduce units)
- Multi-line scheme names
- Multiple page continuation

## Important Patterns

ISIN: `INF[A-Z0-9]{9}` (12 characters starting with INF)
PAN: `[A-Z]{5}[0-9]{4}[A-Z]` (10 characters)
Date formats: `DD-Mon-YYYY`, `DD/MM/YYYY`
