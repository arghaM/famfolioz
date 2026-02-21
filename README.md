# Famfolioz

Family portfolio tracker — parse CDSL CAS PDFs, track mutual funds, NPS, FDs, and more.

## Features

- Extract investor details (Name, PAN, DP ID, Client ID)
- Parse mutual fund holdings (scheme name, ISIN, folio, units, NAV, current value)
- Parse detailed transaction history including:
  - Purchase, Redemption, SIP
  - Switch In/Out, STP In/Out
  - Dividend Payout, Dividend Reinvestment
  - STT, Stamp Duty, Charges
  - Segregated Portfolio entries
- Validation rules with anomaly detection
- JSON export

## Installation

```bash
pip install -r requirements.txt
```

Or install in development mode:

```bash
pip install -e .
```

## Usage

### Command Line

```bash
# Parse a CAS PDF
python -m cas_parser.main statement.pdf

# Output to JSON file
python -m cas_parser.main statement.pdf -o output.json

# With password for encrypted PDF
python -m cas_parser.main statement.pdf -p mypassword

# Validation only
python -m cas_parser.main statement.pdf --validate-only
```

### Python API

```python
from cas_parser import parse_cas_pdf

# Parse PDF
statement = parse_cas_pdf("statement.pdf")

# Access investor info
print(f"Name: {statement.investor.name}")
print(f"PAN: {statement.investor.pan}")

# Access holdings
for holding in statement.holdings:
    print(f"{holding.scheme_name}: {holding.units} units @ {holding.nav}")

# Access transactions
for tx in statement.transactions:
    print(f"{tx.date}: {tx.transaction_type.value} - {tx.units} units")

# Check validation
if not statement.validation.is_valid:
    for error in statement.validation.errors:
        print(f"Error: {error}")

# Export to JSON
json_output = statement.to_dict()
```

## Architecture

The parser uses a Finite State Machine (FSM) for section detection:

```
INITIAL → INVESTOR_INFO → HOLDINGS_SUMMARY → TRANSACTION_DETAILS → END
```

### Modules

- `models.py` - Data models (Investor, Holding, Transaction, CASStatement)
- `extractor.py` - PDF text extraction using pdfplumber
- `section_detector.py` - FSM for section detection
- `holdings_parser.py` - Holdings parsing
- `transactions_parser.py` - Transaction parsing
- `validator.py` - Validation rules
- `main.py` - CLI and orchestration

## Validation Rules

1. **Value Calculation**: `units × NAV ≈ current_value` (1% tolerance)
2. **Unit Balance**: Sum of transaction units ≈ holding units
3. **ISIN Format**: Must match `INF[A-Z0-9]{9}`
4. **PAN Format**: Must match `[A-Z]{5}[0-9]{4}[A-Z]`

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=cas_parser --cov-report=html
```

## License

MIT License
