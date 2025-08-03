# Financial Data Parser

A robust financial data parsing system that can process Excel files, intelligently detect data types, handle various formats, and store data in optimized structures for fast retrieval.

## Project Overview

This project provides tools for processing financial data from Excel files with the following capabilities:

- Excel file processing with multiple libraries (pandas and openpyxl)
- Data type detection and validation
- Format parsing for financial data
- Data structure optimization for performance
- Error handling and data quality assurance

## Installation

1. Clone the repository
   
   git clone https://github.com/meerabijaz/CodeGeneration.git

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Project Structure

```
├── config/              # Configuration files
├── data/                # Data directory
│   ├── processed/       # Processed data output
│   └── sample/          # Sample input files
├── examples/            # Example usage scripts
├── scripts/             # Utility scripts
├── src/                 # Source code
│   ├── core/            # Core functionality
│   └── utils/           # Utility functions
└── tests/               # Test cases
```

## Usage

Basic usage example:

```python
from src.core.excel_processor import ExcelProcessor

# Create an instance of ExcelProcessor
processor = ExcelProcessor()

# Load an Excel file
processor.load_file('path/to/excel_file.xlsx')

# Get sheet information
sheet_info = processor.get_sheet_info()
print(sheet_info)

# Preview data from a sheet
preview = processor.preview_data('Sheet1', rows=5)
print(preview)
```

See the `examples` directory for more detailed usage examples.

## Features

### Phase 1: Basic Excel Processing

- Read Excel files using pandas and openpyxl
- Handle multiple worksheets within each file
- Display basic file information (sheets, dimensions, column names)

### Phase 2: Data Type Detection

- Intelligent column classification for:
  - String columns (account names, descriptions, categories, etc.)
  - Number columns (financial amounts, quantities, percentages, etc.)
  - Date columns (transaction dates, reporting periods, etc.)
- Advanced detection features:
  - Pattern recognition for common financial data formats
  - Confidence scoring for type detection
  - Format classification within each data type
  - Support for various number formats (standard, accounting, currency)
  - Support for different date formats (MM/DD/YYYY, DD/MM/YYYY, Excel serial dates)

Example usage of the DataTypeDetector:

```python
from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import DataTypeDetector

# Initialize components
excel_processor = ExcelProcessor()
type_detector = DataTypeDetector()

# Load an Excel file
excel_processor.load_file('path/to/excel_file.xlsx')

# Extract data from a sheet
df = excel_processor.extract_data('Sheet1')

# Analyze the DataFrame to detect column types
results = type_detector.analyze_dataframe(df)

# Display results
for column, result in results.items():
    print(f"{column}: {result['type']} (confidence: {result['confidence']}, format: {result['format']})")
```

### Phase 3: Format Parsing

- Handle various amount formats:
  - US format ($1,234.56)
  - European format (€1.234,56)
  - Indian format (₹1,23,456)
  - Accounting format ((1,234.56) or 1,234.56-)
  - Abbreviated formats (1.5M, 2.3K)
  - Credit/Debit indicators (CR 1,000.00, DR 500.00)
- Parse different date formats:
  - Standard formats (MM/DD/YYYY, DD/MM/YYYY, YYYY-MM-DD)
  - Excel serial dates (44927)
  - Quarter notations (Q1 2023)
  - Month abbreviations (Jan-23, Dec-2023)
- Currency normalization:
  - Detect and standardize currency symbols
  - Convert to standard decimal values
  - Preserve currency information
- Special format handling:
  - Account codes
  - Reference numbers
  - Percentages
  - Identifiers

Example usage of the FormatParser:

```python
from src.core.format_parser import FormatParser

# Initialize the FormatParser
parser = FormatParser()

# Parse amount values
amount = parser.parse_amount("$1,234.56")  # Returns 1234.56
negative_amount = parser.parse_amount("(2,500.00)")  # Returns -2500.0

# Parse date values
date = parser.parse_date("12/31/2023")  # Returns datetime.date(2023, 12, 31)
excel_date = parser.parse_date("44927")  # Returns datetime.date(2023, 1, 1)

# Normalize currency
normalized = parser.normalize_currency("€1.234,56")
# Returns {'value': 1234.56, 'currency': 'EUR'}

# Handle special formats
standardized = parser.handle_special_formats("ACC-12345", "account_code")
# Returns "ACC12345"
```

### Phase 4: Data Structure Implementation

- Multiple storage options:
  - In-memory storage for fast processing
  - SQLite storage for persistence and larger datasets
  - File-based storage (CSV) for compatibility and portability
- Advanced querying capabilities:
  - Equality filters (column = value)
  - Range filters (column > value, column < value, column between values)
  - List filters (column in [value1, value2, ...])
  - Text search (column contains text)
  - Compound filters with multiple conditions
- Indexing for performance optimization:
  - Create indexes on frequently queried columns
  - Automatic index selection based on query type
  - Support for different index types based on data type
- Aggregation functions:
  - Group by one or more columns
  - Aggregate measures (sum, avg, min, max, count)
  - Multiple aggregations in a single query
- Metadata management:
  - Dataset information (rows, columns, data types)
  - Index information
  - Storage statistics

Example usage of the DataStorage class:

```python
from src.core.data_storage import DataStorage
from src.core.excel_processor import ExcelProcessor

# Initialize components
excel_processor = ExcelProcessor()
storage = DataStorage(storage_type="memory")

# Load and process an Excel file
excel_processor.load_file('path/to/excel_file.xlsx')
df = excel_processor.extract_data('Sheet1')

# Store the data
storage.store_data("financial_data", df)

# Create indexes for faster querying
storage.create_index("financial_data", "transaction_date")
storage.create_index("financial_data", "amount")

# Query data with filters
results = storage.query_data(
    "financial_data",
    filters={
        "amount": {"gt": 1000},
        "transaction_date": {"between": ["2023-01-01", "2023-12-31"]},
        "category": {"in": ["Sales", "Revenue"]}
    }
)

# Aggregate data
aggregated = storage.aggregate_data(
    "financial_data",
    group_by=["category", "month"],
    measures={
        "amount": ["sum", "avg"],
        "transaction_id": ["count"]
    }
)

# Get metadata about the dataset
metadata = storage.get_metadata("financial_data")
print(f"Total rows: {metadata['rows']}")
print(f"Columns: {metadata['columns']}")
print(f"Indexes: {metadata['indexes']}")
```

## Testing

Run the tests using:

```bash
python -m unittest discover tests
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
