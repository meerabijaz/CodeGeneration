import sys
import os

# Add the src directory to the path so we can import the modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

from src.core.format_parser import FormatParser

def main():
    # Initialize the FormatParser
    parser = FormatParser()
    
    print("=== FormatParser Basic Examples ===")
    
    # Test amount parsing
    print("\n1. Amount Parsing Examples:")
    amount_examples = [
        "$1,234.56",
        "(2,500.00)",
        "€1.234,56",
        "1.5M",
        "₹1,23,456",
        "$-1,234.56",
        "CR 5,000.00",
        "DR 2,500.00",
        "1234.56-"
    ]
    
    for amount in amount_examples:
        parsed = parser.parse_amount(amount)
        print(f"  Original: {amount} -> Parsed: {parsed}")
    
    # Test date parsing
    print("\n2. Date Parsing Examples:")
    date_examples = [
        "12/31/2023",
        "2023-12-31",
        "Q4 2023",
        "Dec-23",
        "44927",  # Excel serial
        "31.12.2023",
        "31/12/23",
        "December 31, 2023"
    ]
    
    for date in date_examples:
        parsed = parser.parse_date(date)
        print(f"  Original: {date} -> Parsed: {parsed}")
    
    # Test currency normalization
    print("\n3. Currency Normalization Examples:")
    currency_examples = [
        "$1,234.56",
        "€1.234,56",
        "£1,234.56",
        "¥12,345",
        "₹1,23,456",
        "1,234.56 USD",
        "1.234,56 EUR"
    ]
    
    for currency in currency_examples:
        normalized = parser.normalize_currency(currency)
        print(f"  Original: {currency} -> Normalized: {normalized['value']} {normalized['currency']}")
    
    # Test special format handling
    print("\n4. Special Format Handling Examples:")
    special_examples = [
        {"value": "ACC-12345", "format": "account_code"},
        {"value": "REF/2023/12345", "format": "reference_number"},
        {"value": "CUST-001-A", "format": "identifier"},
        {"value": "25%", "format": "percentage"},
        {"value": "INV-2023-12345", "format": "invoice_number"}
    ]
    
    for example in special_examples:
        standardized = parser.handle_special_formats(example["value"], example["format"])
        print(f"  Original: {example['value']} (Format: {example['format']}) -> Standardized: {standardized}")
    
    # Test batch parsing
    print("\n5. Batch Parsing Examples:")
    batch_amounts = ["$1,000", "$2,000", "$3,000", "$4,000", "$5,000"]
    batch_dates = ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01"]
    
    parsed_amounts = parser.batch_parse_amounts(batch_amounts)
    parsed_dates = parser.batch_parse_dates(batch_dates)
    
    print(f"  Batch Amounts: {batch_amounts} -> Parsed: {parsed_amounts}")
    print(f"  Batch Dates: {batch_dates} -> Parsed: {parsed_dates}")

if __name__ == "__main__":
    main()