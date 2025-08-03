import os
import sys
import pandas as pd

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the ExcelProcessor class
from src.core.excel_processor import ExcelProcessor

def main():
    # Create an instance of ExcelProcessor
    processor = ExcelProcessor()
    
    # Define paths to sample files
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sample_dir = os.path.join(base_dir, 'data', 'sample')
    
    kh_bank_file = os.path.join(sample_dir, 'KH_Bank.XLSX')
    customer_ledger_file = os.path.join(sample_dir, 'Customer_Ledger_Entries_FULL.xlsx')
    
    # Check if sample files exist
    for file_path in [kh_bank_file, customer_ledger_file]:
        if not os.path.exists(file_path):
            print(f"Error: Sample file not found: {file_path}")
            return
    
    print("\n=== Loading Files ===")
    # Load both files
    results = processor.load_files([kh_bank_file, customer_ledger_file])
    
    # Print load results
    for file_path, success in results.items():
        print(f"Loaded {os.path.basename(file_path)}: {success}")
    
    # Process KH_Bank.XLSX
    print("\n=== Processing KH_Bank.XLSX ===")
    process_file(processor, kh_bank_file)
    
    # Process Customer_Ledger_Entries_FULL.xlsx
    print("\n=== Processing Customer_Ledger_Entries_FULL.xlsx ===")
    process_file(processor, customer_ledger_file)

def process_file(processor, file_path):
    # Get sheet information
    print("\nSheet Information:")
    sheet_info = processor.get_sheet_info(file_path)
    
    for sheet_name, info in sheet_info.items():
        print(f"\nSheet: {sheet_name}")
        print(f"  Rows: {info['rows']}")
        print(f"  Columns: {info['columns']}")
        print(f"  Column Names: {', '.join(info['column_names'][:5])}{'...' if len(info['column_names']) > 5 else ''}")
        print(f"  Dimensions: {info['openpyxl_dimensions']}")
    
    # Preview data from each sheet
    print("\nData Preview:")
    for sheet_name in sheet_info.keys():
        print(f"\nPreview of sheet '{sheet_name}':")
        preview = processor.preview_data(sheet_name, rows=3, file_path=file_path)
        print(preview)

if __name__ == "__main__":
    main()