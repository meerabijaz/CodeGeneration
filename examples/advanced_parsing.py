import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add the src directory to the path so we can import the modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import DataTypeDetector
from src.core.format_parser import FormatParser

def main():
    # Initialize the components
    excel_processor = ExcelProcessor()
    type_detector = DataTypeDetector()
    format_parser = FormatParser()
    
    # Load the sample Excel files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'sample')
    kh_bank_path = os.path.join(data_dir, 'KH_Bank.XLSX')
    customer_ledger_path = os.path.join(data_dir, 'Customer_Ledger_Entries_FULL.xlsx')
    
    # Load the files
    excel_processor.load_file(kh_bank_path)
    excel_processor.load_file(customer_ledger_path)
    
    # Process each sheet
    print("\n=== Format Parsing Results ===")
    for file_path in excel_processor.files.keys():
        file_name = os.path.basename(file_path)
        print(f"\nFile: {file_name}")
        
        sheet_info = excel_processor.get_sheet_info(file_path)
        for sheet_name in sheet_info.keys():
            print(f"\n  Sheet: {sheet_name}")
            
            # Extract data from the sheet
            df = excel_processor.extract_data(sheet_name, file_path)
            
            if df.empty:
                print("    No data found in this sheet.")
                continue
            
            # Analyze the DataFrame to detect column types
            type_results = type_detector.analyze_dataframe(df)
            
            # Process a sample of each column based on its detected type
            print("    Column Format Parsing:")
            for column, type_result in type_results.items():
                # Skip columns with unknown type
                if type_result['type'] == 'unknown':
                    continue
                
                # Get a sample of non-null values from the column
                sample_values = df[column].dropna().head(5).tolist()
                if not sample_values:
                    continue
                
                print(f"\n      Column: {column} ({type_result['type']}, {type_result['format']})")
                print(f"      Sample Original Values: {sample_values}")
                
                # Parse the values based on their type
                if type_result['type'] == 'number':
                    parsed_values = format_parser.batch_parse_amounts(sample_values, type_result['format'])
                    print(f"      Parsed Number Values: {parsed_values}")
                    
                    # If it looks like currency, try to normalize it
                    if any(isinstance(v, str) and any(sym in v for sym in format_parser.currency_symbols) for v in sample_values):
                        normalized = [format_parser.normalize_currency(v) for v in sample_values]
                        print(f"      Normalized Currency Values:")
                        for i, n in enumerate(normalized):
                            print(f"        {sample_values[i]} -> {n['value']} {n['currency']}")
                
                elif type_result['type'] == 'date':
                    parsed_values = format_parser.batch_parse_dates(sample_values, type_result['format'])
                    print(f"      Parsed Date Values: {parsed_values}")
                
                elif type_result['type'] == 'string':
                    # Handle special string formats if applicable
                    if type_result['format'] in ['account_code', 'reference_number']:
                        parsed_values = [format_parser.handle_special_formats(v, type_result['format']) for v in sample_values]
                        print(f"      Standardized Values: {parsed_values}")
            
            # Create a visualization of the parsing results
            visualize_parsing_results(file_name, sheet_name, df, type_results, format_parser)

def visualize_parsing_results(file_name, sheet_name, df, type_results, format_parser):
    """Create a visualization of the format parsing results."""
    # Filter to only include numeric columns
    numeric_columns = [col for col, result in type_results.items() 
                      if result['type'] == 'number' and col in df.columns]
    
    if not numeric_columns:
        return  # No numeric columns to visualize
    
    # Create a figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 1. Original vs Parsed Values for the first numeric column
    if numeric_columns:
        col = numeric_columns[0]
        # Get original values
        original_values = df[col].dropna().head(10).tolist()
        # Parse values
        parsed_values = [format_parser.parse_amount(val) for val in original_values]
        
        # Create a DataFrame for comparison
        comparison_df = pd.DataFrame({
            'Original': [str(val) for val in original_values],
            'Parsed': parsed_values
        })
        
        # Plot
        comparison_df.plot(kind='bar', ax=ax1)
        ax1.set_title(f'Original vs Parsed Values: {col}')
        ax1.set_ylabel('Value')
        ax1.set_xticklabels([f'Row {i+1}' for i in range(len(original_values))], rotation=45)
    
    # 2. Distribution of parsed values across all numeric columns
    parsed_data = {}
    for col in numeric_columns[:5]:  # Limit to first 5 numeric columns
        values = df[col].dropna().head(50).tolist()
        parsed = [format_parser.parse_amount(val) for val in values]
        # Filter out None values
        parsed = [p for p in parsed if p is not None]
        if parsed:
            parsed_data[col] = parsed
    
    # Create box plot
    if parsed_data:
        sns.boxplot(data=pd.DataFrame(parsed_data), ax=ax2)
        ax2.set_title('Distribution of Parsed Numeric Values')
        ax2.set_ylabel('Value')
        ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # Save the figure
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, f'{file_name}_{sheet_name}_format_parsing.png'))
    plt.close()

if __name__ == "__main__":
    main()