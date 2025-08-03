import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Add the src directory to the path so we can import the modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import DataTypeDetector

def main():
    # Initialize the ExcelProcessor and DataTypeDetector
    excel_processor = ExcelProcessor()
    type_detector = DataTypeDetector()
    
    # Load the sample Excel files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'sample')
    kh_bank_path = os.path.join(data_dir, 'KH_Bank.XLSX')
    customer_ledger_path = os.path.join(data_dir, 'Customer_Ledger_Entries_FULL.xlsx')
    
    # Load the files
    excel_processor.load_file(kh_bank_path)
    excel_processor.load_file(customer_ledger_path)
    
    # Get sheet information
    print("\n=== Available Sheets ===")
    for file_path in excel_processor.files.keys():
        sheet_info = excel_processor.get_sheet_info(file_path)
        print(f"\nFile: {os.path.basename(file_path)}")
        for sheet_name, info in sheet_info.items():
            print(f"  - {sheet_name}: {info['rows']} rows, {info['columns']} columns")
    
    # Process each sheet and detect column types
    print("\n=== Column Type Detection Results ===")
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
            
            # Analyze the DataFrame
            results = type_detector.analyze_dataframe(df)
            
            # Display results
            print("    Column Type Detection:")
            for column, result in results.items():
                print(f"      - {column}: {result['type']} (confidence: {result['confidence']:.2f}, format: {result['format']})")
            
            # Visualize the results
            visualize_results(file_name, sheet_name, results)

def visualize_results(file_name, sheet_name, results):
    """Create a visualization of the type detection results."""
    # Extract data for visualization
    columns = list(results.keys())
    types = [result['type'] for result in results.values()]
    confidences = [result['confidence'] for result in results.values()]
    formats = [result['format'] for result in results.values()]
    
    # Create a DataFrame for easier plotting
    viz_df = pd.DataFrame({
        'Column': columns,
        'Type': types,
        'Confidence': confidences,
        'Format': formats
    })
    
    # Set up the figure
    plt.figure(figsize=(12, 6))
    
    # Create a bar plot of confidence scores colored by type
    ax = sns.barplot(x='Column', y='Confidence', hue='Type', data=viz_df)
    
    # Customize the plot
    plt.title(f'Data Type Detection Results - {file_name} - {sheet_name}')
    plt.xticks(rotation=45, ha='right')
    plt.ylim(0, 1.0)
    plt.tight_layout()
    
    # Save the figure
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, f'{file_name}_{sheet_name}_type_detection.png'))
    plt.close()

if __name__ == "__main__":
    main()