#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Complete Workflow Example

This script demonstrates the complete workflow of the Financial Data Parser:
1. Excel Processing - Loading and extracting data from Excel files
2. Data Type Detection - Analyzing and detecting column data types
3. Format Parsing - Parsing various financial formats
4. Data Storage - Storing, indexing, querying, and aggregating data
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import core components
from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import DataTypeDetector
from src.core.format_parser import FormatParser
from src.core.data_storage import DataStorage

# Set up paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = BASE_DIR / 'data' / 'sample'
OUTPUT_DIR = BASE_DIR / 'output'

# Ensure output directory exists
OUTPUT_DIR.mkdir(exist_ok=True)

def main():
    print("\n=== Financial Data Parser: Complete Workflow Example ===")
    
    # Initialize components
    excel_processor = ExcelProcessor()
    type_detector = DataTypeDetector()
    format_parser = FormatParser()
    
    # Initialize different storage types
    memory_storage = DataStorage(storage_type="memory")
    file_storage = DataStorage(storage_type="file", db_path=str(OUTPUT_DIR))
    sqlite_storage = DataStorage(storage_type="sqlite", db_path=str(OUTPUT_DIR / "financial_data.db"))
    
    # Track processed files to avoid duplicates
    processed_files = set()
    
    # Process each Excel file in the data directory
    for file_path in DATA_DIR.glob("*.xls*"):
        if str(file_path) in processed_files:
            continue
        processed_files.add(str(file_path))
        
        print(f"\n=== Processing {file_path.name} ===")
        
        # Phase 1: Excel Processing
        try:
            excel_processor.load_file(str(file_path))
            sheet_info = excel_processor.get_sheet_info()
            
            print(f"File contains {len(sheet_info)} sheets:")
            for sheet_name, info in sheet_info.items():
                print(f"  - {sheet_name}: {info['rows']} rows, {info['columns']} columns")
            
            # Process each sheet in the file
            for sheet_name, info in sheet_info.items():
                print(f"\n--- Processing sheet: {sheet_name} ---")
                
                # Extract data from the sheet
                df = excel_processor.extract_data(sheet_name)
                if df.empty:
                    print(f"  Sheet {sheet_name} is empty or could not be processed")
                    continue
                
                # Create a unique dataset name
                dataset_name = f"{file_path.stem}_{sheet_name}"
                
                # Phase 2: Data Type Detection
                print("\nDetecting column data types...")
                column_types = type_detector.analyze_dataframe(df)
                
                # Display detected types for a few columns
                sample_columns = list(column_types.keys())[:5]  # Show first 5 columns
                for column in sample_columns:
                    result = column_types[column]
                    print(f"  - {column}: {result['type']} (confidence: {result['confidence']:.2f}, format: {result['format']})")
                
                # Visualize type detection results
                # Create a simple visualization of the detected types
                plt.figure(figsize=(12, 6))
                
                # Count types
                type_counts = {}
                for col, result in column_types.items():
                    col_type = result['type']
                    if col_type not in type_counts:
                        type_counts[col_type] = 0
                    type_counts[col_type] += 1
                
                # Plot
                plt.bar(type_counts.keys(), type_counts.values())
                plt.title(f"Data Types in {sheet_name}")
                plt.xlabel("Data Type")
                plt.ylabel("Count")
                plt.savefig(str(OUTPUT_DIR / f"{file_path.name}_{sheet_name}_type_detection.png"))
                plt.close()
                
                # Phase 3: Format Parsing
                print("\nParsing formats...")
                parsed_data = df.copy()
                
                # Parse dates and amounts based on detected types
                for column, result in column_types.items():
                    if result['type'] == 'date' and column in df.columns:
                        # Parse dates
                        parsed_data[f"{column}_parsed"] = df[column].apply(
                            lambda x: format_parser.parse_date(str(x)) if pd.notna(x) else None
                        )
                        print(f"  - Parsed dates in column: {column}")
                    
                    elif result['type'] == 'number' and column in df.columns:
                        # Parse amounts
                        parsed_data[f"{column}_parsed"] = df[column].apply(
                            lambda x: format_parser.parse_amount(str(x)) if pd.notna(x) else None
                        )
                        print(f"  - Parsed amounts in column: {column}")
                
                # Visualize format parsing results for a sample column
                # Create a simple visualization of the parsed formats
                plt.figure(figsize=(12, 6))
                
                # Count parsed columns by type
                parsed_counts = {'date': 0, 'number': 0}
                for column, result in column_types.items():
                    if result['type'] in ['date', 'number'] and column in df.columns:
                        parsed_counts[result['type']] += 1
                
                # Plot
                plt.bar(parsed_counts.keys(), parsed_counts.values())
                plt.title(f"Parsed Formats in {sheet_name}")
                plt.xlabel("Data Type")
                plt.ylabel("Count of Parsed Columns")
                plt.savefig(str(OUTPUT_DIR / f"{file_path.name}_{sheet_name}_format_parsing.png"))
                plt.close()
                
                # Phase 4: Data Storage
                print("\nStoring data...")
                
                # Store in different storage types
                memory_storage.store_data(dataset_name, parsed_data, column_types)
                file_storage.store_data(dataset_name, parsed_data, column_types)
                sqlite_storage.store_data(dataset_name, parsed_data, column_types)
                
                print("  - Stored in memory storage")
                print("  - Stored in file storage")
                print("  - Stored in SQLite storage")
                
                # Create indexes for faster querying
                # Find numeric and date columns to index
                numeric_columns = [col for col, res in column_types.items() 
                                 if res['type'] == 'number' and col in df.columns][:2]  # Index first 2 numeric columns
                date_columns = [col for col, res in column_types.items() 
                              if res['type'] == 'date' and col in df.columns][:1]  # Index first date column
                
                # Create indexes
                columns_to_index = numeric_columns + date_columns
                if columns_to_index:
                    memory_storage.create_indexes(dataset_name, columns_to_index)
                    sqlite_storage.create_indexes(dataset_name, columns_to_index)
                    print("  - Created indexes for faster querying")
                
                # Demonstrate querying and aggregation
                if len(numeric_columns) > 0 and len(df) > 10:
                    print(f"\n=== Query Examples for {dataset_name} ===")
                    
                    # Example 1: Simple numeric filter
                    numeric_col = numeric_columns[0]
                    threshold = df[numeric_col].median()  # Use median as threshold
                    
                    results = memory_storage.query_data(
                        dataset_name,
                        filters={numeric_col: {"gt": threshold}}
                    )
                    print(f"  - Records with {numeric_col} > {threshold}: {len(results)}")
                    
                    # Example 2: String equality filter if string columns exist
                    string_columns = [col for col, res in column_types.items() 
                                    if res['type'] == 'string' and col in df.columns]
                    
                    if string_columns:
                        string_col = string_columns[0]
                        # Find a common value in the string column
                        common_values = df[string_col].value_counts().head(1).index.tolist()
                        
                        if common_values:
                            common_value = common_values[0]
                            results = memory_storage.query_data(
                                dataset_name,
                                filters={string_col: common_value}
                            )
                            print(f"  - Records with {string_col} = '{common_value}': {len(results)}")
                            
                            # Example 3: Combined filter
                            results = memory_storage.query_data(
                                dataset_name,
                                filters={
                                    numeric_col: {"gt": threshold},
                                    string_col: common_value
                                }
                            )
                            print(f"  - Records with {numeric_col} > {threshold} AND {string_col} = '{common_value}': {len(results)}")
                    
                    # Example 4: Aggregation
                    print(f"\n=== Aggregation Examples for {dataset_name} ===")
                    
                    # Group by a string column if available, otherwise use first column
                    group_by_col = string_columns[0] if string_columns else df.columns[0]
                    
                    aggregated = memory_storage.aggregate_data(
                        dataset_name,
                        group_by=[group_by_col],
                        measures={numeric_col: ["sum"]}
                    )
                    
                    print(f"  - Aggregation by {group_by_col}, sum of {numeric_col}:")
                    print(aggregated)
                    
                    # Visualize aggregation results
                    plt.figure(figsize=(10, 6))
                    aggregated.plot(kind='bar', x=group_by_col, y=f"{numeric_col}_sum")
                    plt.title(f"Sum of {numeric_col} by {group_by_col}")
                    plt.tight_layout()
                    plt.savefig(str(OUTPUT_DIR / f"{dataset_name}_aggregation.png"))
                    plt.close()
                    
                    print(f"  - Visualization saved to {dataset_name}_aggregation.png")
        
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
    
    # Clean up resources
    sqlite_storage.close()
    
    print("\n=== Complete Workflow Processing Complete ===")

if __name__ == "__main__":
    main()