import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))
from src.core.data_storage import DataStorage
from src.core.type_detector import DataTypeDetector
from src.core.format_parser import FormatParser


def main():
    # Set up paths
    current_dir = Path(__file__).parent.parent
    data_dir = current_dir / 'data' / 'sample'
    output_dir = current_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    # Initialize our components
    type_detector = DataTypeDetector()
    format_parser = FormatParser()
    
    # Initialize different storage types
    memory_storage = DataStorage(storage_type='memory')
    file_storage = DataStorage(storage_type='file')
    sqlite_storage = DataStorage(storage_type='sqlite', db_path=str(output_dir / 'financial_data.db'))
    
    # Find Excel files (avoid duplicates by normalizing paths)
    excel_files = set()
    for ext in ['*.xlsx', '*.XLSX']:
        for file_path in data_dir.glob(ext):
            excel_files.add(file_path)
    excel_files = list(excel_files)  # Convert back to list
    
    if not excel_files:
        print("No Excel files found in the data directory.")
        return
    
    print("\n=== Processing Financial Data Files ===\n")
    
    # Process each Excel file
    for file_path in excel_files:
        print(f"Processing file: {file_path.name}")
        
        # Load Excel file
        try:
            excel_data = pd.ExcelFile(file_path)
            for sheet_name in excel_data.sheet_names:
                print(f"  Sheet: {sheet_name}")
                
                # Extract data
                df = pd.read_excel(excel_data, sheet_name)
                if df.empty:
                    print(f"  - Empty sheet, skipping")
                    continue
                
                print(f"  - Rows: {len(df)}, Columns: {len(df.columns)}")
                
                # Detect column types
                column_types = {}
                for column in df.columns:
                    # Use the analyze_column method directly with the Series
                    # The DataTypeDetector.analyze_column method expects a pandas Series
                    detection_result = type_detector.analyze_column(df[column])
                    column_types[column] = detection_result
                
                # Store data in different storage systems
                dataset_name = f"{file_path.stem}_{sheet_name}"
                
                # Store in memory
                memory_storage.store_data(dataset_name, df, column_types)
                print(f"  - Stored in memory storage")
                
                # Store in file system
                file_storage.store_data(dataset_name, df, column_types)
                print(f"  - Stored in file storage")
                
                # Store in SQLite
                sqlite_storage.store_data(dataset_name, df, column_types)
                print(f"  - Stored in SQLite storage")
                
                # Create indexes for faster querying
                memory_storage.create_indexes(dataset_name, df.columns.tolist()[:3])  # Index first 3 columns
                print(f"  - Created indexes for faster querying")
                
                # Demonstrate querying
                demonstrate_querying(memory_storage, dataset_name, df)
                
                # Demonstrate aggregation
                demonstrate_aggregation(memory_storage, dataset_name, df)
                
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
    
    print("\n=== Storage Processing Complete ===\n")


def demonstrate_querying(storage, dataset_name, df):
    """Demonstrate various query operations on the dataset."""
    print(f"\n  === Query Examples for {dataset_name} ===")
    
    # Find numeric columns for filtering examples
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    if numeric_columns:
        numeric_col = numeric_columns[0]
        # Get the median value for filtering
        median_value = df[numeric_col].median()
        
        # Query for values greater than median
        query_result = storage.query_by_criteria(dataset_name, {f"{numeric_col}__gt": median_value})
        print(f"  - Records with {numeric_col} > {median_value:.2f}: {len(query_result)}")
    
    # Find categorical columns for filtering examples
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
    if categorical_columns:
        cat_col = categorical_columns[0]
        # Get the most common value
        most_common = df[cat_col].value_counts().index[0] if not df[cat_col].value_counts().empty else None
        
        if most_common is not None:
            # Query for records matching the most common value
            query_result = storage.query_by_criteria(dataset_name, {cat_col: most_common})
            print(f"  - Records with {cat_col} = '{most_common}': {len(query_result)}")
    
    # Demonstrate multiple criteria
    if numeric_columns and categorical_columns:
        numeric_col = numeric_columns[0]
        cat_col = categorical_columns[0]
        
        # Combined query
        query_result = storage.query_by_criteria(dataset_name, {
            f"{numeric_col}__gt": median_value,
            cat_col: most_common
        })
        print(f"  - Records with {numeric_col} > {median_value:.2f} AND {cat_col} = '{most_common}': {len(query_result)}")


def demonstrate_aggregation(storage, dataset_name, df):
    """Demonstrate various aggregation operations on the dataset."""
    print(f"\n  === Aggregation Examples for {dataset_name} ===")
    
    # Find numeric and categorical columns for aggregation examples
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
    
    if numeric_columns and categorical_columns:
        numeric_col = numeric_columns[0]
        cat_col = categorical_columns[0]
        
        # Aggregate by categorical column, sum numeric column
        agg_result = storage.aggregate_data(
            dataset_name,
            group_by=[cat_col],
            measures={numeric_col: 'sum'}
        )
        
        print(f"  - Aggregation by {cat_col}, sum of {numeric_col}:")
        if not agg_result.empty:
            # Display top 5 results
            print(agg_result.sort_values(by=numeric_col, ascending=False).head(5).to_string())
            
            # Create a visualization of the aggregation
            plt.figure(figsize=(10, 6))
            sns.barplot(x=cat_col, y=numeric_col, data=agg_result.head(10))
            plt.title(f"Sum of {numeric_col} by {cat_col}")
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Save the visualization
            output_path = Path(__file__).parent.parent / 'output' / f"{dataset_name}_aggregation.png"
            plt.savefig(output_path)
            plt.close()
            print(f"  - Visualization saved to {output_path.name}")
        else:
            print("  - No aggregation results available")


if __name__ == "__main__":
    main()