import pandas as pd
import openpyxl
import os
from typing import Dict, List, Tuple, Union, Any

class ExcelProcessor:
    """
    A class for processing Excel files with support for multiple libraries.
    
    This class provides functionality to load Excel files, extract data,
    get sheet information, and preview data using both pandas and openpyxl.
    """
    
    def __init__(self):
        """
        Initialize the ExcelProcessor with empty data structures.
        """
        self.files = {}
        self.dataframes = {}
        self.workbooks = {}
        self.current_file = None
    
    def load_file(self, file_path: str) -> bool:
        """
        Load a single Excel file using both pandas and openpyxl.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            bool: True if file was loaded successfully, False otherwise
        """
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist.")
            return False
        
        try:
            # Load with pandas
            self.dataframes[file_path] = pd.ExcelFile(file_path)
            
            # Load with openpyxl
            self.workbooks[file_path] = openpyxl.load_workbook(file_path, data_only=True)
            
            # Store file path
            self.files[file_path] = {
                'path': file_path,
                'name': os.path.basename(file_path)
            }
            
            # Set as current file
            self.current_file = file_path
            
            return True
        except Exception as e:
            print(f"Error loading file {file_path}: {str(e)}")
            return False
    
    def load_files(self, file_paths: List[str]) -> Dict[str, bool]:
        """
        Load multiple Excel files.
        
        Args:
            file_paths: List of paths to Excel files
            
        Returns:
            Dict[str, bool]: Dictionary with file paths as keys and load status as values
        """
        results = {}
        for file_path in file_paths:
            results[file_path] = self.load_file(file_path)
        return results
    
    def get_sheet_info(self, file_path: str = None) -> Dict[str, Any]:
        """
        Get information about sheets in the Excel file.
        
        Args:
            file_path: Path to the Excel file (uses current file if None)
            
        Returns:
            Dict: Dictionary containing sheet information
        """
        file_path = file_path or self.current_file
        if not file_path or file_path not in self.files:
            print("No file loaded or specified file not found.")
            return {}
        
        sheet_info = {}
        
        # Get sheet names from pandas
        pd_sheets = self.dataframes[file_path].sheet_names
        
        # Get sheet names from openpyxl
        openpyxl_sheets = self.workbooks[file_path].sheetnames
        
        # Combine information
        for sheet_name in pd_sheets:
            # Get pandas dataframe for dimensions
            df = self.dataframes[file_path].parse(sheet_name)
            
            # Get openpyxl worksheet
            ws = self.workbooks[file_path][sheet_name]
            
            sheet_info[sheet_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'openpyxl_dimensions': ws.dimensions
            }
        
        return sheet_info
    
    def extract_data(self, sheet_name: str, file_path: str = None) -> pd.DataFrame:
        """
        Extract data from a specific sheet as a pandas DataFrame.
        
        Args:
            sheet_name: Name of the sheet to extract data from
            file_path: Path to the Excel file (uses current file if None)
            
        Returns:
            pd.DataFrame: DataFrame containing the sheet data
        """
        file_path = file_path or self.current_file
        if not file_path or file_path not in self.files:
            print("No file loaded or specified file not found.")
            return pd.DataFrame()
        
        try:
            return self.dataframes[file_path].parse(sheet_name)
        except Exception as e:
            print(f"Error extracting data from sheet {sheet_name}: {str(e)}")
            return pd.DataFrame()
    
    def preview_data(self, sheet_name: str = None, rows: int = 5, file_path: str = None) -> pd.DataFrame:
        """
        Preview data from a sheet with a specified number of rows.
        
        Args:
            sheet_name: Name of the sheet to preview (uses first sheet if None)
            rows: Number of rows to preview
            file_path: Path to the Excel file (uses current file if None)
            
        Returns:
            pd.DataFrame: DataFrame containing the preview data
        """
        file_path = file_path or self.current_file
        if not file_path or file_path not in self.files:
            print("No file loaded or specified file not found.")
            return pd.DataFrame()
        
        # If sheet_name is not provided, use the first sheet
        if sheet_name is None:
            sheet_name = self.dataframes[file_path].sheet_names[0]
        
        try:
            df = self.extract_data(sheet_name, file_path)
            return df.head(rows)
        except Exception as e:
            print(f"Error previewing data: {str(e)}")
            return pd.DataFrame()
    
    def get_all_sheets_data(self, file_path: str = None) -> Dict[str, pd.DataFrame]:
        """
        Extract data from all sheets in the Excel file.
        
        Args:
            file_path: Path to the Excel file (uses current file if None)
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with sheet names as keys and DataFrames as values
        """
        file_path = file_path or self.current_file
        if not file_path or file_path not in self.files:
            print("No file loaded or specified file not found.")
            return {}
        
        all_data = {}
        for sheet_name in self.dataframes[file_path].sheet_names:
            all_data[sheet_name] = self.extract_data(sheet_name, file_path)
        
        return all_data