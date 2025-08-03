import unittest
import os
import pandas as pd
import openpyxl
from src.core.excel_processor import ExcelProcessor

class TestExcelProcessor(unittest.TestCase):
    """
    Test cases for the ExcelProcessor class.
    
    These tests verify the functionality of the ExcelProcessor class for handling
    Excel files using both pandas and openpyxl libraries.
    """
    
    def setUp(self):
        """
        Set up test environment before each test method.
        """
        self.processor = ExcelProcessor()
        
        # Define paths to sample files
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.sample_dir = os.path.join(self.base_dir, 'data', 'sample')
        
        self.kh_bank_file = os.path.join(self.sample_dir, 'KH_Bank.XLSX')
        self.customer_ledger_file = os.path.join(self.sample_dir, 'Customer_Ledger_Entries_FULL.xlsx')
        
        # Ensure sample files exist
        self.assertTrue(os.path.exists(self.kh_bank_file), f"Sample file not found: {self.kh_bank_file}")
        self.assertTrue(os.path.exists(self.customer_ledger_file), f"Sample file not found: {self.customer_ledger_file}")
    
    def test_init(self):
        """
        Test initialization of ExcelProcessor.
        """
        self.assertEqual({}, self.processor.files)
        self.assertEqual({}, self.processor.dataframes)
        self.assertEqual({}, self.processor.workbooks)
        self.assertIsNone(self.processor.current_file)
    
    def test_load_file_success(self):
        """
        Test loading a single file successfully.
        """
        result = self.processor.load_file(self.kh_bank_file)
        
        self.assertTrue(result)
        self.assertEqual(self.kh_bank_file, self.processor.current_file)
        self.assertIn(self.kh_bank_file, self.processor.files)
        self.assertIn(self.kh_bank_file, self.processor.dataframes)
        self.assertIn(self.kh_bank_file, self.processor.workbooks)
        
        # Check that file info was stored correctly
        self.assertEqual(self.kh_bank_file, self.processor.files[self.kh_bank_file]['path'])
        self.assertEqual('KH_Bank.XLSX', self.processor.files[self.kh_bank_file]['name'])
        
        # Check that pandas and openpyxl objects were created
        self.assertIsInstance(self.processor.dataframes[self.kh_bank_file], pd.ExcelFile)
        self.assertIsInstance(self.processor.workbooks[self.kh_bank_file], openpyxl.workbook.workbook.Workbook)
    
    def test_load_file_nonexistent(self):
        """
        Test loading a non-existent file.
        """
        non_existent_file = os.path.join(self.sample_dir, 'non_existent.xlsx')
        result = self.processor.load_file(non_existent_file)
        
        self.assertFalse(result)
        self.assertNotIn(non_existent_file, self.processor.files)
    
    def test_load_files(self):
        """
        Test loading multiple files.
        """
        file_paths = [self.kh_bank_file, self.customer_ledger_file]
        results = self.processor.load_files(file_paths)
        
        self.assertEqual(2, len(results))
        self.assertTrue(results[self.kh_bank_file])
        self.assertTrue(results[self.customer_ledger_file])
        
        # Check that both files were loaded
        self.assertIn(self.kh_bank_file, self.processor.files)
        self.assertIn(self.customer_ledger_file, self.processor.files)
        
        # Check that the current file is set to the last loaded file
        self.assertEqual(self.customer_ledger_file, self.processor.current_file)
    
    def test_get_sheet_info(self):
        """
        Test getting sheet information.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Get sheet info
        sheet_info = self.processor.get_sheet_info()
        
        # Check that sheet info is not empty
        self.assertNotEqual({}, sheet_info)
        
        # Check that each sheet has the expected information
        for sheet_name, info in sheet_info.items():
            self.assertIn('rows', info)
            self.assertIn('columns', info)
            self.assertIn('column_names', info)
            self.assertIn('openpyxl_dimensions', info)
            
            # Check that the information is of the correct type
            self.assertIsInstance(info['rows'], int)
            self.assertIsInstance(info['columns'], int)
            self.assertIsInstance(info['column_names'], list)
            self.assertIsInstance(info['openpyxl_dimensions'], str)
    
    def test_get_sheet_info_no_file(self):
        """
        Test getting sheet information when no file is loaded.
        """
        sheet_info = self.processor.get_sheet_info()
        self.assertEqual({}, sheet_info)
    
    def test_extract_data(self):
        """
        Test extracting data from a sheet.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Get sheet names
        sheet_names = self.processor.dataframes[self.kh_bank_file].sheet_names
        
        # Extract data from the first sheet
        df = self.processor.extract_data(sheet_names[0])
        
        # Check that the dataframe is not empty
        self.assertFalse(df.empty)
        
        # Check that the dataframe has the expected structure
        self.assertIsInstance(df, pd.DataFrame)
    
    def test_extract_data_invalid_sheet(self):
        """
        Test extracting data from an invalid sheet.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Extract data from an invalid sheet
        df = self.processor.extract_data('invalid_sheet')
        
        # Check that the dataframe is empty
        self.assertTrue(df.empty)
    
    def test_preview_data(self):
        """
        Test previewing data from a sheet.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Get sheet names
        sheet_names = self.processor.dataframes[self.kh_bank_file].sheet_names
        
        # Preview data from the first sheet with default rows
        df = self.processor.preview_data(sheet_names[0])
        
        # Check that the dataframe is not empty
        self.assertFalse(df.empty)
        
        # Check that the dataframe has at most 5 rows (default)
        self.assertLessEqual(len(df), 5)
        
        # Preview data with custom number of rows
        df = self.processor.preview_data(sheet_names[0], rows=3)
        
        # Check that the dataframe has at most 3 rows
        self.assertLessEqual(len(df), 3)
    
    def test_preview_data_no_sheet(self):
        """
        Test previewing data when no sheet is specified.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Preview data without specifying a sheet
        df = self.processor.preview_data()
        
        # Check that the dataframe is not empty
        self.assertFalse(df.empty)
        
        # Check that the dataframe has at most 5 rows (default)
        self.assertLessEqual(len(df), 5)
    
    def test_get_all_sheets_data(self):
        """
        Test getting data from all sheets.
        """
        # Load a file first
        self.processor.load_file(self.kh_bank_file)
        
        # Get all sheets data
        all_data = self.processor.get_all_sheets_data()
        
        # Check that the result is not empty
        self.assertNotEqual({}, all_data)
        
        # Check that each sheet has data
        for sheet_name, df in all_data.items():
            self.assertIsInstance(df, pd.DataFrame)
    
    def test_get_all_sheets_data_no_file(self):
        """
        Test getting all sheets data when no file is loaded.
        """
        all_data = self.processor.get_all_sheets_data()
        self.assertEqual({}, all_data)

if __name__ == '__main__':
    unittest.main()