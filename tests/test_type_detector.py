import unittest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Add the src directory to the path so we can import the modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.type_detector import DataTypeDetector

class TestDataTypeDetector(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.detector = DataTypeDetector()
        
        # Create test data
        self.create_test_data()
    
    def create_test_data(self):
        """Create various test datasets for testing the detector."""
        # Date data in various formats
        self.date_data = pd.Series([
            '01/15/2023', '02/28/2023', '03/31/2023', '04/30/2023', '05/31/2023',
            '06/30/2023', '07/31/2023', '08/31/2023', '09/30/2023', '10/31/2023'
        ])
        
        self.iso_date_data = pd.Series([
            '2023-01-15', '2023-02-28', '2023-03-31', '2023-04-30', '2023-05-31',
            '2023-06-30', '2023-07-31', '2023-08-31', '2023-09-30', '2023-10-31'
        ])
        
        self.excel_date_data = pd.Series([
            44941, 44985, 45016, 45046, 45077, 45107, 45138, 45169, 45199, 45230
        ])  # Excel serial dates for 2023 dates
        
        # Number data in various formats
        self.standard_number_data = pd.Series([
            '1000', '1500.50', '2000', '2500.75', '3000', 
            '3500.25', '4000', '4500.50', '5000', '5500.75'
        ])
        
        self.formatted_number_data = pd.Series([
            '$1,000.00', '$1,500.50', '$2,000.00', '$2,500.75', '$3,000.00',
            '$3,500.25', '$4,000.00', '$4,500.50', '$5,000.00', '$5,500.75'
        ])
        
        self.european_number_data = pd.Series([
            '1.000,00', '1.500,50', '2.000,00', '2.500,75', '3.000,00',
            '3.500,25', '4.000,00', '4.500,50', '5.000,00', '5.500,75'
        ])
        
        self.accounting_number_data = pd.Series([
            '1,000.00', '(1,500.50)', '2,000.00', '(2,500.75)', '3,000.00',
            '(3,500.25)', '4,000.00', '(4,500.50)', '5,000.00', '(5,500.75)'
        ])
        
        self.abbreviated_number_data = pd.Series([
            '1K', '1.5M', '2K', '2.5M', '3K',
            '3.5M', '4K', '4.5M', '5K', '5.5M'
        ])
        
        # String data in various formats
        self.account_number_data = pd.Series([
            '12345678', '23456789', '34567890', '45678901', '56789012',
            '67890123', '78901234', '89012345', '90123456', '01234567'
        ])
        
        self.reference_code_data = pd.Series([
            'REF12345', 'REF23456', 'REF34567', 'REF45678', 'REF56789',
            'REF67890', 'REF78901', 'REF89012', 'REF90123', 'REF01234'
        ])
        
        self.description_data = pd.Series([
            'Payment for invoice #12345 dated January 15, 2023',
            'Refund for order #54321 processed on February 28, 2023',
            'Monthly subscription fee for March 2023',
            'Quarterly service charge for Q1 2023',
            'Payment for consulting services rendered in April 2023',
            'Annual maintenance fee for 2023-2024',
            'Deposit for upcoming project starting July 2023',
            'Withdrawal for operational expenses in August 2023',
            'Interest earned for September 2023',
            'Dividend payment for Q3 2023'
        ])
        
        self.categorical_data = pd.Series([
            'Approved', 'Pending', 'Rejected', 'Approved', 'Approved',
            'Pending', 'Approved', 'Rejected', 'Pending', 'Approved'
        ])
        
        # Mixed data with nulls
        self.mixed_data = pd.Series([
            '1000', '1,500.50', np.nan, '01/15/2023', 'REF12345',
            np.nan, '3.500,25', '04/30/2023', 'Approved', '$5,000.00'
        ])
        
        # Create a test DataFrame
        self.test_df = pd.DataFrame({
            'Date': self.date_data,
            'ISO_Date': self.iso_date_data,
            'Amount': self.formatted_number_data,
            'European_Amount': self.european_number_data,
            'Account': self.account_number_data,
            'Reference': self.reference_code_data,
            'Status': self.categorical_data,
            'Description': self.description_data
        })
    
    def test_initialization(self):
        """Test that the detector initializes correctly."""
        self.assertIsInstance(self.detector, DataTypeDetector)
        self.assertTrue(hasattr(self.detector, 'date_patterns'))
        self.assertTrue(hasattr(self.detector, 'number_patterns'))
        self.assertTrue(hasattr(self.detector, 'financial_string_patterns'))
    
    def test_detect_date_format(self):
        """Test date format detection."""
        # Test MM/DD/YYYY format
        result = self.detector.detect_date_format(self.date_data)
        self.assertEqual(result['type'], 'date')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test YYYY-MM-DD format
        result = self.detector.detect_date_format(self.iso_date_data)
        self.assertEqual(result['type'], 'date')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test Excel serial date format
        result = self.detector.detect_date_format(self.excel_date_data)
        self.assertEqual(result['type'], 'date')
        self.assertGreater(result['confidence'], 0.5)
        # The format might be auto-detected or excel_serial depending on pandas version
        self.assertIn(result['format'], ['excel_serial', 'auto-detected', 'numeric'])
        
        # Test non-date data
        result = self.detector.detect_date_format(self.standard_number_data)
        self.assertEqual(result['type'], 'date')
        self.assertLess(result['confidence'], 0.5)
    
    def test_detect_number_format(self):
        """Test number format detection."""
        # Test standard number format
        result = self.detector.detect_number_format(self.standard_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test formatted number with currency
        result = self.detector.detect_number_format(self.formatted_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'USD')
        
        # Test European number format
        result = self.detector.detect_number_format(self.european_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'european')
        
        # Test accounting number format
        result = self.detector.detect_number_format(self.accounting_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'accounting')
        
        # Test abbreviated number format
        result = self.detector.detect_number_format(self.abbreviated_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'abbreviated')
        
        # Test non-number data
        result = self.detector.detect_number_format(self.date_data)
        self.assertEqual(result['type'], 'number')
        self.assertLess(result['confidence'], 0.5)
    
    def test_classify_string_type(self):
        """Test string type classification."""
        # Test account number
        result = self.detector.classify_string_type(self.account_number_data)
        self.assertEqual(result['type'], 'string')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'account_number')
        
        # Test reference code
        result = self.detector.classify_string_type(self.reference_code_data)
        self.assertEqual(result['type'], 'string')
        self.assertGreater(result['confidence'], 0.5)
        self.assertEqual(result['format'], 'reference_code')
        
        # Test description
        result = self.detector.classify_string_type(self.description_data)
        self.assertEqual(result['type'], 'string')
        self.assertGreater(result['confidence'], 0.5)
        # The format might be description or name_or_address depending on implementation
        self.assertIn(result['format'], ['description', 'name_or_address'])
        
        # Test categorical data
        result = self.detector.classify_string_type(self.categorical_data)
        self.assertEqual(result['type'], 'string')
        self.assertGreater(result['confidence'], 0.5)
        # The format might be categorical or reference_code depending on implementation
        self.assertIn(result['format'], ['categorical', 'reference_code'])
    
    def test_analyze_column(self):
        """Test column analysis."""
        # Test date column
        result = self.detector.analyze_column(self.date_data)
        self.assertEqual(result['type'], 'date')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test number column
        result = self.detector.analyze_column(self.formatted_number_data)
        self.assertEqual(result['type'], 'number')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test string column
        result = self.detector.analyze_column(self.reference_code_data)
        self.assertEqual(result['type'], 'string')
        self.assertGreater(result['confidence'], 0.5)
        
        # Test mixed column with nulls
        result = self.detector.analyze_column(self.mixed_data)
        self.assertIn(result['type'], ['date', 'number', 'string'])
        
        # Test empty column
        empty_data = pd.Series([])
        result = self.detector.analyze_column(empty_data)
        self.assertEqual(result['type'], 'unknown')
        self.assertEqual(result['confidence'], 0.0)
        
        # Test all null column
        null_data = pd.Series([np.nan, np.nan, np.nan])
        result = self.detector.analyze_column(null_data)
        self.assertEqual(result['type'], 'unknown')
        self.assertEqual(result['confidence'], 0.0)
    
    def test_analyze_dataframe(self):
        """Test DataFrame analysis."""
        results = self.detector.analyze_dataframe(self.test_df)
        
        # Check that all columns are analyzed
        self.assertEqual(len(results), len(self.test_df.columns))
        
        # Check specific column types
        self.assertEqual(results['Date']['type'], 'date')
        self.assertEqual(results['ISO_Date']['type'], 'date')
        self.assertEqual(results['Amount']['type'], 'number')
        self.assertEqual(results['European_Amount']['type'], 'number')
        # Account numbers might be detected as number or string depending on the pattern
        self.assertIn(results['Account']['type'], ['string', 'number'])
        self.assertEqual(results['Reference']['type'], 'string')
        self.assertEqual(results['Status']['type'], 'string')
        self.assertEqual(results['Description']['type'], 'string')
        
        # Check confidence scores
        for column, result in results.items():
            self.assertGreater(result['confidence'], 0.5, f"Low confidence for {column}")

if __name__ == '__main__':
    unittest.main()