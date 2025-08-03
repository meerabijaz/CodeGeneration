import unittest
import pandas as pd
import numpy as np
import os
import json
import sqlite3
import tempfile
import shutil
from datetime import datetime
from src.core.data_storage import DataStorage

class TestDataStorage(unittest.TestCase):
    """
    Test cases for the DataStorage class.
    """
    
    def setUp(self):
        """
        Set up test fixtures.
        """
        # Create a sample DataFrame
        self.df = pd.DataFrame({
            'Date': pd.date_range(start='2023-01-01', periods=10),
            'Amount': [100.50, 200.75, -150.25, 300.00, -50.50, 1000.00, 2000.00, -500.00, 750.50, 1250.75],
            'Category': ['Income', 'Income', 'Expense', 'Income', 'Expense', 'Income', 'Income', 'Expense', 'Income', 'Income'],
            'Description': ['Salary', 'Bonus', 'Rent', 'Freelance', 'Utilities', 'Investment', 'Sale', 'Purchase', 'Dividend', 'Contract'],
            'Account': ['A001', 'A001', 'A002', 'A001', 'A002', 'A003', 'A003', 'A002', 'A003', 'A001']
        })
        
        # Create column types dictionary
        self.column_types = {
            'Date': {'type': 'date', 'format': 'iso', 'confidence': 0.95},
            'Amount': {'type': 'number', 'format': 'standard', 'confidence': 0.98},
            'Category': {'type': 'string', 'format': 'categorical', 'confidence': 0.90},
            'Description': {'type': 'string', 'format': 'text', 'confidence': 0.85},
            'Account': {'type': 'string', 'format': 'identifier', 'confidence': 0.92}
        }
        
        # Create temporary directory for file-based storage tests
        self.temp_dir = tempfile.mkdtemp()
        os.chdir(self.temp_dir)  # Change to temp directory for file operations
        os.makedirs('data/processed', exist_ok=True)
        
        # Create a temporary SQLite database file
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        
        # Initialize storage instances
        self.memory_storage = DataStorage(storage_type='memory')
        self.sqlite_storage = DataStorage(storage_type='sqlite', db_path=self.db_path)
        self.file_storage = DataStorage(storage_type='file')
    
    def tearDown(self):
        """
        Clean up test fixtures.
        """
        # Close SQLite connection
        if hasattr(self, 'sqlite_storage') and self.sqlite_storage.conn:
            self.sqlite_storage.close()
        
        # Remove temporary directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except PermissionError as e:
                print(f"Warning: Could not remove temporary directory due to permission error: {e}")
                # On Windows, files might still be in use by other processes
                # This is not a critical error for the test itself
    
    def test_memory_storage(self):
        """
        Test in-memory storage functionality.
        """
        # Store data
        result = self.memory_storage.store_data('test_data', self.df, self.column_types)
        self.assertTrue(result)
        
        # Check if data was stored correctly
        self.assertIn('test_data', self.memory_storage.data)
        self.assertIn('test_data', self.memory_storage.metadata)
        self.assertIn('test_data', self.memory_storage.indexes)
        
        # Check metadata
        metadata = self.memory_storage.get_metadata('test_data')
        self.assertEqual(metadata['row_count'], 10)
        self.assertEqual(metadata['column_count'], 5)
        self.assertEqual(metadata['column_types'], self.column_types)
    
    def test_sqlite_storage(self):
        """
        Test SQLite storage functionality.
        """
        # Store data
        result = self.sqlite_storage.store_data('test_data', self.df, self.column_types)
        self.assertTrue(result)
        
        # Check if data was stored correctly by querying it
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_data'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check row count
        cursor.execute("SELECT COUNT(*) FROM test_data")
        self.assertEqual(cursor.fetchone()[0], 10)
        
        # Check metadata
        cursor.execute("SELECT * FROM metadata WHERE dataset_name='test_data'")
        metadata_row = cursor.fetchone()
        self.assertIsNotNone(metadata_row)
        self.assertEqual(metadata_row[3], 10)  # row_count
        self.assertEqual(metadata_row[4], 5)   # column_count
        
        conn.close()
    
    def test_file_storage(self):
        """
        Test file-based storage functionality.
        """
        # Store data
        result = self.file_storage.store_data('test_data', self.df, self.column_types)
        self.assertTrue(result)
        
        # Check if files were created
        csv_path = 'data/processed/test_data.csv'
        json_path = 'data/processed/test_data_metadata.json'
        
        self.assertTrue(os.path.exists(csv_path))
        self.assertTrue(os.path.exists(json_path))
        
        # Check CSV content
        df_loaded = pd.read_csv(csv_path)
        self.assertEqual(len(df_loaded), 10)
        self.assertEqual(len(df_loaded.columns), 5)
        
        # Check metadata JSON
        with open(json_path, 'r') as f:
            metadata = json.load(f)
        
        self.assertEqual(metadata['row_count'], 10)
        self.assertEqual(metadata['column_count'], 5)
        self.assertEqual(metadata['column_types'], self.column_types)
    
    def test_create_indexes(self):
        """
        Test index creation functionality.
        """
        # Store data first
        self.memory_storage.store_data('test_data', self.df, self.column_types)
        
        # Create indexes
        result = self.memory_storage.create_indexes('test_data', ['Date', 'Amount', 'Category'])
        self.assertTrue(result)
        
        # Check if indexes were created
        self.assertIn('Date', self.memory_storage.indexes['test_data']['date_index'])
        self.assertIn('Amount', self.memory_storage.indexes['test_data']['amount_index'])
        self.assertIn('Category', self.memory_storage.indexes['test_data']['category_index'])
    
    def test_query_by_criteria(self):
        """
        Test querying functionality.
        """
        # Store data first
        self.memory_storage.store_data('test_data', self.df, self.column_types)
        
        # Test simple equality filter
        result = self.memory_storage.query_by_criteria('test_data', {'Category': 'Income'})
        self.assertEqual(len(result), 7)  # 7 income records
        
        # Test greater than filter
        result = self.memory_storage.query_by_criteria('test_data', {'Amount__gt': 500})
        self.assertEqual(len(result), 4)  # 4 records with Amount > 500
        
        # Test less than filter
        result = self.memory_storage.query_by_criteria('test_data', {'Amount__lt': 0})
        self.assertEqual(len(result), 3)  # 3 negative amounts
        
        # Test between filter
        result = self.memory_storage.query_by_criteria('test_data', {'Amount__between': (100, 300)})
        self.assertEqual(len(result), 3)  # 3 records with 100 <= Amount <= 300
        
        # Test in filter
        result = self.memory_storage.query_by_criteria('test_data', {'Account__in': ['A001', 'A003']})
        self.assertEqual(len(result), 7)  # 7 records with Account in A001 or A003
        
        # Test contains filter
        result = self.memory_storage.query_by_criteria('test_data', {'Description__contains': 'a'})
        self.assertTrue(len(result) > 0)  # At least one description contains 'a'
        
        # Test multiple filters
        result = self.memory_storage.query_by_criteria('test_data', {
            'Category': 'Income',
            'Amount__gt': 500
        })
        self.assertEqual(len(result), 4)  # 4 income records with Amount > 500
    
    def test_aggregate_data(self):
        """
        Test data aggregation functionality.
        """
        # Store data first
        self.memory_storage.store_data('test_data', self.df, self.column_types)
        
        # Test groupby with sum aggregation
        result = self.memory_storage.aggregate_data(
            'test_data',
            group_by=['Category'],
            measures={'Amount': 'sum'}
        )
        
        self.assertEqual(len(result), 2)  # 2 categories: Income and Expense
        
        # Find the Income row and check sum
        income_row = result[result['Category'] == 'Income']
        self.assertAlmostEqual(income_row['Amount'].iloc[0], 5602.5)  # Sum of Income amounts
        
        # Test groupby with multiple aggregations
        result = self.memory_storage.aggregate_data(
            'test_data',
            group_by=['Account'],
            measures={'Amount': 'sum'}
        )
        
        self.assertEqual(len(result), 3)  # 3 accounts: A001, A002, A003
        
        # Test groupby with multiple columns
        result = self.memory_storage.aggregate_data(
            'test_data',
            group_by=['Category', 'Account'],
            measures={'Amount': 'sum'}
        )
        
        # There should be exactly 3 combinations of Category and Account in our test data:
        # (Expense, A002), (Income, A001), (Income, A003)
        self.assertEqual(len(result), 3)  # 3 combinations of Category and Account
    
    def test_nonexistent_dataset(self):
        """
        Test behavior with nonexistent dataset.
        """
        # Query nonexistent dataset
        result = self.memory_storage.query_by_criteria('nonexistent', {'Category': 'Income'})
        self.assertTrue(result.empty)
        
        # Aggregate nonexistent dataset
        result = self.memory_storage.aggregate_data(
            'nonexistent',
            group_by=['Category'],
            measures={'Amount': 'sum'}
        )
        self.assertTrue(result.empty)
        
        # Get metadata for nonexistent dataset
        metadata = self.memory_storage.get_metadata('nonexistent')
        self.assertEqual(metadata, {})

if __name__ == '__main__':
    unittest.main()