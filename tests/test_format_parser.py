import unittest
import datetime
import pandas as pd
import numpy as np
from src.core.format_parser import FormatParser

class TestFormatParser(unittest.TestCase):
    """
    Test cases for the FormatParser class.
    """
    
    def setUp(self):
        """
        Set up test fixtures.
        """
        self.parser = FormatParser()
        
        # Test data for amounts
        self.test_amounts = [
            "$1,234.56",      # US format with currency
            "(2,500.00)",     # Accounting format (negative)
            "€1.234,56",      # European format with currency
            "1.5M",           # Abbreviated format
            "₹1,23,456",      # Indian format
            "1234.56",        # Plain number
            "-9,876.54",      # Negative with minus sign
            "$0.00",          # Zero amount
            "1,234,567.89",   # Large number with separators
            "42",             # Integer
            "N/A",            # Non-numeric
            None,             # None value
            "",               # Empty string
            "   ",            # Whitespace
        ]
        
        # Test data for dates
        self.test_dates = [
            "12/31/2023",     # MM/DD/YYYY
            "2023-12-31",     # ISO format
            "Q4 2023",        # Quarter
            "Dec-23",         # MMM-YY
            "44927",          # Excel serial
            "January 15, 2023", # Full text date
            "15-Jan-23",      # DD-MMM-YY
            "2023/01/15",     # YYYY/MM/DD
            "N/A",            # Non-date
            None,             # None value
            "",               # Empty string
            "   ",            # Whitespace
        ]
        
        # Test data for special formats
        self.test_special = [
            "ACC-12345",       # Account code
            "REF#98765",      # Reference number
            "25%",            # Percentage
            "N/A",            # Special text
            None,             # None value
            "",               # Empty string
            "   ",            # Whitespace
        ]
    
    def test_init(self):
        """
        Test initialization of FormatParser.
        """
        self.assertIsInstance(self.parser, FormatParser)
        self.assertIsInstance(self.parser.currency_symbols, dict)
        self.assertIsInstance(self.parser.date_formats, dict)
        self.assertIsInstance(self.parser.abbreviations, dict)
    
    def test_parse_amount_us_format(self):
        """
        Test parsing US format amounts.
        """
        self.assertEqual(self.parser.parse_amount("$1,234.56"), 1234.56)
        self.assertEqual(self.parser.parse_amount("1,234.56"), 1234.56)
        self.assertEqual(self.parser.parse_amount("1234.56"), 1234.56)
    
    def test_parse_amount_european_format(self):
        """
        Test parsing European format amounts.
        """
        self.assertEqual(self.parser.parse_amount("€1.234,56"), 1234.56)
        self.assertEqual(self.parser.parse_amount("1.234,56"), 1234.56)
    
    def test_parse_amount_accounting_format(self):
        """
        Test parsing accounting format amounts (negative in parentheses).
        """
        self.assertEqual(self.parser.parse_amount("(2,500.00)"), -2500.00)
        self.assertEqual(self.parser.parse_amount("-9,876.54"), -9876.54)
    
    def test_parse_amount_abbreviated_format(self):
        """
        Test parsing abbreviated amounts (K, M, B, T).
        """
        self.assertEqual(self.parser.parse_amount("1.5M"), 1500000.00)
        self.assertEqual(self.parser.parse_amount("2.3K"), 2300.00)
        self.assertEqual(self.parser.parse_amount("4.7B"), 4700000000.00)
    
    def test_parse_amount_indian_format(self):
        """
        Test parsing Indian format amounts.
        """
        self.assertEqual(self.parser.parse_amount("₹1,23,456", detected_format='indian'), 123456.00)
    
    def test_parse_amount_edge_cases(self):
        """
        Test parsing amount edge cases.
        """
        self.assertEqual(self.parser.parse_amount("$0.00"), 0.00)
        self.assertEqual(self.parser.parse_amount(42), 42.00)
        self.assertEqual(self.parser.parse_amount(42.5), 42.50)
        self.assertIsNone(self.parser.parse_amount("N/A"))
        self.assertIsNone(self.parser.parse_amount(None))
        self.assertIsNone(self.parser.parse_amount(""))
        self.assertIsNone(self.parser.parse_amount("   "))
    
    def test_parse_date_standard_formats(self):
        """
        Test parsing standard date formats.
        """
        self.assertEqual(self.parser.parse_date("12/31/2023"), datetime.date(2023, 12, 31))
        self.assertEqual(self.parser.parse_date("2023-12-31"), datetime.date(2023, 12, 31))
        self.assertEqual(self.parser.parse_date("January 15, 2023"), datetime.date(2023, 1, 15))
    
    def test_parse_date_special_formats(self):
        """
        Test parsing special date formats.
        """
        self.assertEqual(self.parser.parse_date("Q4 2023"), datetime.date(2023, 10, 1))
        self.assertEqual(self.parser.parse_date("15-Jan-23"), datetime.date(2023, 1, 15))
    
    def test_parse_date_excel_serial(self):
        """
        Test parsing Excel serial date numbers.
        """
        # Excel serial 44927 corresponds to December 31, 2022
        expected_date = pd.to_datetime(44927, unit='D', origin='1899-12-30').date()
        self.assertEqual(self.parser.parse_date(44927), expected_date)
    
    def test_parse_date_edge_cases(self):
        """
        Test parsing date edge cases.
        """
        today = datetime.date.today()
        self.assertEqual(self.parser.parse_date(today), today)
        self.assertEqual(self.parser.parse_date(datetime.datetime.now()), today)
        self.assertIsNone(self.parser.parse_date("N/A"))
        self.assertIsNone(self.parser.parse_date(None))
        self.assertIsNone(self.parser.parse_date(""))
        self.assertIsNone(self.parser.parse_date("   "))
    
    def test_normalize_currency(self):
        """
        Test normalizing currency values.
        """
        # Test with USD
        result = self.parser.normalize_currency("$1,234.56")
        self.assertEqual(result['value'], 1234.56)
        self.assertEqual(result['currency'], 'USD')
        self.assertEqual(result['original_value'], "$1,234.56")
        
        # Test with EUR
        result = self.parser.normalize_currency("€1.234,56")
        self.assertEqual(result['value'], 1234.56)
        self.assertEqual(result['currency'], 'EUR')
        self.assertEqual(result['original_value'], "€1.234,56")
        
        # Test with exchange rates
        exchange_rates = {'EUR': 0.85, 'GBP': 0.75}
        result = self.parser.normalize_currency("€100", target_currency='USD', exchange_rates=exchange_rates)
        self.assertAlmostEqual(result['value'], 100 / 0.85)
        self.assertEqual(result['currency'], 'EUR')
    
    def test_handle_special_formats(self):
        """
        Test handling special formats.
        """
        # Test account code
        self.assertEqual(self.parser.handle_special_formats("ACC-12345", format_type='account_code'), "ACC12345")
        
        # Test reference number
        self.assertEqual(self.parser.handle_special_formats("REF#98765", format_type='reference_number'), "REF#98765")
        
        # Test percentage
        self.assertEqual(self.parser.handle_special_formats("25%", format_type='percentage'), 0.25)
        
        # Test default behavior
        self.assertEqual(self.parser.handle_special_formats("Some text"), "Some text")
    
    def test_batch_parse_amounts(self):
        """
        Test batch parsing of amounts.
        """
        amounts = ["$100", "€200", "300"]
        expected = [100.0, 200.0, 300.0]
        self.assertEqual(self.parser.batch_parse_amounts(amounts), expected)
    
    def test_batch_parse_dates(self):
        """
        Test batch parsing of dates.
        """
        dates = ["2023-01-01", "01/15/2023", "Q1 2023"]
        expected = [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 15),
            datetime.date(2023, 1, 1)
        ]
        self.assertEqual(self.parser.batch_parse_dates(dates), expected)

if __name__ == '__main__':
    unittest.main()