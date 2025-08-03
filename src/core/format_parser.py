import pandas as pd
import re
import datetime
from dateutil import parser
from typing import Dict, List, Union, Any, Tuple

class FormatParser:
    """
    A class for parsing and normalizing various financial data formats.
    
    This class provides functionality to parse and normalize different formats of:
    - Amounts (US, European, Indian, etc.)
    - Dates (MM/DD/YYYY, DD/MM/YYYY, Excel serial, etc.)
    - Special formats (abbreviations, codes, etc.)
    """
    
    def __init__(self):
        """
        Initialize the FormatParser with default settings.
        """
        # Currency symbols mapping
        self.currency_symbols = {
            '$': 'USD',
            '€': 'EUR',
            '£': 'GBP',
            '¥': 'JPY',
            '₹': 'INR',
            'kr': 'SEK',  # Swedish Krona
            'CHF': 'CHF',  # Swiss Franc
            'A$': 'AUD',  # Australian Dollar
            'C$': 'CAD',  # Canadian Dollar
        }
        
        # Date format patterns
        self.date_formats = {
            'mm/dd/yyyy': r'^(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/\d{4}$',
            'dd/mm/yyyy': r'^(0?[1-9]|[12]\d|3[01])/(0?[1-9]|1[0-2])/\d{4}$',
            'yyyy-mm-dd': r'^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12]\d|3[01])$',
            'dd-mmm-yy': r'^(0?[1-9]|[12]\d|3[01])-[A-Za-z]{3}-\d{2}$',
            'mmm-yy': r'^[A-Za-z]{3}-\d{2}$',
            'quarter': r'^Q[1-4]\s+\d{4}$',
            'excel_serial': r'^\d{5}$',  # Excel date serial numbers typically 5 digits
        }
        
        # Abbreviation mappings
        self.abbreviations = {
            'K': 1_000,
            'M': 1_000_000,
            'B': 1_000_000_000,
            'T': 1_000_000_000_000,
        }
    
    def parse_amount(self, value: Any, detected_format: str = None) -> float:
        """
        Parse a financial amount in various formats and return a normalized float value.
        
        Args:
            value: The amount value to parse
            detected_format: Optional format hint (e.g., 'us', 'european', 'accounting', etc.)
            
        Returns:
            float: The normalized amount as a float
        """
        if pd.isna(value):
            return None
        
        # Convert to string if not already
        if not isinstance(value, str):
            # If it's already a number, just return it
            if isinstance(value, (int, float)):
                return float(value)
            value = str(value)
        
        # Remove whitespace
        value = value.strip()
        
        # Handle empty strings
        if not value:
            return None
        
        # Extract currency symbol if present
        currency_code = None
        for symbol, code in self.currency_symbols.items():
            if symbol in value:
                currency_code = code
                value = value.replace(symbol, '')
                break
        
        # Handle parentheses for negative values (accounting format)
        is_negative = False
        if value.startswith('(') and value.endswith(')'):
            is_negative = True
            value = value[1:-1]
        elif value.startswith('-'):
            is_negative = True
            value = value[1:]
        
        # Handle abbreviations (K, M, B, T)
        multiplier = 1
        for abbr, mult in self.abbreviations.items():
            if value.upper().endswith(abbr):
                multiplier = mult
                value = value[:-1]  # Remove the abbreviation
                break
        
        # Clean the string for parsing
        # First, determine if it's European format (using comma as decimal separator)
        if ',' in value and '.' in value:
            # If both comma and period exist, determine which is the decimal separator
            # based on position (rightmost is usually the decimal separator)
            if value.rindex(',') > value.rindex('.'):
                # European format: 1.234,56
                value = value.replace('.', '')
                value = value.replace(',', '.')
            else:
                # US format: 1,234.56
                value = value.replace(',', '')
        elif ',' in value:
            # Could be European decimal or US thousands separator
            # If it's followed by exactly 2 digits at the end, likely European decimal
            if re.search(r',\d{2}$', value):
                value = value.replace(',', '.')
            else:
                value = value.replace(',', '')
        
        # Handle Indian format (e.g., 1,23,456.78)
        if detected_format == 'indian':
            value = re.sub(r'[,\s]', '', value)
        
        # Try to convert to float
        try:
            result = float(value) * multiplier
            if is_negative:
                result = -result
            return result
        except ValueError:
            # If conversion fails, return None
            return None
    
    def parse_date(self, value: Any, detected_format: str = None) -> datetime.date:
        """
        Parse a date in various formats and return a normalized datetime.date object.
        
        Args:
            value: The date value to parse
            detected_format: Optional format hint (e.g., 'mm/dd/yyyy', 'excel_serial', etc.)
            
        Returns:
            datetime.date: The normalized date
        """
        if pd.isna(value):
            return None
        
        # If it's already a datetime object, convert to date and return
        if isinstance(value, datetime.datetime):
            return value.date()
        elif isinstance(value, datetime.date):
            return value
        
        # Convert to string if not already
        if not isinstance(value, str):
            # Handle Excel serial date numbers
            if isinstance(value, (int, float)) and 36000 <= value <= 50000:  # Typical Excel date range
                try:
                    # Excel dates are number of days since December 30, 1899
                    # But Excel has a leap year bug, so we use pandas to convert
                    return pd.to_datetime(value, unit='D', origin='1899-12-30').date()
                except:
                    pass
            value = str(value)
        
        # Remove whitespace
        value = value.strip()
        
        # Handle empty strings
        if not value:
            return None
        
        # Handle quarter format (e.g., "Q1 2023")
        quarter_match = re.match(r'Q([1-4])\s+(\d{4})', value)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year = int(quarter_match.group(2))
            # Return the first day of the quarter
            month = (quarter - 1) * 3 + 1
            return datetime.date(year, month, 1)
        
        # Try parsing with dateutil parser
        try:
            return parser.parse(value).date()
        except:
            # If parsing fails, return None
            return None
    
    def normalize_currency(self, value: Any, target_currency: str = 'USD', exchange_rates: Dict[str, float] = None) -> Dict[str, Any]:
        """
        Normalize a currency value to a target currency.
        
        Args:
            value: The currency value to normalize
            target_currency: The target currency code
            exchange_rates: Dictionary of exchange rates relative to the target currency
            
        Returns:
            Dict: Dictionary with normalized value and metadata
        """
        if pd.isna(value):
            return {'value': None, 'currency': None, 'original_value': value}
        
        # Convert to string if not already
        if not isinstance(value, str):
            if isinstance(value, (int, float)):
                return {'value': float(value), 'currency': target_currency, 'original_value': value}
            value = str(value)
        
        # Remove whitespace
        value = value.strip()
        
        # Handle empty strings
        if not value:
            return {'value': None, 'currency': None, 'original_value': value}
        
        # Extract currency symbol if present
        currency_code = target_currency
        original_value = value
        
        for symbol, code in self.currency_symbols.items():
            if symbol in value:
                currency_code = code
                value = value.replace(symbol, '')
                break
        
        # Parse the amount
        amount = self.parse_amount(value)
        
        # Apply exchange rate if provided and needed
        if amount is not None and exchange_rates and currency_code != target_currency:
            if currency_code in exchange_rates:
                amount = amount / exchange_rates[currency_code]
            else:
                # If no exchange rate is available, keep the original amount but note the currency
                pass
        
        return {
            'value': amount,
            'currency': currency_code,
            'original_value': original_value
        }
    
    def handle_special_formats(self, value: Any, format_type: str = None) -> Any:
        """
        Handle special formats like codes, abbreviations, etc.
        
        Args:
            value: The value to parse
            format_type: Type of special format (e.g., 'account_code', 'reference_number', etc.)
            
        Returns:
            Parsed and normalized value
        """
        if pd.isna(value):
            return None
        
        # Convert to string if not already
        if not isinstance(value, str):
            value = str(value)
        
        # Remove whitespace
        value = value.strip()
        
        # Handle empty strings
        if not value:
            return None
        
        # Handle based on format type
        if format_type == 'account_code':
            # Standardize account codes (remove spaces, dashes, etc.)
            return re.sub(r'[\s-]', '', value).upper()
        
        elif format_type == 'reference_number':
            # Standardize reference numbers
            return re.sub(r'[\s]', '', value)
        
        elif format_type == 'percentage':
            # Handle percentage values
            value = value.replace('%', '')
            try:
                return float(value) / 100.0
            except ValueError:
                return None
        
        # Default: return the value as is
        return value
    
    def batch_parse_amounts(self, values: List[Any], detected_format: str = None) -> List[float]:
        """
        Parse a list of financial amounts.
        
        Args:
            values: List of amount values to parse
            detected_format: Optional format hint
            
        Returns:
            List[float]: List of normalized amounts
        """
        return [self.parse_amount(value, detected_format) for value in values]
    
    def batch_parse_dates(self, values: List[Any], detected_format: str = None) -> List[datetime.date]:
        """
        Parse a list of dates.
        
        Args:
            values: List of date values to parse
            detected_format: Optional format hint
            
        Returns:
            List[datetime.date]: List of normalized dates
        """
        return [self.parse_date(value, detected_format) for value in values]