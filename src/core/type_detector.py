import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Dict, List, Tuple, Union, Any

class DataTypeDetector:
    """
    A class for detecting and classifying data types in financial datasets.
    
    This class provides functionality to analyze columns in a DataFrame and
    determine their likely data types (string, number, date) with confidence scores.
    """
    
    def __init__(self):
        """
        Initialize the DataTypeDetector with common patterns for financial data.
        """
        # Patterns for date detection
        self.date_patterns = [
            # MM/DD/YYYY or DD/MM/YYYY
            r'^\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}$',
            # YYYY-MM-DD
            r'^\d{4}[/.-]\d{1,2}[/.-]\d{1,2}$',
            # DD-MON-YYYY or DD-MON-YY
            r'^\d{1,2}[-]?[A-Za-z]{3}[-]?\d{2,4}$',
            # Month YYYY or Mon YYYY
            r'^[A-Za-z]{3,9}\s+\d{4}$',
            # Quarter formats
            r'^Q[1-4]\s+\d{4}$',
            r'^Quarter\s+[1-4]\s+\d{4}$'
        ]
        
        # Patterns for number detection
        self.number_patterns = [
            # Standard numbers with optional commas and decimal points
            r'^[+-]?\d{1,3}(,\d{3})*(\.\d+)?$',
            # Numbers with parentheses for negative values
            r'^\(\d{1,3}(,\d{3})*(\.\d+)?\)$',
            # European format with dots as thousand separators and commas for decimals
            r'^[+-]?\d{1,3}(\.\d{3})*(,\d+)?$',
            # Indian format with special thousands grouping
            r'^[+-]?\d{1,2}(,\d{2})*(,\d{3})*(\.\d+)?$',
            # Numbers with trailing negative sign
            r'^\d{1,3}(,\d{3})*(\.\d+)?-$',
            # Numbers with currency symbols
            r'^[$€£¥₹]\d{1,3}(,\d{3})*(\.\d+)?$',
            # Abbreviated amounts (K, M, B)
            r'^[+-]?\d+(\.\d+)?[KMB]$'
        ]
        
        # Common financial string patterns
        self.financial_string_patterns = [
            # Account numbers
            r'^\d{4,}$',
            # Reference codes
            r'^[A-Za-z0-9]{5,}$',
            # Transaction IDs
            r'^[A-Za-z]{2,}\d{4,}$'
        ]
    
    def analyze_column(self, data: pd.Series) -> Dict[str, Any]:
        """
        Analyze a column of data to determine its likely data type.
        
        Args:
            data: Pandas Series containing the column data
            
        Returns:
            Dict: Dictionary with detected type, confidence score, and format information
        """
        # Remove null values for analysis
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {
                'type': 'unknown',
                'confidence': 0.0,
                'format': None
            }
        
        # Sample data for analysis (up to 100 values)
        sample_size = min(100, len(clean_data))
        sample_data = clean_data.sample(sample_size) if len(clean_data) > sample_size else clean_data
        
        # Check for date type
        date_result = self.detect_date_format(sample_data)
        if date_result['confidence'] > 0.7:
            return date_result
        
        # Check for number type
        number_result = self.detect_number_format(sample_data)
        if number_result['confidence'] > 0.7:
            return number_result
        
        # If not clearly date or number, classify as string
        string_result = self.classify_string_type(sample_data)
        return string_result
    
    def detect_date_format(self, sample_values: pd.Series) -> Dict[str, Any]:
        """
        Detect if the column contains date values and identify the format.
        
        Args:
            sample_values: Sample values from the column
            
        Returns:
            Dict: Dictionary with type, confidence score, and format information
        """
        # Try to parse as pandas datetime
        try:
            # Check if already datetime
            if pd.api.types.is_datetime64_any_dtype(sample_values):
                return {
                    'type': 'date',
                    'confidence': 1.0,
                    'format': 'datetime64'
                }
            
            # Try pandas to_datetime
            pd.to_datetime(sample_values, errors='raise')
            return {
                'type': 'date',
                'confidence': 0.9,
                'format': 'auto-detected'
            }
        except:
            pass
        
        # Check for Excel serial dates (numeric values around 40000-50000)
        if pd.api.types.is_numeric_dtype(sample_values):
            excel_date_count = sum((sample_values >= 36500) & (sample_values <= 50000))
            if excel_date_count / len(sample_values) > 0.8:
                return {
                    'type': 'date',
                    'confidence': 0.8,
                    'format': 'excel_serial'
                }
        
        # Check against date patterns
        if sample_values.dtype == 'object':
            pattern_matches = 0
            matched_pattern = None
            
            for pattern in self.date_patterns:
                pattern_count = sum(sample_values.astype(str).str.match(pattern, na=False))
                if pattern_count > pattern_matches:
                    pattern_matches = pattern_count
                    matched_pattern = pattern
            
            confidence = pattern_matches / len(sample_values)
            if confidence > 0.5:
                return {
                    'type': 'date',
                    'confidence': confidence,
                    'format': matched_pattern
                }
        
        return {
            'type': 'date',
            'confidence': 0.0,
            'format': None
        }
    
    def detect_number_format(self, sample_values: pd.Series) -> Dict[str, Any]:
        """
        Detect if the column contains numeric values and identify the format.
        
        Args:
            sample_values: Sample values from the column
            
        Returns:
            Dict: Dictionary with type, confidence score, and format information
        """
        # Check if already numeric
        if pd.api.types.is_numeric_dtype(sample_values):
            return {
                'type': 'number',
                'confidence': 1.0,
                'format': 'numeric'
            }
        
        # For object types, try to identify number patterns
        if sample_values.dtype == 'object':
            # Try to convert to numeric after cleaning
            cleaned_values = sample_values.astype(str).copy()
            
            # Remove currency symbols, commas, parentheses
            cleaned_values = cleaned_values.str.replace(r'[$€£¥₹]', '', regex=True)
            cleaned_values = cleaned_values.str.replace(r',', '', regex=True)
            cleaned_values = cleaned_values.str.replace(r'\((.+)\)', r'-\1', regex=True)
            cleaned_values = cleaned_values.str.replace(r'(\d+)-$', r'-\1', regex=True)
            
            # Handle K, M, B abbreviations
            cleaned_values = cleaned_values.str.replace(r'(\d+\.?\d*)K$', lambda x: str(float(x.group(1)) * 1000), regex=True)
            cleaned_values = cleaned_values.str.replace(r'(\d+\.?\d*)M$', lambda x: str(float(x.group(1)) * 1000000), regex=True)
            cleaned_values = cleaned_values.str.replace(r'(\d+\.?\d*)B$', lambda x: str(float(x.group(1)) * 1000000000), regex=True)
            
            # Handle European format (replace comma with dot for decimal)
            european_format = cleaned_values.str.contains(r'^\d{1,3}(\.\d{3})*(,\d+)$', regex=True)
            if european_format.sum() > 0:
                cleaned_values.loc[european_format] = cleaned_values.loc[european_format].str.replace('.', '')
                cleaned_values.loc[european_format] = cleaned_values.loc[european_format].str.replace(',', '.')
            
            # Try to convert to numeric
            numeric_success = pd.to_numeric(cleaned_values, errors='coerce')
            success_rate = 1 - (numeric_success.isna().sum() / len(numeric_success))
            
            if success_rate > 0.5:
                # Determine format based on original values
                format_type = self._determine_number_format(sample_values)
                return {
                    'type': 'number',
                    'confidence': success_rate,
                    'format': format_type
                }
        
        # Check against number patterns
        if sample_values.dtype == 'object':
            pattern_matches = 0
            matched_pattern = None
            
            for pattern in self.number_patterns:
                pattern_count = sum(sample_values.astype(str).str.match(pattern, na=False))
                if pattern_count > pattern_matches:
                    pattern_matches = pattern_count
                    matched_pattern = pattern
            
            confidence = pattern_matches / len(sample_values)
            if confidence > 0.5:
                return {
                    'type': 'number',
                    'confidence': confidence,
                    'format': matched_pattern
                }
        
        return {
            'type': 'number',
            'confidence': 0.0,
            'format': None
        }
    
    def _determine_number_format(self, sample_values: pd.Series) -> str:
        """
        Determine the specific number format used in the sample values.
        
        Args:
            sample_values: Sample values from the column
            
        Returns:
            str: Detected number format
        """
        sample_str = sample_values.astype(str)
        
        # Check for currency symbols
        if sample_str.str.contains(r'^[$€£¥₹]', regex=True).any():
            if sample_str.str.contains(r'^\$', regex=True).mean() > 0.5:
                return 'USD'
            elif sample_str.str.contains(r'^€', regex=True).mean() > 0.5:
                return 'EUR'
            elif sample_str.str.contains(r'^£', regex=True).mean() > 0.5:
                return 'GBP'
            elif sample_str.str.contains(r'^¥', regex=True).mean() > 0.5:
                return 'JPY'
            elif sample_str.str.contains(r'^₹', regex=True).mean() > 0.5:
                return 'INR'
            return 'currency'
        
        # Check for parentheses (negative values)
        if sample_str.str.contains(r'^\(.+\)$', regex=True).mean() > 0.2:
            return 'accounting'
        
        # Check for trailing negative
        if sample_str.str.contains(r'\d+\.?\d*-$', regex=True).mean() > 0.2:
            return 'trailing_negative'
        
        # Check for K, M, B abbreviations
        if sample_str.str.contains(r'[KMB]$', regex=True).mean() > 0.2:
            return 'abbreviated'
        
        # Check for European format
        if sample_str.str.contains(r'^\d{1,3}(\.\d{3})*(,\d+)$', regex=True).mean() > 0.5:
            return 'european'
        
        # Check for Indian format
        if sample_str.str.contains(r'^\d{1,2}(,\d{2})*(,\d{3})*(\.\d+)?$', regex=True).mean() > 0.5:
            return 'indian'
        
        # Default to standard
        return 'standard'
    
    def classify_string_type(self, sample_values: pd.Series) -> Dict[str, Any]:
        """
        Classify the type of string data in the column.
        
        Args:
            sample_values: Sample values from the column
            
        Returns:
            Dict: Dictionary with type, confidence score, and format information
        """
        # Convert to string for analysis
        sample_str = sample_values.astype(str)
        
        # Check for common financial string patterns
        for pattern_name, pattern in zip(
            ['account_number', 'reference_code', 'transaction_id'],
            self.financial_string_patterns
        ):
            pattern_match_rate = sample_str.str.match(pattern, na=False).mean()
            if pattern_match_rate > 0.7:
                return {
                    'type': 'string',
                    'confidence': pattern_match_rate,
                    'format': pattern_name
                }
        
        # Check for categorical data (few unique values)
        unique_ratio = len(sample_values.unique()) / len(sample_values)
        if unique_ratio < 0.1:  # Less than 10% unique values
            return {
                'type': 'string',
                'confidence': 0.9,
                'format': 'categorical'
            }
        
        # Check average length
        avg_length = sample_str.str.len().mean()
        
        if avg_length > 100:
            # Long text, likely description
            return {
                'type': 'string',
                'confidence': 0.8,
                'format': 'description'
            }
        elif avg_length > 30:
            # Medium length, likely name or address
            return {
                'type': 'string',
                'confidence': 0.8,
                'format': 'name_or_address'
            }
        else:
            # Short text, likely code or identifier
            return {
                'type': 'string',
                'confidence': 0.8,
                'format': 'identifier'
            }
    
    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Analyze all columns in a DataFrame to determine their data types.
        
        Args:
            df: Pandas DataFrame to analyze
            
        Returns:
            Dict: Dictionary with column names as keys and type information as values
        """
        results = {}
        for column in df.columns:
            results[column] = self.analyze_column(df[column])
        return results