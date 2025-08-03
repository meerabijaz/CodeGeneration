import pandas as pd
import numpy as np
import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Union, Tuple, Optional

class DataStorage:
    """
    A class for storing, indexing, and querying financial data efficiently.
    Supports multiple storage strategies including in-memory, SQLite, and file-based.
    """
    
    def __init__(self, storage_type='memory', db_path=None):
        """
        Initialize the DataStorage with the specified storage type.
        
        Args:
            storage_type (str): Type of storage to use ('memory', 'sqlite', or 'file')
            db_path (str, optional): Path to the database file for SQLite storage
        """
        self.storage_type = storage_type
        self.db_path = db_path
        
        # In-memory storage
        self.data = {}
        self.indexes = {}
        self.metadata = {}
        
        # SQLite connection (if applicable)
        self.conn = None
        if storage_type == 'sqlite' and db_path:
            self.conn = sqlite3.connect(db_path)
            self._initialize_sqlite_db()
    
    def _initialize_sqlite_db(self):
        """
        Initialize the SQLite database with necessary tables.
        """
        if not self.conn:
            return
            
        cursor = self.conn.cursor()
        
        # Create metadata table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            dataset_name TEXT PRIMARY KEY,
            column_types TEXT,
            created_at TEXT,
            row_count INTEGER,
            column_count INTEGER
        )
        """)
        
        # Create indexes table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS indexes (
            dataset_name TEXT,
            index_name TEXT,
            index_type TEXT,
            column_name TEXT,
            PRIMARY KEY (dataset_name, index_name, column_name)
        )
        """)
        
        self.conn.commit()
    
    def store_data(self, name: str, df: pd.DataFrame, column_types: Dict[str, Dict[str, str]]) -> bool:
        """
        Store a DataFrame with its metadata.
        
        Args:
            name (str): Name of the dataset
            df (pd.DataFrame): The DataFrame to store
            column_types (Dict): Dictionary of column types from DataTypeDetector
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.storage_type == 'memory':
            return self._store_in_memory(name, df, column_types)
        elif self.storage_type == 'sqlite':
            return self._store_in_sqlite(name, df, column_types)
        elif self.storage_type == 'file':
            return self._store_in_file(name, df, column_types)
        return False
    
    def _store_in_memory(self, name: str, df: pd.DataFrame, column_types: Dict[str, Dict[str, str]]) -> bool:
        """
        Store data in memory.
        """
        try:
            # Store the DataFrame
            self.data[name] = df.copy()
            
            # Store metadata
            self.metadata[name] = {
                'column_types': column_types,
                'created_at': datetime.now().isoformat(),
                'row_count': len(df),
                'column_count': len(df.columns)
            }
            
            # Initialize empty indexes
            self.indexes[name] = {
                'date_index': {},
                'amount_index': {},
                'category_index': {},
                'text_index': {}
            }
            
            return True
        except Exception as e:
            print(f"Error storing data in memory: {e}")
            return False
    
    def _store_in_sqlite(self, name: str, df: pd.DataFrame, column_types: Dict[str, Dict[str, str]]) -> bool:
        """
        Store data in SQLite database.
        """
        if not self.conn:
            print("SQLite connection not established")
            return False
            
        try:
            # Create table for the dataset
            df.to_sql(name, self.conn, if_exists='replace', index=False)
            
            # Store metadata
            cursor = self.conn.cursor()
            cursor.execute("""
            INSERT OR REPLACE INTO metadata (dataset_name, column_types, created_at, row_count, column_count)
            VALUES (?, ?, ?, ?, ?)
            """, (
                name,
                json.dumps(column_types),
                datetime.now().isoformat(),
                len(df),
                len(df.columns)
            ))
            
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error storing data in SQLite: {e}")
            return False
    
    def _store_in_file(self, name: str, df: pd.DataFrame, column_types: Dict[str, Dict[str, str]]) -> bool:
        """
        Store data in CSV and JSON files.
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs('data/processed', exist_ok=True)
            
            # Save DataFrame to CSV
            csv_path = f"data/processed/{name}.csv"
            df.to_csv(csv_path, index=False)
            
            # Save metadata to JSON
            metadata = {
                'column_types': column_types,
                'created_at': datetime.now().isoformat(),
                'row_count': len(df),
                'column_count': len(df.columns)
            }
            
            json_path = f"data/processed/{name}_metadata.json"
            with open(json_path, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            return True
        except Exception as e:
            print(f"Error storing data in files: {e}")
            return False
    
    def create_indexes(self, name: str, columns: List[str]) -> bool:
        """
        Create indexes for specified columns to speed up queries.
        
        Args:
            name (str): Name of the dataset
            columns (List[str]): List of columns to index
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.storage_type == 'memory':
            return self._create_memory_indexes(name, columns)
        elif self.storage_type == 'sqlite':
            return self._create_sqlite_indexes(name, columns)
        return False
    
    def _create_memory_indexes(self, name: str, columns: List[str]) -> bool:
        """
        Create in-memory indexes for faster lookups.
        """
        if name not in self.data or name not in self.metadata:
            print(f"Dataset '{name}' not found")
            return False
            
        try:
            df = self.data[name]
            column_types = self.metadata[name]['column_types']
            
            for column in columns:
                if column not in df.columns:
                    continue
                    
                # Get column type
                col_type = 'unknown'
                if column in column_types:
                    col_type = column_types[column]['type']
                
                # Create appropriate index based on column type
                if col_type == 'date':
                    # Date index: map dates to row indices
                    date_values = df[column].dropna()
                    self.indexes[name]['date_index'][column] = {
                        str(date): idx for idx, date in zip(date_values.index, date_values)
                    }
                    
                elif col_type == 'number':
                    # Amount index: create bins for range queries
                    amount_values = df[column].dropna()
                    if len(amount_values) > 0:
                        min_val = amount_values.min()
                        max_val = amount_values.max()
                        bins = np.linspace(min_val, max_val, 20)  # 20 bins
                        
                        binned_data = {}
                        for i in range(len(bins)-1):
                            bin_min, bin_max = bins[i], bins[i+1]
                            mask = (amount_values >= bin_min) & (amount_values < bin_max)
                            binned_data[f"{bin_min:.2f}-{bin_max:.2f}"] = amount_values[mask].index.tolist()
                            
                        self.indexes[name]['amount_index'][column] = binned_data
                        
                elif col_type == 'string':
                    # Category/text index: map unique values to row indices
                    string_values = df[column].dropna()
                    unique_values = string_values.unique()
                    
                    if len(unique_values) < len(string_values) * 0.5:  # If cardinality is low enough
                        # Categorical index
                        self.indexes[name]['category_index'][column] = {
                            str(val): string_values[string_values == val].index.tolist() 
                            for val in unique_values
                        }
                    else:
                        # Text index (simple implementation)
                        text_index = {}
                        for idx, text in zip(string_values.index, string_values):
                            words = str(text).lower().split()
                            for word in words:
                                if word not in text_index:
                                    text_index[word] = []
                                text_index[word].append(idx)
                                
                        self.indexes[name]['text_index'][column] = text_index
            
            return True
        except Exception as e:
            print(f"Error creating memory indexes: {e}")
            return False
    
    def _create_sqlite_indexes(self, name: str, columns: List[str]) -> bool:
        """
        Create SQLite indexes for faster queries.
        """
        if not self.conn:
            print("SQLite connection not established")
            return False
            
        try:
            cursor = self.conn.cursor()
            
            # Get column types from metadata
            cursor.execute("SELECT column_types FROM metadata WHERE dataset_name = ?", (name,))
            result = cursor.fetchone()
            if not result:
                print(f"Dataset '{name}' metadata not found")
                return False
                
            column_types = json.loads(result[0])
            
            for column in columns:
                # Get column type
                col_type = 'unknown'
                if column in column_types:
                    col_type = column_types[column]['type']
                
                # Create SQLite index
                index_name = f"idx_{name}_{column.replace(' ', '_')}"
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {name} ({column})")
                
                # Record the index in our indexes table
                cursor.execute("""
                INSERT OR REPLACE INTO indexes (dataset_name, index_name, index_type, column_name)
                VALUES (?, ?, ?, ?)
                """, (name, index_name, col_type, column))
            
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error creating SQLite indexes: {e}")
            return False
    
    def query_by_criteria(self, name: str, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Query data based on specified criteria.
        
        Args:
            name (str): Name of the dataset
            filters (Dict): Dictionary of column-value pairs to filter by
                            Special operators can be used with '__' suffix:
                            - column__gt: Greater than
                            - column__lt: Less than
                            - column__between: Between two values (tuple)
                            - column__in: In a list of values
                            - column__contains: Contains substring (for text)
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        if self.storage_type == 'memory':
            return self._query_memory(name, filters)
        elif self.storage_type == 'sqlite':
            return self._query_sqlite(name, filters)
        elif self.storage_type == 'file':
            return self._query_file(name, filters)
        return pd.DataFrame()
    
    def _query_memory(self, name: str, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Query in-memory data using indexes when available.
        """
        if name not in self.data:
            print(f"Dataset '{name}' not found")
            return pd.DataFrame()
            
        try:
            df = self.data[name]
            result_indices = set(range(len(df)))  # Start with all indices
            
            for key, value in filters.items():
                # Check for special operators
                if '__' in key:
                    column, operator = key.split('__', 1)
                    if column not in df.columns:
                        continue
                        
                    # Apply the appropriate filter based on operator
                    if operator == 'gt':
                        mask = df[column] > value
                        result_indices &= set(df[mask].index)
                    elif operator == 'lt':
                        mask = df[column] < value
                        result_indices &= set(df[mask].index)
                    elif operator == 'between' and isinstance(value, tuple) and len(value) == 2:
                        mask = (df[column] >= value[0]) & (df[column] <= value[1])
                        result_indices &= set(df[mask].index)
                    elif operator == 'in' and isinstance(value, list):
                        mask = df[column].isin(value)
                        result_indices &= set(df[mask].index)
                    elif operator == 'contains' and isinstance(value, str):
                        mask = df[column].astype(str).str.contains(value, na=False)
                        result_indices &= set(df[mask].index)
                else:
                    # Simple equality filter
                    if key in df.columns:
                        mask = df[key] == value
                        result_indices &= set(df[mask].index)
            
            # Return filtered DataFrame
            return df.loc[list(result_indices)].reset_index(drop=True)
        except Exception as e:
            print(f"Error querying memory data: {e}")
            return pd.DataFrame()
    
    def _query_sqlite(self, name: str, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Query SQLite data with SQL WHERE clauses.
        """
        if not self.conn:
            print("SQLite connection not established")
            return pd.DataFrame()
            
        try:
            # Build SQL query
            where_clauses = []
            params = []
            
            for key, value in filters.items():
                # Check for special operators
                if '__' in key:
                    column, operator = key.split('__', 1)
                    
                    # Apply the appropriate filter based on operator
                    if operator == 'gt':
                        where_clauses.append(f"{column} > ?")
                        params.append(value)
                    elif operator == 'lt':
                        where_clauses.append(f"{column} < ?")
                        params.append(value)
                    elif operator == 'between' and isinstance(value, tuple) and len(value) == 2:
                        where_clauses.append(f"{column} BETWEEN ? AND ?")
                        params.extend(value)
                    elif operator == 'in' and isinstance(value, list):
                        placeholders = ', '.join(['?'] * len(value))
                        where_clauses.append(f"{column} IN ({placeholders})")
                        params.extend(value)
                    elif operator == 'contains' and isinstance(value, str):
                        where_clauses.append(f"{column} LIKE ?")
                        params.append(f"%{value}%")
                else:
                    # Simple equality filter
                    where_clauses.append(f"{key} = ?")
                    params.append(value)
            
            # Construct the full query
            query = f"SELECT * FROM {name}"
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Execute query and return as DataFrame
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            print(f"Error querying SQLite data: {e}")
            return pd.DataFrame()
    
    def _query_file(self, name: str, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Query file-based data by loading CSV and applying filters.
        """
        try:
            # Load the CSV file
            csv_path = f"data/processed/{name}.csv"
            if not os.path.exists(csv_path):
                print(f"Dataset file '{csv_path}' not found")
                return pd.DataFrame()
                
            df = pd.read_csv(csv_path)
            
            # Apply filters (similar to in-memory filtering)
            for key, value in filters.items():
                # Check for special operators
                if '__' in key:
                    column, operator = key.split('__', 1)
                    if column not in df.columns:
                        continue
                        
                    # Apply the appropriate filter based on operator
                    if operator == 'gt':
                        df = df[df[column] > value]
                    elif operator == 'lt':
                        df = df[df[column] < value]
                    elif operator == 'between' and isinstance(value, tuple) and len(value) == 2:
                        df = df[(df[column] >= value[0]) & (df[column] <= value[1])]
                    elif operator == 'in' and isinstance(value, list):
                        df = df[df[column].isin(value)]
                    elif operator == 'contains' and isinstance(value, str):
                        df = df[df[column].astype(str).str.contains(value, na=False)]
                else:
                    # Simple equality filter
                    if key in df.columns:
                        df = df[df[key] == value]
            
            return df.reset_index(drop=True)
        except Exception as e:
            print(f"Error querying file data: {e}")
            return pd.DataFrame()
    
    def aggregate_data(self, name: str, group_by: List[str], measures: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate data by grouping and applying aggregate functions.
        
        Args:
            name (str): Name of the dataset
            group_by (List[str]): Columns to group by
            measures (Dict[str, str]): Dictionary mapping columns to aggregate functions
                                       (e.g., {'Amount': 'sum', 'Quantity': 'mean'})
            
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        if self.storage_type == 'memory':
            return self._aggregate_memory(name, group_by, measures)
        elif self.storage_type == 'sqlite':
            return self._aggregate_sqlite(name, group_by, measures)
        elif self.storage_type == 'file':
            return self._aggregate_file(name, group_by, measures)
        return pd.DataFrame()
    
    def _aggregate_memory(self, name: str, group_by: List[str], measures: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate in-memory data using pandas groupby.
        """
        if name not in self.data:
            print(f"Dataset '{name}' not found")
            return pd.DataFrame()
            
        try:
            df = self.data[name]
            
            # Check if all group_by columns exist
            missing_columns = [col for col in group_by if col not in df.columns]
            if missing_columns:
                print(f"Missing group_by columns: {missing_columns}")
                return pd.DataFrame()
                
            # Check if all measure columns exist
            missing_measures = [col for col in measures.keys() if col not in df.columns]
            if missing_measures:
                print(f"Missing measure columns: {missing_measures}")
                return pd.DataFrame()
            
            # Perform groupby and aggregation
            grouped = df.groupby(group_by)
            agg_dict = {col: func for col, func in measures.items()}
            result = grouped.agg(agg_dict).reset_index()
            
            return result
        except Exception as e:
            print(f"Error aggregating memory data: {e}")
            return pd.DataFrame()
    
    def _aggregate_sqlite(self, name: str, group_by: List[str], measures: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate SQLite data using SQL GROUP BY.
        """
        if not self.conn:
            print("SQLite connection not established")
            return pd.DataFrame()
            
        try:
            # Build SQL aggregation query
            group_by_cols = ", ".join(group_by)
            
            # Map pandas aggregation functions to SQL
            sql_agg_map = {
                'sum': 'SUM',
                'mean': 'AVG',
                'min': 'MIN',
                'max': 'MAX',
                'count': 'COUNT'
            }
            
            # Build the SELECT clause with aggregations
            select_clauses = [f"{col}" for col in group_by]
            for col, func in measures.items():
                sql_func = sql_agg_map.get(func, 'SUM')  # Default to SUM if unknown
                select_clauses.append(f"{sql_func}({col}) AS {col}_{func}")
            
            select_clause = ", ".join(select_clauses)
            
            # Construct the full query
            query = f"SELECT {select_clause} FROM {name} GROUP BY {group_by_cols}"
            
            # Execute query and return as DataFrame
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"Error aggregating SQLite data: {e}")
            return pd.DataFrame()
    
    def _aggregate_file(self, name: str, group_by: List[str], measures: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate file-based data by loading CSV and using pandas groupby.
        """
        try:
            # Load the CSV file
            csv_path = f"data/processed/{name}.csv"
            if not os.path.exists(csv_path):
                print(f"Dataset file '{csv_path}' not found")
                return pd.DataFrame()
                
            df = pd.read_csv(csv_path)
            
            # Check if all group_by columns exist
            missing_columns = [col for col in group_by if col not in df.columns]
            if missing_columns:
                print(f"Missing group_by columns: {missing_columns}")
                return pd.DataFrame()
                
            # Check if all measure columns exist
            missing_measures = [col for col in measures.keys() if col not in df.columns]
            if missing_measures:
                print(f"Missing measure columns: {missing_measures}")
                return pd.DataFrame()
            
            # Perform groupby and aggregation
            grouped = df.groupby(group_by)
            agg_dict = {col: func for col, func in measures.items()}
            result = grouped.agg(agg_dict).reset_index()
            
            return result
        except Exception as e:
            print(f"Error aggregating file data: {e}")
            return pd.DataFrame()
    
    def get_metadata(self, name: str) -> Dict[str, Any]:
        """
        Get metadata for a dataset.
        
        Args:
            name (str): Name of the dataset
            
        Returns:
            Dict: Metadata dictionary
        """
        if self.storage_type == 'memory':
            return self.metadata.get(name, {})
        elif self.storage_type == 'sqlite':
            if not self.conn:
                return {}
                
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM metadata WHERE dataset_name = ?", (name,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'dataset_name': result[0],
                        'column_types': json.loads(result[1]),
                        'created_at': result[2],
                        'row_count': result[3],
                        'column_count': result[4]
                    }
                return {}
            except Exception as e:
                print(f"Error getting SQLite metadata: {e}")
                return {}
        elif self.storage_type == 'file':
            json_path = f"data/processed/{name}_metadata.json"
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    print(f"Error loading metadata file: {e}")
            return {}
        return {}
    
    def close(self):
        """
        Close any open connections and clean up resources.
        """
        if self.storage_type == 'sqlite' and self.conn:
            self.conn.close()
            self.conn = None