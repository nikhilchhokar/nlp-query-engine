"""
Schema Discovery Service
Dynamically analyzes database structure without hard-coding table or column names
"""

import sqlalchemy
from sqlalchemy import create_engine, inspect, MetaData, text
from typing import Dict, List, Any, Optional
import logging
from collections import defaultdict
import re

logger = logging.getLogger(__name__)

class SchemaDiscovery:
    """
    Automatically discovers database schema including:
    - Table names and their semantic meaning
    - Column names, types, and constraints
    - Relationships between tables
    - Sample data for context understanding
    """
    
    def __init__(self):
        self.engine = None
        self.inspector = None
        self.metadata = None
        self.schema = None
        
        # Common naming patterns for employee databases
        self.table_patterns = {
            'employees': ['employee', 'employees', 'emp', 'staff', 'personnel', 'worker'],
            'departments': ['department', 'departments', 'dept', 'division', 'divisions'],
            'salaries': ['salary', 'salaries', 'compensation', 'pay', 'payroll'],
            'performance': ['performance', 'review', 'reviews', 'evaluation', 'evaluations'],
            'documents': ['document', 'documents', 'file', 'files', 'attachment']
        }
        
        self.column_patterns = {
            'id': ['id', '_id', 'key', 'pk', 'code'],
            'name': ['name', 'full_name', 'fullname', 'fname', 'lname', 'employee_name'],
            'email': ['email', 'e_mail', 'mail', 'contact_email'],
            'salary': ['salary', 'compensation', 'pay', 'wage', 'annual_salary', 'pay_rate'],
            'date': ['date', 'hired', 'joined', 'start', 'created', 'modified'],
            'department': ['dept', 'department', 'division', 'team', 'group']
        }
    
    async def analyze_database(self, connection_string: str) -> Dict[str, Any]:
        """
        Connect to database and discover complete schema structure.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            Dictionary containing tables, columns, relationships, and metadata
        """
        try:
            # Create database connection
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True  # Verify connections before using
            )
            
            self.inspector = inspect(self.engine)
            self.metadata = MetaData()
            self.metadata.reflect(bind=self.engine)
            
            # Discover all components
            tables = self._discover_tables()
            relationships = self._discover_relationships()
            indexes = self._discover_indexes()
            
            self.schema = {
                'tables': tables,
                'relationships': relationships,
                'indexes': indexes,
                'connection_info': self._get_connection_info()
            }
            
            logger.info(f"Schema discovery complete: {len(tables)} tables found")
            return self.schema
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {str(e)}")
            raise
    
    def _discover_tables(self) -> List[Dict[str, Any]]:
        """Discover all tables with their columns and metadata"""
        tables = []
        table_names = self.inspector.get_table_names()
        
        for table_name in table_names:
            table_info = {
                'name': table_name,
                'semantic_type': self._infer_table_type(table_name),
                'columns': self._discover_columns(table_name),
                'primary_keys': self.inspector.get_pk_constraint(table_name)['constrained_columns'],
                'row_count': self._get_row_count(table_name),
                'sample_data': self._get_sample_data(table_name)
            }
            tables.append(table_info)
        
        return tables
    
    def _discover_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Discover all columns for a given table"""
        columns = []
        column_info = self.inspector.get_columns(table_name)
        
        for col in column_info:
            column_data = {
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col['nullable'],
                'default': col.get('default'),
                'semantic_type': self._infer_column_type(col['name']),
                'primary_key': col['name'] in self.inspector.get_pk_constraint(table_name)['constrained_columns'],
                'foreign_key': None
            }
            
            # Check for foreign keys
            fks = self.inspector.get_foreign_keys(table_name)
            for fk in fks:
                if col['name'] in fk['constrained_columns']:
                    idx = fk['constrained_columns'].index(col['name'])
                    column_data['foreign_key'] = {
                        'table': fk['referred_table'],
                        'column': fk['referred_columns'][idx]
                    }
            
            columns.append(column_data)
        
        return columns
    
    def _discover_relationships(self) -> List[Dict[str, Any]]:
        """Discover relationships between tables"""
        relationships = []
        
        for table_name in self.inspector.get_table_names():
            fks = self.inspector.get_foreign_keys(table_name)
            
            for fk in fks:
                for i, col in enumerate(fk['constrained_columns']):
                    relationships.append({
                        'from_table': table_name,
                        'from_column': col,
                        'to_table': fk['referred_table'],
                        'to_column': fk['referred_columns'][i],
                        'type': 'foreign_key'
                    })
        
        # Infer implicit relationships based on naming conventions
        relationships.extend(self._infer_implicit_relationships())
        
        return relationships
    
    def _infer_implicit_relationships(self) -> List[Dict[str, Any]]:
        """Infer relationships based on column naming patterns"""
        implicit_rels = []
        tables = self.inspector.get_table_names()
        
        for table in tables:
            columns = self.inspector.get_columns(table)
            
            for col in columns:
                col_name = col['name'].lower()
                
                # Look for patterns like dept_id, department_id, etc.
                for other_table in tables:
                    if table == other_table:
                        continue
                    
                    # Check if column name suggests relationship
                    other_table_singular = other_table.rstrip('s')
                    if (f"{other_table}_id" in col_name or 
                        f"{other_table_singular}_id" in col_name):
                        
                        implicit_rels.append({
                            'from_table': table,
                            'from_column': col['name'],
                            'to_table': other_table,
                            'to_column': 'id',  # Assumption
                            'type': 'inferred'
                        })
        
        return implicit_rels
    
    def _discover_indexes(self) -> Dict[str, List[str]]:
        """Discover indexes for query optimization"""
        indexes = {}
        
        for table_name in self.inspector.get_table_names():
            table_indexes = self.inspector.get_indexes(table_name)
            indexes[table_name] = [
                {
                    'name': idx['name'],
                    'columns': idx['column_names'],
                    'unique': idx['unique']
                }
                for idx in table_indexes
            ]
        
        return indexes
    
    def _infer_table_type(self, table_name: str) -> str:
        """Infer semantic meaning of table based on name"""
        table_lower = table_name.lower()
        
        for semantic_type, patterns in self.table_patterns.items():
            for pattern in patterns:
                if pattern in table_lower:
                    return semantic_type
        
        return 'unknown'
    
    def _infer_column_type(self, column_name: str) -> str:
        """Infer semantic meaning of column based on name"""
        col_lower = column_name.lower()
        
        for semantic_type, patterns in self.column_patterns.items():
            for pattern in patterns:
                if pattern in col_lower:
                    return semantic_type
        
        return 'general'
    
    def _get_row_count(self, table_name: str) -> int:
        """Get approximate row count for table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {str(e)}")
            return 0
    
    def _get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict]:
        """Get sample rows for context understanding"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
                columns = result.keys()
                rows = result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.warning(f"Could not get sample data for {table_name}: {str(e)}")
            return []
    
    def _get_connection_info(self) -> Dict[str, Any]:
        """Get database connection metadata"""
        return {
            'dialect': self.engine.dialect.name,
            'driver': self.engine.driver,
            'database': self.engine.url.database
        }
    
    def map_natural_language_to_schema(self, query: str) -> Dict[str, Any]:
        """
        Map natural language terms to actual database schema elements.
        
        Args:
            query: Natural language query string
            
        Returns:
            Mapping of NL terms to database objects
        """
        if not self.schema:
            raise ValueError("Schema not discovered. Call analyze_database first.")
        
        query_lower = query.lower()
        mappings = {
            'tables': [],
            'columns': {},
            'conditions': []
        }
        
        # Match tables
        for table in self.schema['tables']:
            table_name = table['name'].lower()
            semantic_type = table['semantic_type']
            
            # Direct match
            if table_name in query_lower:
                mappings['tables'].append(table['name'])
            # Semantic match
            elif semantic_type in query_lower:
                mappings['tables'].append(table['name'])
            # Pattern match
            else:
                for pattern in self.table_patterns.get(semantic_type, []):
                    if pattern in query_lower:
                        mappings['tables'].append(table['name'])
                        break
        
        # Match columns
        for table in self.schema['tables']:
            if table['name'] in mappings['tables']:
                mappings['columns'][table['name']] = []
                
                for col in table['columns']:
                    col_name = col['name'].lower()
                    semantic_type = col['semantic_type']
                    
                    # Check various matching strategies
                    if (col_name in query_lower or 
                        semantic_type in query_lower or
                        any(p in query_lower for p in self.column_patterns.get(semantic_type, []))):
                        mappings['columns'][table['name']].append(col['name'])
        
        return mappings
    
    def get_table_by_semantic_type(self, semantic_type: str) -> Optional[Dict]:
        """Find table by its semantic type"""
        for table in self.schema.get('tables', []):
            if table['semantic_type'] == semantic_type:
                return table
        return None
    
    def get_column_suggestions(self, table_name: str, semantic_type: str) -> List[str]:
        """Get column suggestions for a specific semantic type"""
        for table in self.schema.get('tables', []):
            if table['name'] == table_name:
                return [
                    col['name'] 
                    for col in table['columns'] 
                    if col['semantic_type'] == semantic_type
                ]
        return []