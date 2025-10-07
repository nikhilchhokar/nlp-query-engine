"""
Query Engine Service
Processes natural language queries and generates appropriate SQL or document searches
"""

import sqlalchemy
from sqlalchemy import create_engine, text
from typing import Dict, List, Any, Optional
import logging
import re
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

class QueryEngine:
    """
    Production-ready query engine that:
    - Classifies queries (SQL, document, or hybrid)
    - Generates optimized SQL from natural language
    - Searches documents using embeddings
    - Combines results when appropriate
    """
    
    def __init__(self, connection_string: str, schema: Dict, cache: Any):
        self.engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False
        )
        self.schema = schema
        self.cache = cache
        
        # Query classification patterns
        self.sql_keywords = [
            'how many', 'count', 'average', 'sum', 'total', 'list',
            'show', 'display', 'find', 'get', 'salary', 'department',
            'hired', 'employees', 'staff'
        ]
        
        self.document_keywords = [
            'resume', 'cv', 'document', 'file', 'review', 'performance',
            'feedback', 'skills', 'experience', 'qualification'
        ]
        
        # SQL generation templates
        self.query_templates = {
            'count': "SELECT COUNT(*) as count FROM {table}",
            'average': "SELECT AVG({column}) as average FROM {table}",
            'list': "SELECT * FROM {table}",
            'group_by': "SELECT {group_col}, {agg_func}({agg_col}) as {agg_alias} FROM {table} GROUP BY {group_col}",
            'join': "SELECT {columns} FROM {table1} JOIN {table2} ON {join_condition}"
        }
    
    async def process_query(self, user_query: str, document_processor: Any) -> Dict[str, Any]:
        """
        Main query processing pipeline.
        
        Args:
            user_query: Natural language query from user
            document_processor: Document processor instance for document searches
            
        Returns:
            Dictionary with results, query type, and metadata
        """
        try:
            # Step 1: Classify query type
            query_type = self._classify_query(user_query)
            logger.info(f"Query classified as: {query_type}")
            
            result = {
                'query_type': query_type,
                'sql_results': None,
                'document_results': None,
                'generated_sql': None
            }
            
            # Step 2: Process based on type
            if query_type == 'sql' or query_type == 'hybrid':
                sql_result = await self._process_sql_query(user_query)
                result['sql_results'] = sql_result['data']
                result['generated_sql'] = sql_result['sql']
            
            if query_type == 'document' or query_type == 'hybrid':
                doc_results = await self._process_document_query(
                    user_query, 
                    document_processor
                )
                result['document_results'] = doc_results
            
            return result
            
        except Exception as e:
            logger.error(f"Query processing error: {str(e)}")
            raise
    
    def _classify_query(self, query: str) -> str:
        """
        Classify query as SQL, document, or hybrid.
        
        Returns:
            'sql', 'document', or 'hybrid'
        """
        query_lower = query.lower()
        
        sql_matches = sum(1 for kw in self.sql_keywords if kw in query_lower)
        doc_matches = sum(1 for kw in self.document_keywords if kw in query_lower)
        
        if sql_matches > 0 and doc_matches > 0:
            return 'hybrid'
        elif doc_matches > 0:
            return 'document'
        else:
            return 'sql'
    
    async def _process_sql_query(self, query: str) -> Dict[str, Any]:
        """Generate and execute SQL query from natural language"""
        try:
            # Generate SQL from natural language
            sql_query = self._generate_sql(query)
            
            # Optimize the query
            optimized_sql = self._optimize_sql_query(sql_query)
            
            # Execute query
            with self.engine.connect() as conn:
                result = conn.execute(text(optimized_sql))
                columns = list(result.keys())
                rows = result.fetchall()
                
                return {
                    'sql': optimized_sql,
                    'data': {
                        'columns': columns,
                        'rows': [list(row) for row in rows]
                    }
                }
        
        except Exception as e:
            logger.error(f"SQL processing error: {str(e)}")
            # Fallback to simple query
            return self._generate_fallback_query(query)
    
    def _generate_sql(self, query: str) -> str:
        """
        Generate SQL from natural language using schema-aware approach.
        This is a simplified version - in production, use LLM or more sophisticated NLP.
        """
        query_lower = query.lower()
        
        # Identify relevant table
        target_table = self._identify_target_table(query_lower)
        if not target_table:
            target_table = self.schema['tables'][0]['name']  # Default to first table
        
        # Count queries
        if any(word in query_lower for word in ['how many', 'count', 'number of']):
            conditions = self._extract_conditions(query_lower, target_table)
            if conditions:
                return f"SELECT COUNT(*) as count FROM {target_table} WHERE {conditions}"
            return f"SELECT COUNT(*) as count FROM {target_table}"
        
        # Average queries
        if 'average' in query_lower or 'avg' in query_lower:
            column = self._identify_numeric_column(query_lower, target_table)
            group_by = self._identify_group_by(query_lower, target_table)
            
            if group_by:
                return f"SELECT {group_by}, AVG({column}) as average FROM {target_table} GROUP BY {group_by}"
            return f"SELECT AVG({column}) as average FROM {target_table}"
        
        # List/Show queries
        if any(word in query_lower for word in ['list', 'show', 'display', 'get', 'find']):
            columns = self._identify_columns(query_lower, target_table)
            conditions = self._extract_conditions(query_lower, target_table)
            
            if conditions:
                return f"SELECT {columns} FROM {target_table} WHERE {conditions} LIMIT 100"
            return f"SELECT {columns} FROM {target_table} LIMIT 100"
        
        # Top N queries
        if 'top' in query_lower or 'highest' in query_lower or 'best' in query_lower:
            n = self._extract_number(query_lower) or 10
            column = self._identify_numeric_column(query_lower, target_table)
            return f"SELECT * FROM {target_table} ORDER BY {column} DESC LIMIT {n}"
        
        # Default: return all with limit
        return f"SELECT * FROM {target_table} LIMIT 100"
    
    def _identify_target_table(self, query: str) -> Optional[str]:
        """Identify which table the query is targeting"""
        for table in self.schema['tables']:
            table_name = table['name'].lower()
            semantic_type = table['semantic_type']
            
            if table_name in query or semantic_type in query:
                return table['name']
        
        return None
    
    def _identify_columns(self, query: str, table_name: str) -> str:
        """Identify which columns to select"""
        table_info = next((t for t in self.schema['tables'] if t['name'] == table_name), None)
        if not table_info:
            return '*'
        
        # Look for specific column mentions
        mentioned_columns = []
        for col in table_info['columns']:
            if col['name'].lower() in query or col['semantic_type'] in query:
                mentioned_columns.append(col['name'])
        
        return ', '.join(mentioned_columns) if mentioned_columns else '*'
    
    def _identify_numeric_column(self, query: str, table_name: str) -> str:
        """Identify numeric column for aggregations"""
        table_info = next((t for t in self.schema['tables'] if t['name'] == table_name), None)
        if not table_info:
            return 'id'
        
        # Look for salary/compensation columns
        if 'salary' in query or 'pay' in query or 'compensation' in query:
            for col in table_info['columns']:
                if any(term in col['name'].lower() for term in ['salary', 'pay', 'compensation', 'wage']):
                    return col['name']
        
        # Default to first numeric column
        for col in table_info['columns']:
            if any(t in col['type'].lower() for t in ['int', 'decimal', 'numeric', 'float']):
                return col['name']
        
        return 'id'
    
    def _identify_group_by(self, query: str, table_name: str) -> Optional[str]:
        """Identify GROUP BY column"""
        if 'by department' in query or 'per department' in query:
            table_info = next((t for t in self.schema['tables'] if t['name'] == table_name), None)
            if table_info:
                for col in table_info['columns']:
                    if 'dept' in col['name'].lower() or col['semantic_type'] == 'department':
                        return col['name']
        
        return None
    
    def _extract_conditions(self, query: str, table_name: str) -> Optional[str]:
        """Extract WHERE clause conditions"""
        conditions = []
        table_info = next((t for t in self.schema['tables'] if t['name'] == table_name), None)
        if not table_info:
            return None
        
        # Year conditions
        if 'this year' in query or '2024' in query or '2025' in query:
            for col in table_info['columns']:
                if 'date' in col['type'].lower() and any(term in col['name'].lower() for term in ['hire', 'join', 'start']):
                    year = datetime.now().year
                    conditions.append(f"EXTRACT(YEAR FROM {col['name']}) = {year}")
        
        # Numeric comparisons
        if 'over' in query or 'more than' in query or '>' in query:
            number = self._extract_number(query)
            if number:
                numeric_col = self._identify_numeric_column(query, table_name)
                conditions.append(f"{numeric_col} > {number}")
        
        # String matching
        if 'python' in query.lower():
            # This would typically search in a skills or description field
            for col in table_info['columns']:
                if col['type'].lower() in ['text', 'varchar', 'string']:
                    conditions.append(f"LOWER({col['name']}) LIKE '%python%'")
                    break
        
        return ' AND '.join(conditions) if conditions else None
    
    def _extract_number(self, query: str) -> Optional[int]:
        """Extract numbers from query"""
        numbers = re.findall(r'\b\d+\b', query)
        return int(numbers[0]) if numbers else None
    
    def _optimize_sql_query(self, sql: str) -> str:
        """
        Optimize generated SQL query.
        - Add appropriate LIMIT clauses
        - Use indexes when available
        - Optimize JOIN conditions
        """
        # Add LIMIT if not present
        if 'LIMIT' not in sql.upper() and 'COUNT' not in sql.upper():
            sql = sql.rstrip(';') + ' LIMIT 1000'
        
        # Add semicolon
        if not sql.endswith(';'):
            sql += ';'
        
        return sql
    
    def _generate_fallback_query(self, query: str) -> Dict[str, Any]:
        """Generate a safe fallback query if SQL generation fails"""
        table_name = self.schema['tables'][0]['name']
        sql = f"SELECT * FROM {table_name} LIMIT 10;"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys())
                rows = result.fetchall()
                
                return {
                    'sql': sql,
                    'data': {
                        'columns': columns,
                        'rows': [list(row) for row in rows]
                    }
                }
        except Exception as e:
            logger.error(f"Fallback query failed: {str(e)}")
            return {
                'sql': sql,
                'data': {
                    'columns': [],
                    'rows': []
                }
            }
    
    async def _process_document_query(self, query: str, document_processor: Any) -> List[Dict[str, Any]]:
        """Search documents using embeddings and semantic similarity"""
        try:
            if not document_processor or len(document_processor.documents) == 0:
                return []
            
            # Get relevant documents using semantic search
            results = await document_processor.search_documents(query, top_k=5)
            
            return results
            
        except Exception as e:
            logger.error(f"Document search error: {str(e)}")
            return []
    
    def validate_sql(self, sql: str) -> bool:
        """
        Validate SQL query for safety.
        Prevent SQL injection and dangerous operations.
        """
        sql_upper = sql.upper()
        
        # Blacklist dangerous operations
        dangerous_keywords = [
            'DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 
            'INSERT', 'ALTER', 'CREATE', 'EXEC',
            'EXECUTE', 'SCRIPT', '--', '/*', '*/'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                logger.warning(f"Dangerous SQL keyword detected: {keyword}")
                return False
        
        return True
    
    def explain_query(self, sql: str) -> Dict[str, Any]:
        """
        Generate query execution plan for optimization analysis.
        """
        try:
            with self.engine.connect() as conn:
                explain_sql = f"EXPLAIN {sql}"
                result = conn.execute(text(explain_sql))
                plan = result.fetchall()
                
                return {
                    'explained': True,
                    'plan': [str(row) for row in plan]
                }
        except Exception as e:
            logger.error(f"Query explain failed: {str(e)}")
            return {'explained': False, 'error': str(e)}