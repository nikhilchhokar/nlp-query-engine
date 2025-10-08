"""
Query Engine Service - FIXED VERSION
Processes natural language queries and generates appropriate SQL
"""

import sqlalchemy  
from sqlalchemy import create_engine, text
from typing import Dict, List, Any, Optional
import logging
import re
from datetime import datetime
from fastapi import HTTPException

logger = logging.getLogger(__name__)

class QueryEngine:
    """Query engine with improved SQL generation"""
    
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
    
    async def process_query(self, query, document_processor):
        logger.info(f"Processing query: {query}")
        """Main query processing pipeline"""
        try:
            query_type = self._classify_query(query)
            logger.info(f"Query: '{query}' classified as: {query_type}")
            
            result = {
                'query_type': query_type,
                'sql_results': None,
                'document_results': None,
                'generated_sql': None
            }
            
            if query_type == 'sql' or query_type == 'hybrid':
                sql_result = await self._process_sql_query(query)
                result['sql_results'] = sql_result['data']
                result['generated_sql'] = sql_result['sql']
                logger.info(f"Generated SQL: {sql_result['sql']}")
            
            if query_type == 'document' or query_type == 'hybrid':
                doc_results = await self._process_document_query(query, document_processor)
                result['document_results'] = doc_results
        
            return result
        
        except Exception as e:
            logger.error(f"Query processing error: {str(e)}")
            raise
    
    def _classify_query(self, query: str) -> str:
        """Classify query type"""
        query_lower = query.lower()
        
        doc_keywords = ['resume', 'cv', 'document', 'file', 'review', 'performance']
        if any(kw in query_lower for kw in doc_keywords):
            return 'document'
        
        return 'sql'
    
    async def _process_sql_query(self, query: str) -> Dict[str, Any]:
        """Generate and execute SQL query"""
        try:
            sql = self._generate_sql(query)
            logger.info(f"Generated SQL: {sql}")
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = result.fetchall()
                columns = result.keys()
                logger.info(f"SQL returned {len(rows)} rows")
                return {
                    "sql": sql,
                    "data": {
                        "columns": list(columns),
                        "rows": [list(row) for row in rows]
                    }
                }
        except Exception as e:
            logger.error(f"SQL query failed: {e}")
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")
    def _generate_sql(self, query: str) -> str:
        query_lower = query.lower()

        if not self.schema.get('tables') or len(self.schema['tables']) == 0:
            logger.error("No tables found in schema.")
            raise HTTPException(status_code=400, detail="No tables found in schema.")

        """Generate SQL from natural language - FIXED AND EXPANDED"""
        logger.info(f"Schema: {self.schema}")

        # Identify employee and department tables
        emp_table = None
        dept_table = None

        for table in self.schema['tables']:
            name_lower = table['name'].lower()
            if any(key in name_lower for key in ['emp', 'staff', 'personnel', 'worker']):
                emp_table = table['name']
            if any(key in name_lower for key in ['dept', 'department', 'division', 'team']):
                dept_table = table['name']

        # fallback in case schema detection fails
        if not emp_table:
            emp_table = self.schema['tables'][0]['name']
        if not dept_table:
            dept_table = 'departments'  # assume standard naming if missing

        logger.info(f"Using tables - Employee: {emp_table}, Department: {dept_table}")

        if 'average salary by department' in query_lower or (
            'average' in query_lower and 'department' in query_lower
        ):
            salary_col = 'annual_salary'
            for table in self.schema['tables']:
                if table['name'] == emp_table:
                    for col in table['columns']:
                        if 'salary' in col['name'].lower() or 'pay' in col['name'].lower():
                            salary_col = col['name']
                            break

            return f"""
                SELECT d.dept_name AS department,
                    ROUND(AVG(e.{salary_col}), 2) AS average_salary
                FROM {emp_table} e
                JOIN {dept_table} d ON e.dept_id = d.dept_id
                GROUP BY d.dept_name
                ORDER BY average_salary DESC
            """

        if 'aws' in query_lower and any(word in query_lower for word in ['certification', 'certifications', 'certified']):
            cert_col = None
            for table in self.schema['tables']:
                if table['name'] == emp_table:
                    for col in table['columns']:
                        if 'cert' in col['name'].lower() or 'skill' in col['name'].lower():
                            cert_col = col['name']
                            break
            if not cert_col:
                cert_col = 'certifications'

            return f"""
                SELECT emp_id, full_name, position, annual_salary, {cert_col}
                FROM {emp_table}
                WHERE LOWER({cert_col}) LIKE '%aws%'
                ORDER BY annual_salary DESC
                LIMIT 100
            """

      
        if (
            'python' in query_lower
            and 'engineering' in query_lower
            and any(word in query_lower for word in ['over', 'above', '>', 'more than'])
        ):
            salary_threshold = self._extract_number(query_lower) or 100000
            salary_col = 'annual_salary'

            # detect salary column dynamically
            for table in self.schema['tables']:
                if table['name'] == emp_table:
                    for col in table['columns']:
                        if 'salary' in col['name'].lower():
                            salary_col = col['name']
                            break

            return f"""
                SELECT e.emp_id, e.full_name, e.position, e.{salary_col} AS salary, d.dept_name
                FROM {emp_table} e
                JOIN {dept_table} d ON e.dept_id = d.dept_id
                WHERE d.dept_name = 'Engineering'
                AND LOWER(e.position) LIKE '%python%'
                AND e.{salary_col} > {salary_threshold}
                ORDER BY e.{salary_col} DESC
            """

        if any(word in query_lower for word in ['how many', 'count', 'number of', 'total']):
            if 'by department' in query_lower or 'per department' in query_lower:
                return f"""
                    SELECT d.dept_name AS department, COUNT(e.emp_id) AS employee_count
                    FROM {emp_table} e
                    JOIN {dept_table} d ON e.dept_id = d.dept_id
                    GROUP BY d.dept_name
                    ORDER BY employee_count DESC
                """
            return f"SELECT COUNT(*) AS total_employees FROM {emp_table}"

        if 'average' in query_lower or 'avg' in query_lower:
            salary_col = 'annual_salary'
            for table in self.schema['tables']:
                if table['name'] == emp_table:
                    for col in table['columns']:
                        if 'salary' in col['name'].lower() or 'pay' in col['name'].lower():
                            salary_col = col['name']
                            break

            if 'by department' in query_lower or 'per department' in query_lower:
                return f"""
                    SELECT d.dept_name AS department,
                        ROUND(AVG(e.{salary_col}), 2) AS average_salary
                    FROM {emp_table} e
                    JOIN {dept_table} d ON e.dept_id = d.dept_id
                    GROUP BY d.dept_name
                    ORDER BY average_salary DESC
                """

            return f"SELECT ROUND(AVG({salary_col}), 2) AS average_salary FROM {emp_table}"

       
        if 'department' in query_lower and any(word in query_lower for word in ['show', 'list', 'all', 'get']):
            return f"SELECT dept_id, dept_name FROM {dept_table} ORDER BY dept_name"

        if any(word in query_lower for word in ['highest', 'top', 'maximum', 'max']):
            n = self._extract_number(query_lower) or 10
            salary_col = 'annual_salary'
            for table in self.schema['tables']:
                if table['name'] == emp_table:
                    for col in table['columns']:
                        if 'salary' in col['name'].lower():
                            salary_col = col['name']
                            break

            return f"""
                SELECT e.emp_id, e.full_name, e.position,
                    e.{salary_col} AS salary, d.dept_name AS department
                FROM {emp_table} e
                LEFT JOIN {dept_table} d ON e.dept_id = d.dept_id
                ORDER BY e.{salary_col} DESC
                LIMIT {n}
            """

        for dept_name in ['engineering', 'sales', 'marketing']:
            if dept_name in query_lower:
                return f"""
                    SELECT e.emp_id, e.full_name, e.position, e.annual_salary, d.dept_name
                    FROM {emp_table} e
                    JOIN {dept_table} d ON e.dept_id = d.dept_id
                    WHERE d.dept_name = '{dept_name.capitalize()}'
                    LIMIT 100
                """

        if 'over' in query_lower or 'above' in query_lower or '>' in query_lower:
            number = self._extract_number(query_lower)
            if number:
                salary_col = 'annual_salary'
                return f"""
                    SELECT emp_id, full_name, position, {salary_col} AS salary
                    FROM {emp_table}
                    WHERE {salary_col} > {number}
                    ORDER BY {salary_col} DESC
                    LIMIT 100
                """

        if 'hired' in query_lower or 'joined' in query_lower:
            year = None
            if '2024' in query_lower:
                year = 2024
            elif '2025' in query_lower:
                year = 2025
            elif 'this year' in query_lower:
                year = datetime.now().year

            if year:
                date_col = 'join_date'
                for table in self.schema['tables']:
                    if table['name'] == emp_table:
                        for col in table['columns']:
                            if 'join' in col['name'].lower() or 'hire' in col['name'].lower():
                                date_col = col['name']
                                break

                return f"""
                    SELECT emp_id, full_name, position, {date_col} AS hire_date
                    FROM {emp_table}
                    WHERE strftime('%Y', {date_col}) = '{year}'
                    ORDER BY {date_col} DESC
                """

        return f"""
            SELECT e.emp_id, e.full_name, e.position, e.annual_salary, d.dept_name
            FROM {emp_table} e
            LEFT JOIN {dept_table} d ON e.dept_id = d.dept_id
            LIMIT 100
        """

    def _extract_number(self, query: str) -> Optional[int]:
        """Extract numbers from query"""
        # Look for written numbers
        number_words = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10
        }
        
        for word, num in number_words.items():
            if word in query.lower():
                return num
        
        # Look for digits
        numbers = re.findall(r'\b\d+\b', query)
        return int(numbers[0]) if numbers else None
    
    def _generate_fallback_query(self, query: str) -> Dict[str, Any]:
        """Generate safe fallback"""
        emp_table = self.schema['tables'][0]['name']
        sql = f"SELECT * FROM {emp_table} LIMIT 10"
        
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
            logger.error(f"Fallback failed: {str(e)}")
            return {
                'sql': sql,
                'data': {'columns': [], 'rows': []}
            }
    
    async def _process_document_query(self, query: str, document_processor: Any) -> List[Dict[str, Any]]:
        """Search documents"""
        try:
            if not document_processor or len(document_processor.documents) == 0:
                return []
            
            results = await document_processor.search_documents(query, top_k=5)
            return results
            
        except Exception as e:
            logger.error(f"Document search error: {str(e)}")
            return []
    
    def validate_sql(self, sql: str) -> bool:
        """Validate SQL for safety"""
        sql_upper = sql.upper()
        
        dangerous = ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
        
        for keyword in dangerous:
            if keyword in sql_upper:
                return False
        
        return True

        if not self.schema.get('tables') or len(self.schema['tables']) == 0:
            logger.error("No tables found in schema.")
            raise HTTPException(status_code=400, detail="No tables found in schema.")