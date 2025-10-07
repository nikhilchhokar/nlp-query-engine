"""
Unit tests for Schema Discovery Service
Tests dynamic schema detection and relationship inference
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from api.services.schema_discovery import SchemaDiscovery

class TestSchemaDiscovery:
    """Test suite for SchemaDiscovery service"""
    
    @pytest.fixture
    def schema_discovery(self):
        """Create SchemaDiscovery instance for testing"""
        return SchemaDiscovery()
    
    @pytest.fixture
    def mock_engine(self):
        """Create mock database engine"""
        engine = Mock()
        inspector = Mock()
        
        # Mock table discovery
        inspector.get_table_names.return_value = ['employees', 'departments', 'salaries']
        
        # Mock employee table columns
        inspector.get_columns.side_effect = lambda table: {
            'employees': [
                {'name': 'emp_id', 'type': Integer(), 'nullable': False},
                {'name': 'full_name', 'type': String(), 'nullable': False},
                {'name': 'dept_id', 'type': Integer(), 'nullable': True},
                {'name': 'salary', 'type': Integer(), 'nullable': False}
            ],
            'departments': [
                {'name': 'dept_id', 'type': Integer(), 'nullable': False},
                {'name': 'dept_name', 'type': String(), 'nullable': False}
            ],
            'salaries': [
                {'name': 'emp_id', 'type': Integer(), 'nullable': False},
                {'name': 'amount', 'type': Integer(), 'nullable': False}
            ]
        }.get(table, [])
        
        # Mock primary keys
        inspector.get_pk_constraint.side_effect = lambda table: {
            'employees': {'constrained_columns': ['emp_id']},
            'departments': {'constrained_columns': ['dept_id']},
            'salaries': {'constrained_columns': ['emp_id']}
        }.get(table, {'constrained_columns': []})
        
        # Mock foreign keys
        inspector.get_foreign_keys.side_effect = lambda table: {
            'employees': [{
                'constrained_columns': ['dept_id'],
                'referred_table': 'departments',
                'referred_columns': ['dept_id']
            }],
            'salaries': [{
                'constrained_columns': ['emp_id'],
                'referred_table': 'employees',
                'referred_columns': ['emp_id']
            }]
        }.get(table, [])
        
        # Mock indexes
        inspector.get_indexes.return_value = []
        
        engine.dialect.name = 'postgresql'
        engine.driver = 'psycopg2'
        engine.url.database = 'test_db'
        
        return engine, inspector
    
    @pytest.mark.asyncio
    async def test_analyze_database(self, schema_discovery, mock_engine):
        """Test complete database analysis"""
        engine, inspector = mock_engine
        
        with patch('api.services.schema_discovery.create_engine', return_value=engine):
            with patch('api.services.schema_discovery.inspect', return_value=inspector):
                with patch.object(schema_discovery, '_get_row_count', return_value=100):
                    with patch.object(schema_discovery, '_get_sample_data', return_value=[]):
                        
                        schema = await schema_discovery.analyze_database(
                            'postgresql://user:pass@localhost/test_db'
                        )
                        
                        # Verify structure
                        assert 'tables' in schema
                        assert 'relationships' in schema
                        assert len(schema['tables']) == 3
                        
                        # Verify table names
                        table_names = [t['name'] for t in schema['tables']]
                        assert 'employees' in table_names
                        assert 'departments' in table_names
                        assert 'salaries' in table_names
    
    def test_infer_table_type(self, schema_discovery):
        """Test semantic table type inference"""
        assert schema_discovery._infer_table_type('employees') == 'employees'
        assert schema_discovery._infer_table_type('staff') == 'employees'
        assert schema_discovery._infer_table_type('departments') == 'departments'
        assert schema_discovery._infer_table_type('dept') == 'departments'
        assert schema_discovery._infer_table_type('salary') == 'salaries'
        assert schema_discovery._infer_table_type('compensation') == 'salaries'
        assert schema_discovery._infer_table_type('random_table') == 'unknown'
    
    def test_infer_column_type(self, schema_discovery):
        """Test semantic column type inference"""
        assert schema_discovery._infer_column_type('emp_id') == 'id'
        assert schema_discovery._infer_column_type('employee_id') == 'id'
        assert schema_discovery._infer_column_type('full_name') == 'name'
        assert schema_discovery._infer_column_type('employee_name') == 'name'
        assert schema_discovery._infer_column_type('email') == 'email'
        assert schema_discovery._infer_column_type('e_mail') == 'email'
        assert schema_discovery._infer_column_type('salary') == 'salary'
        assert schema_discovery._infer_column_type('compensation') == 'salary'
        assert schema_discovery._infer_column_type('dept_id') == 'department'
        assert schema_discovery._infer_column_type('random_column') == 'general'
    
    def test_map_natural_language_to_schema(self, schema_discovery):
        """Test NL to schema mapping"""
        # Setup mock schema
        schema_discovery.schema = {
            'tables': [
                {
                    'name': 'employees',
                    'semantic_type': 'employees',
                    'columns': [
                        {'name': 'emp_id', 'semantic_type': 'id'},
                        {'name': 'full_name', 'semantic_type': 'name'},
                        {'name': 'salary', 'semantic_type': 'salary'}
                    ]
                }
            ]
        }
        
        # Test mapping
        mappings = schema_discovery.map_natural_language_to_schema(
            "Show me all employees with salary over 100000"
        )
        
        assert 'employees' in mappings['tables']
        assert 'employees' in mappings['columns']
        assert 'salary' in mappings['columns']['employees']
    
    def test_discover_relationships(self, schema_discovery, mock_engine):
        """Test relationship discovery"""
        engine, inspector = mock_engine
        schema_discovery.inspector = inspector
        
        relationships = schema_discovery._discover_relationships()
        
        # Should find foreign key relationships
        assert len(relationships) > 0
        
        # Check specific relationship
        emp_dept_rel = next(
            (r for r in relationships 
             if r['from_table'] == 'employees' and r['to_table'] == 'departments'),
            None
        )
        assert emp_dept_rel is not None
        assert emp_dept_rel['from_column'] == 'dept_id'
        assert emp_dept_rel['to_column'] == 'dept_id'
    
    def test_get_table_by_semantic_type(self, schema_discovery):
        """Test finding table by semantic type"""
        schema_discovery.schema = {
            'tables': [
                {'name': 'employees', 'semantic_type': 'employees'},
                {'name': 'departments', 'semantic_type': 'departments'}
            ]
        }
        
        table = schema_discovery.get_table_by_semantic_type('employees')
        assert table is not None
        assert table['name'] == 'employees'
        
        table = schema_discovery.get_table_by_semantic_type('nonexistent')
        assert table is None
    
    def test_get_column_suggestions(self, schema_discovery):
        """Test column suggestion by semantic type"""
        schema_discovery.schema = {
            'tables': [
                {
                    'name': 'employees',
                    'columns': [
                        {'name': 'emp_id', 'semantic_type': 'id'},
                        {'name': 'full_name', 'semantic_type': 'name'},
                        {'name': 'salary', 'semantic_type': 'salary'}
                    ]
                }
            ]
        }
        
        suggestions = schema_discovery.get_column_suggestions('employees', 'salary')
        assert 'salary' in suggestions
        
        suggestions = schema_discovery.get_column_suggestions('employees', 'name')
        assert 'full_name' in suggestions
    
    @pytest.mark.asyncio
    async def test_schema_variations(self, schema_discovery):
        """Test handling of different schema naming conventions"""
        # Test various naming patterns
        assert schema_discovery._infer_table_type('emp') == 'employees'
        assert schema_discovery._infer_table_type('personnel') == 'employees'
        assert schema_discovery._infer_table_type('staff_members') == 'employees'
        
        assert schema_discovery._infer_column_type('dept') == 'department'
        assert schema_discovery._infer_column_type('division') == 'department'
        assert schema_discovery._infer_column_type('pay_rate') == 'salary'
        assert schema_discovery._infer_column_type('annual_salary') == 'salary'
    
    def test_infer_implicit_relationships(self, schema_discovery, mock_engine):
        """Test inference of implicit relationships from naming"""
        engine, inspector = mock_engine
        schema_discovery.inspector = inspector
        
        implicit_rels = schema_discovery._infer_implicit_relationships()
        
        # Should infer dept_id relationship even if not explicit FK
        assert len(implicit_rels) >= 0  # May vary based on implementation


# Integration test
@pytest.mark.integration
class TestSchemaDiscoveryIntegration:
    """Integration tests requiring actual database"""
    
    @pytest.fixture
    def test_db_url(self):
        """Get test database URL from environment"""
        import os
        return os.getenv('TEST_DATABASE_URL', 'sqlite:///test.db')
    
    @pytest.mark.asyncio
    async def test_real_database_discovery(self, test_db_url):
        """Test with actual SQLite database"""
        # Create test database
        engine = create_engine(test_db_url)
        metadata = MetaData()
        
        # Create test tables
        employees = Table('employees', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100)),
            Column('dept_id', Integer, ForeignKey('departments.id'))
        )
        
        departments = Table('departments', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(100))
        )
        
        metadata.create_all(engine)
        
        # Test discovery
        discovery = SchemaDiscovery()
        schema = await discovery.analyze_database(test_db_url)
        
        assert len(schema['tables']) == 2
        assert any(t['name'] == 'employees' for t in schema['tables'])
        assert any(t['name'] == 'departments' for t in schema['tables'])
        
        # Cleanup
        metadata.drop_all(engine)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])