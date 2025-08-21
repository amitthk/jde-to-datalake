"""
Schema Manager
Handles database schema management and evolution for the bakery operations system
"""
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from utility import get_db_connection
import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    def __init__(self):
        self.schema_table = 'schema_versions'
    
    def initialize_schema_tracking(self):
        """Initialize the schema tracking table if it doesn't exist"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create schema versions tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    schema_definition JSONB NOT NULL,
                    version_number INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by VARCHAR(100) DEFAULT 'system',
                    description TEXT,
                    UNIQUE(table_name, version_number)
                );
            """)
            
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_schema_versions_table_version 
                ON schema_versions(table_name, version_number DESC);
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Schema tracking initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize schema tracking: {str(e)}")
            raise
    
    def register_schema(self, table_name: str, schema_definition: Dict, description: str = None) -> int:
        """
        Register a new schema version for a table
        
        Args:
            table_name: Name of the table
            schema_definition: Schema definition as dictionary
            description: Optional description of changes
            
        Returns:
            Version number assigned to this schema
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get next version number
            cursor.execute("""
                SELECT COALESCE(MAX(version_number), 0) + 1 
                FROM schema_versions 
                WHERE table_name = %s
            """, (table_name,))
            
            next_version = cursor.fetchone()[0]
            
            # Insert new schema version
            cursor.execute("""
                INSERT INTO schema_versions 
                (table_name, schema_definition, version_number, description) 
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (table_name, json.dumps(schema_definition), next_version, description))
            
            schema_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Registered schema version {next_version} for table {table_name}")
            return next_version
            
        except Exception as e:
            logger.error(f"Failed to register schema: {str(e)}")
            raise
    
    def get_current_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get the current (latest) schema for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Schema definition dictionary or None if not found
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT schema_definition, version_number, created_at
                FROM schema_versions 
                WHERE table_name = %s 
                ORDER BY version_number DESC 
                LIMIT 1
            """, (table_name,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return {
                    'schema': json.loads(result[0]),
                    'version': result[1],
                    'created_at': result[2].isoformat()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get current schema: {str(e)}")
            raise
    
    def get_schema_history(self, table_name: str) -> List[Dict]:
        """
        Get the version history for a table schema
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of schema versions with metadata
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT version_number, schema_definition, created_at, created_by, description
                FROM schema_versions 
                WHERE table_name = %s 
                ORDER BY version_number DESC
            """, (table_name,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            history = []
            for row in results:
                history.append({
                    'version': row[0],
                    'schema': json.loads(row[1]),
                    'created_at': row[2].isoformat(),
                    'created_by': row[3],
                    'description': row[4]
                })
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get schema history: {str(e)}")
            raise
    
    def infer_schema_from_data(self, data: List[Dict]) -> Dict:
        """
        Infer schema from sample data
        
        Args:
            data: List of data records
            
        Returns:
            Inferred schema definition
        """
        if not data:
            return {}
        
        schema = {}
        sample_record = data[0]
        
        for field, value in sample_record.items():
            if value is None:
                # Check other records for non-null values
                for record in data[1:]:
                    if record.get(field) is not None:
                        value = record[field]
                        break
            
            if isinstance(value, int):
                schema[field] = {'type': 'integer', 'nullable': True}
            elif isinstance(value, float):
                schema[field] = {'type': 'float', 'nullable': True}
            elif isinstance(value, bool):
                schema[field] = {'type': 'boolean', 'nullable': True}
            elif isinstance(value, str):
                max_length = max(len(str(record.get(field, ''))) for record in data)
                schema[field] = {'type': 'string', 'max_length': max_length, 'nullable': True}
            elif isinstance(value, datetime):
                schema[field] = {'type': 'timestamp', 'nullable': True}
            elif isinstance(value, dict):
                schema[field] = {'type': 'json', 'nullable': True}
            elif isinstance(value, list):
                schema[field] = {'type': 'array', 'nullable': True}
            else:
                schema[field] = {'type': 'string', 'nullable': True}
        
        return {
            'fields': schema,
            'inferred_at': datetime.now().isoformat(),
            'sample_size': len(data)
        }
    
    def create_ddl_from_schema(self, table_name: str, schema_definition: Dict) -> str:
        """
        Generate CREATE TABLE DDL from schema definition
        
        Args:
            table_name: Name of the table
            schema_definition: Schema definition dictionary
            
        Returns:
            CREATE TABLE DDL statement
        """
        if 'fields' not in schema_definition:
            raise ValueError("Schema definition must contain 'fields' key")
        
        columns = []
        
        for field_name, field_def in schema_definition['fields'].items():
            field_type = field_def.get('type', 'string')
            nullable = field_def.get('nullable', True)
            
            # Map types to PostgreSQL types
            if field_type == 'integer':
                pg_type = 'INTEGER'
            elif field_type == 'float':
                pg_type = 'DECIMAL'
            elif field_type == 'boolean':
                pg_type = 'BOOLEAN'
            elif field_type == 'timestamp':
                pg_type = 'TIMESTAMP'
            elif field_type == 'json':
                pg_type = 'JSONB'
            elif field_type == 'array':
                pg_type = 'JSONB'
            else:  # string
                max_length = field_def.get('max_length', 255)
                if max_length > 255:
                    pg_type = 'TEXT'
                else:
                    pg_type = f'VARCHAR({max_length})'
            
            null_constraint = '' if nullable else ' NOT NULL'
            columns.append(f'    {field_name} {pg_type}{null_constraint}')
        
        # Add standard audit fields
        columns.extend([
            '    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            '    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        ])
        
        ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
{',\\n'.join(columns)}
);"""
        
        return ddl

# Global instance
schema_manager = SchemaManager()
