"""
Database utilities for the Bitcoin Quantum Vulnerability Scanner.
Provides connection management and common database operations.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

from .config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and provides utility methods."""
    
    def __init__(self):
        self.connection = None
        self._test_connection()
    
    def _test_connection(self) -> bool:
        """Test database connection and create database if it doesn't exist."""
        try:
            # First try to connect to PostgreSQL server
            conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database='postgres'  # Connect to default database first
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Check if our database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.DB_NAME,))
            if not cursor.fetchone():
                logger.info(f"Creating database {config.DB_NAME}")
                cursor.execute(f"CREATE DATABASE {config.DB_NAME}")
            
            cursor.close()
            conn.close()
            
            # Now connect to our database
            self.connection = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME
            )
            self.connection.autocommit = False
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    @contextmanager
    def get_cursor(self, commit: bool = True):
        """Context manager for database cursors."""
        if not self.connection or self.connection.closed:
            if not self._test_connection():
                raise Exception("Cannot establish database connection")
        
        if self.connection:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
                if commit:
                    self.connection.commit()
            except Exception as e:
                self.connection.rollback()
                logger.error(f"Database operation failed: {e}")
                raise
            finally:
                cursor.close()
        else:
            raise Exception("No database connection available")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> int:
        """Execute a command and return the number of affected rows."""
        with self.get_cursor() as cursor:
            cursor.execute(command, params)
            return cursor.rowcount
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """
        result = self.execute_query(query, (table_name,))
        return result[0]['exists'] if result else False
    
    def get_table_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        if not self.table_exists(table_name):
            return 0
        
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def close(self):
        """Close the database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Database connection closed")


# Global database manager instance
db_manager = DatabaseManager() 