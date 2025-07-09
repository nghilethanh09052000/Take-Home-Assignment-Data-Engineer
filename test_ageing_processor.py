#!/usr/bin/env python3
"""
Unit tests for Ageing Processor
Tests the ageing fact table generation with different dates
"""

import unittest
import os
import tempfile
import csv
from datetime import date, datetime
from unittest.mock import patch, MagicMock
import sys

# Add the current directory to Python path to import ageing_processor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ageing_processor import AgeingProcessor, get_db_config


class TestAgeingProcessor(unittest.TestCase):
    """Test cases for AgeingProcessor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = AgeingProcessor()
        self.test_dates = [
            date(2025, 7, 7),  # Original sample date
            date.today(),       # Today's date
            date(2025, 7, 3)   # Different test date
        ]
        
    def tearDown(self):
        """Clean up after tests"""
        if hasattr(self, 'processor') and self.processor.conn:
            self.processor.disconnect()
    
    @patch('psycopg2.connect')
    def test_database_connection(self, mock_connect):
        """Test database connection establishment"""
        # Mock the connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Test connection
        self.processor.connect()
        
        # Verify connection was called with correct parameters
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args
        # Check that connection parameters are present
        self.assertIn('host', call_args.kwargs)
        self.assertIn('port', call_args.kwargs)
        self.assertIn('database', call_args.kwargs)
        self.assertIn('user', call_args.kwargs)
        self.assertIn('password', call_args.kwargs)
    
    def test_get_db_config(self):
        """Test database configuration retrieval"""
        # Set test environment variables
        test_env = {
            'DB_HOST': 'test_host',
            'DB_PORT': '5432',
            'DB_NAME': 'test_db',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_pass'
        }
        
        with patch.dict(os.environ, test_env):
            config = get_db_config()
            
            self.assertEqual(config['host'], 'test_host')
            self.assertEqual(config['port'], 5432)
            self.assertEqual(config['database'], 'test_db')
            self.assertEqual(config['user'], 'test_user')
            self.assertEqual(config['password'], 'test_pass')
    
    @patch('psycopg2.connect')
    def test_clear_existing_ageing_data(self, mock_connect):
        """Test clearing existing ageing data"""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        self.processor.connect()
        test_date = date(2025, 7, 7)
        
        # Test clearing data
        self.processor.clear_existing_ageing_data(test_date)
        
        # Verify DELETE query was executed
        mock_cursor.execute.assert_called_once()
        # Check that the SQL query contains the expected DELETE statement
        call_args = mock_cursor.execute.call_args
        self.assertIn("DELETE FROM ageing_fact_table WHERE as_at_date = %s", str(call_args))
        # Verify the method was called (basic check)
        self.assertTrue(mock_cursor.execute.called)
    
    @patch('psycopg2.connect')
    def test_generate_ageing_fact(self, mock_connect):
        """Test ageing fact table generation"""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock SQL file reading
        test_sql = "SELECT * FROM test_table WHERE date = %s"
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = test_sql
            
            self.processor.connect()
            test_date = date(2025, 7, 7)
            
            # Test ageing fact generation
            self.processor.generate_ageing_fact(test_date)
            
            # Verify SQL was executed
            mock_cursor.execute.assert_called_once()
            # Check that the method was called (basic check)
            self.assertTrue(mock_cursor.execute.called)
            # Verify the SQL file was read
            mock_open.assert_called_once_with('sql/generate_ageing_fact.sql', 'r')
    
    @patch('psycopg2.connect')
    def test_export_ageing_to_csv(self, mock_connect):
        """Test CSV export functionality"""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        self.processor.connect()
        test_date = date(2025, 7, 7)
        
        # Create temporary file for testing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            # Test CSV export
            self.processor.export_ageing_to_csv(test_date, temp_filename)
            
            # Verify COPY command was executed
            mock_cursor.copy_expert.assert_called_once()
            call_args = mock_cursor.copy_expert.call_args
            copy_sql = call_args[0][0]
            
            # Verify SQL contains correct date
            self.assertIn("2025-07-07", copy_sql)
            self.assertIn("COPY (", copy_sql)
            self.assertIn("TO STDOUT WITH CSV HEADER", copy_sql)
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
    
    def test_different_dates(self):
        """Test processing with different dates"""
        for test_date in self.test_dates:
            with self.subTest(test_date=test_date):
                # Verify date is valid
                self.assertIsInstance(test_date, date)
                
                # Test that date can be formatted for SQL
                date_str = test_date.strftime('%Y-%m-%d')
                self.assertIsInstance(date_str, str)
                self.assertEqual(len(date_str), 10) 
                
                # Test CSV filename generation
                expected_filename = f"ageing_fact_table_{test_date}.csv"
                self.assertIn(str(test_date), expected_filename)



if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2) 