#!/usr/bin/env python3
"""
Database setup script for Quantum Vulnerability Analysis.
Creates all necessary tables and indexes for the quantum analysis system.
"""

import sys
import os
import logging
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.config import config
from utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_tables():
    """Create all necessary tables for the quantum analysis system."""
    
    # Create quantum_analysis_results table
    quantum_analysis_results_sql = """
    CREATE TABLE IF NOT EXISTS quantum_analysis_results (
        id SERIAL PRIMARY KEY,
        analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metric_name VARCHAR(100) NOT NULL,
        metric_value DECIMAL(20,8),
        metric_json JSONB,
        description TEXT,
        risk_level VARCHAR(20) CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create anomaly_events table
    anomaly_events_sql = """
    CREATE TABLE IF NOT EXISTS anomaly_events (
        id SERIAL PRIMARY KEY,
        event_date TIMESTAMP NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        severity VARCHAR(20) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
        description TEXT NOT NULL,
        affected_addresses INTEGER,
        affected_balance_satoshi BIGINT,
        confidence_score DECIMAL(3,2) CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),
        details_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create risk_assessments table
    risk_assessments_sql = """
    CREATE TABLE IF NOT EXISTS risk_assessments (
        id SERIAL PRIMARY KEY,
        assessment_date TIMESTAMP NOT NULL,
        total_at_risk_satoshi BIGINT NOT NULL,
        total_at_risk_usd DECIMAL(20,2),
        active_addresses INTEGER,
        dormant_addresses INTEGER,
        whale_addresses INTEGER, -- > 1000 BTC
        medium_addresses INTEGER, -- 100-1000 BTC
        small_addresses INTEGER, -- < 100 BTC
        risk_score DECIMAL(3,2) CHECK (risk_score >= 0.00 AND risk_score <= 1.00),
        btc_price_usd DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create spending_patterns table for transaction analysis
    spending_patterns_sql = """
    CREATE TABLE IF NOT EXISTS spending_patterns (
        id SERIAL PRIMARY KEY,
        analysis_date TIMESTAMP NOT NULL,
        period_start TIMESTAMP NOT NULL,
        period_end TIMESTAMP NOT NULL,
        total_spent_satoshi BIGINT NOT NULL,
        address_count INTEGER NOT NULL,
        avg_fee_satoshi BIGINT,
        max_fee_satoshi BIGINT,
        unusual_patterns INTEGER,
        risk_indicator DECIMAL(3,2),
        details_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create address_clusters table for clustering analysis
    address_clusters_sql = """
    CREATE TABLE IF NOT EXISTS address_clusters (
        id SERIAL PRIMARY KEY,
        cluster_id VARCHAR(50) NOT NULL,
        cluster_date TIMESTAMP NOT NULL,
        address_count INTEGER NOT NULL,
        total_balance_satoshi BIGINT NOT NULL,
        cluster_type VARCHAR(50), -- 'geographic', 'temporal', 'behavioral'
        confidence_score DECIMAL(3,2),
        details_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for better performance
    indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_quantum_analysis_results_date ON quantum_analysis_results(analysis_date);",
        "CREATE INDEX IF NOT EXISTS idx_quantum_analysis_results_metric ON quantum_analysis_results(metric_name);",
        "CREATE INDEX IF NOT EXISTS idx_quantum_analysis_results_risk ON quantum_analysis_results(risk_level);",
        "CREATE INDEX IF NOT EXISTS idx_anomaly_events_date ON anomaly_events(event_date);",
        "CREATE INDEX IF NOT EXISTS idx_anomaly_events_type ON anomaly_events(event_type);",
        "CREATE INDEX IF NOT EXISTS idx_anomaly_events_severity ON anomaly_events(severity);",
        "CREATE INDEX IF NOT EXISTS idx_risk_assessments_date ON risk_assessments(assessment_date);",
        "CREATE INDEX IF NOT EXISTS idx_risk_assessments_score ON risk_assessments(risk_score);",
        "CREATE INDEX IF NOT EXISTS idx_spending_patterns_date ON spending_patterns(analysis_date);",
        "CREATE INDEX IF NOT EXISTS idx_spending_patterns_period ON spending_patterns(period_start, period_end);",
        "CREATE INDEX IF NOT EXISTS idx_address_clusters_date ON address_clusters(cluster_date);",
        "CREATE INDEX IF NOT EXISTS idx_address_clusters_type ON address_clusters(cluster_type);",
        "CREATE INDEX IF NOT EXISTS idx_address_clusters_balance ON address_clusters(total_balance_satoshi);"
    ]
    
    try:
        logger.info("Creating quantum analysis database tables...")
        
        # Execute table creation
        db_manager.execute_command(quantum_analysis_results_sql)
        logger.info("Created quantum_analysis_results table")
        
        db_manager.execute_command(anomaly_events_sql)
        logger.info("Created anomaly_events table")
        
        db_manager.execute_command(risk_assessments_sql)
        logger.info("Created risk_assessments table")
        
        db_manager.execute_command(spending_patterns_sql)
        logger.info("Created spending_patterns table")
        
        db_manager.execute_command(address_clusters_sql)
        logger.info("Created address_clusters table")
        
        # Create indexes
        for index_sql in indexes_sql:
            db_manager.execute_command(index_sql)
        logger.info("Created database indexes")
        
        logger.info("Database setup completed successfully!")
        
        # Show table counts
        tables = ['quantum_analysis_results', 'anomaly_events', 'risk_assessments', 'spending_patterns', 'address_clusters']
        for table in tables:
            count = db_manager.get_table_count(table)
            logger.info(f"Table {table}: {count} rows")
            
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)


def drop_tables():
    """Drop all quantum analysis tables (for reset)."""
    try:
        logger.info("Dropping quantum analysis tables...")
        
        tables = [
            'address_clusters',
            'spending_patterns',
            'risk_assessments',
            'anomaly_events',
            'quantum_analysis_results'
        ]
        
        for table in tables:
            if db_manager.table_exists(table):
                db_manager.execute_command(f"DROP TABLE {table} CASCADE;")
                logger.info(f"Dropped table: {table}")
        
        logger.info("All quantum analysis tables dropped successfully!")
        
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        sys.exit(1)


def verify_p2pk_data():
    """Verify that P2PK scanner data exists for analysis."""
    try:
        logger.info("Verifying P2PK scanner data...")
        
        # Check if P2PK tables exist
        p2pk_tables = ['p2pk_addresses', 'p2pk_transactions', 'p2pk_address_blocks']
        for table in p2pk_tables:
            if not db_manager.table_exists(table):
                logger.error(f"Required P2PK table '{table}' not found!")
                logger.error("Please run the P2PK scanner first to collect data.")
                return False
        
        # Check if we have data
        address_count = db_manager.get_table_count('p2pk_addresses')
        transaction_count = db_manager.get_table_count('p2pk_transactions')
        
        if address_count == 0:
            logger.error("No P2PK addresses found in database!")
            logger.error("Please run the P2PK scanner first to collect data.")
            return False
        
        logger.info(f"✓ P2PK data verified: {address_count} addresses, {transaction_count} transactions")
        return True
        
    except Exception as e:
        logger.error(f"Failed to verify P2PK data: {e}")
        return False


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) > 1 and sys.argv[1] == '--reset':
        logger.info("Resetting database...")
        drop_tables()
        create_tables()
    elif len(sys.argv) > 1 and sys.argv[1] == '--verify':
        logger.info("Verifying setup...")
        verify_p2pk_data()
    else:
        create_tables()
        verify_p2pk_data()


if __name__ == "__main__":
    main() 