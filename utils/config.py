"""
Configuration management for the Bitcoin Quantum Vulnerability Scanner.
Handles environment variable loading and provides centralized configuration.
"""

import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Find the project root directory (where .env should be located)
def find_project_root():
    """Find the project root directory by looking for .env file."""
    current = Path.cwd()
    
    # Look for .env file in current directory and parent directories
    while current != current.parent:
        if (current / '.env').exists():
            return current
        current = current.parent
    
    # If not found, return current directory
    return Path.cwd()

# Load environment variables from .env file in project root
project_root = find_project_root()
env_path = project_root / '.env'
load_dotenv(env_path)


class Config:
    """Centralized configuration class for the scanner."""
    
    # Bitcoin Core RPC Configuration
    BITCOIN_RPC_HOST = os.getenv('BITCOIN_RPC_HOST', '127.0.0.1')
    BITCOIN_RPC_PORT = int(os.getenv('BITCOIN_RPC_PORT', '8332'))
    BITCOIN_RPC_COOKIE_PATH = os.getenv('BITCOIN_RPC_COOKIE_PATH', '/home/user/.bitcoin/.cookie')
    
    # PostgreSQL Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'bitcoin_scanner')
    DB_USER = os.getenv('DB_USER', 'scanneruser')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'abc123')
    
    # Scanner Configuration
    SCAN_BATCH_SIZE = int(os.getenv('SCAN_BATCH_SIZE', '1000'))
    PROGRESS_UPDATE_INTERVAL = int(os.getenv('PROGRESS_UPDATE_INTERVAL', '100'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Optional Advanced Configuration
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))
    CONNECTION_TIMEOUT = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    
    @classmethod
    def get_database_url(cls) -> str:
        """Get the PostgreSQL connection URL."""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate that all required configuration is present."""
        required_vars = [
            'BITCOIN_RPC_HOST',
            'BITCOIN_RPC_PORT',
            'BITCOIN_RPC_COOKIE_PATH',
            'DB_HOST',
            'DB_PORT',
            'DB_NAME',
            'DB_USER',
            'DB_PASSWORD'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"Missing required configuration variables: {', '.join(missing_vars)}")
            return False
        
        return True


# Global configuration instance
config = Config() 