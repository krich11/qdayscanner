"""
Bitcoin RPC client for the P2PK Scanner.
Handles communication with Bitcoin Core node via RPC.
"""

import json
import logging
import requests
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from requests.adapters import HTTPAdapter

from utils.config import config

logger = logging.getLogger(__name__)


class BitcoinRPC:
    """Bitcoin RPC client for communicating with Bitcoin Core."""
    
    def __init__(self):
        self.host = config.BITCOIN_RPC_HOST
        self.port = config.BITCOIN_RPC_PORT
        self.cookie_path = config.BITCOIN_RPC_COOKIE_PATH
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=128, pool_maxsize=128)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Load RPC credentials from cookie file
        self._load_credentials()
    
    def _load_credentials(self):
        """Load RPC credentials from Bitcoin Core cookie file."""
        try:
            cookie_file = Path(self.cookie_path)
            if not cookie_file.exists():
                raise FileNotFoundError(f"Bitcoin Core cookie file not found: {self.cookie_path}")
            
            with open(cookie_file, 'r') as f:
                cookie_content = f.read().strip()
            
            # Cookie format: username:password
            if ':' not in cookie_content:
                raise ValueError("Invalid cookie file format")
            
            username, password = cookie_content.split(':', 1)
            self.auth = (username, password)
            logger.info("Loaded Bitcoin Core RPC credentials from cookie file")
            
        except Exception as e:
            logger.error(f"Failed to load RPC credentials: {e}")
            raise
    
    def _make_request(self, method: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
        """Make an RPC request to Bitcoin Core."""
        url = f"http://{self.host}:{self.port}"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            'jsonrpc': '1.0',
            'id': 'p2pk_scanner',
            'method': method,
            'params': params or []
        }
        
        for attempt in range(config.MAX_RETRIES):
            try:
                response = self.session.post(
                    url,
                    auth=self.auth,
                    headers=headers,
                    json=payload,
                    timeout=config.CONNECTION_TIMEOUT
                )
                response.raise_for_status()
                
                result = response.json()
                
                if 'error' in result and result['error'] is not None:
                    raise Exception(f"RPC error: {result['error']}")
                
                result_data = result.get('result')
                if result_data is None:
                    raise Exception("No result in RPC response")
                
                return result_data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"RPC request failed (attempt {attempt + 1}/{config.MAX_RETRIES}): {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_DELAY)
                else:
                    raise Exception(f"RPC request failed after {config.MAX_RETRIES} attempts: {e}")
    
    def get_blockchain_info(self) -> Dict[str, Any]:
        """Get blockchain information."""
        return self._make_request('getblockchaininfo')
    
    def get_block_count(self) -> int:
        """Get the current block count."""
        result = self._make_request('getblockcount')
        if isinstance(result, int):
            return result
        raise Exception("get_block_count did not return an int")
    
    def get_block_hash(self, height: int) -> str:
        """Get block hash by height."""
        result = self._make_request('getblockhash', [height])
        if isinstance(result, str):
            return result
        raise Exception("get_block_hash did not return a str")
    
    def get_block(self, block_hash: str, verbosity: int = 2) -> Dict[str, Any]:
        """Get block information by hash."""
        result = self._make_request('getblock', [block_hash, verbosity])
        if isinstance(result, dict):
            return result
        raise Exception("get_block did not return a dict")
    
    def get_block_by_height(self, height: int, verbosity: int = 2) -> Dict[str, Any]:
        """Get block information by height."""
        block_hash = self.get_block_hash(height)
        if block_hash is None:
            raise Exception(f"get_block_hash returned None for height {height}")
        return self.get_block(block_hash, verbosity)
    
    def get_raw_transaction(self, txid: str, verbose: bool = True, block_hash: str = None) -> Dict[str, Any]:
        """Get raw transaction information."""
        params = [txid, verbose]
        if block_hash:
            params.append(block_hash)
        result = self._make_request('getrawtransaction', params)
        if isinstance(result, dict):
            return result
        raise Exception("get_raw_transaction did not return a dict")
    
    def test_connection(self) -> bool:
        """Test RPC connection to Bitcoin Core."""
        try:
            info = self.get_blockchain_info()
            logger.info(f"Connected to Bitcoin Core {info.get('version', 'unknown')}")
            logger.info(f"Current block height: {info.get('blocks', 0)}")
            logger.info(f"Chain: {info.get('chain', 'unknown')}")
            return True
        except Exception as e:
            logger.error(f"Bitcoin Core RPC connection test failed: {e}")
            return False
    
    def get_blocks_range(self, start_height: int, end_height: int) -> List[Dict[str, Any]]:
        """Get a range of blocks."""
        blocks = []
        for height in range(start_height, end_height + 1):
            try:
                block = self.get_block_by_height(height)
                blocks.append(block)
            except Exception as e:
                logger.error(f"Failed to get block {height}: {e}")
                # Continue with next block instead of failing completely
                continue
        return blocks


# Global RPC client instance
bitcoin_rpc = BitcoinRPC() 