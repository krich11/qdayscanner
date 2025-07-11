#!/usr/bin/env python3
"""
BTCNow - Bitcoin Price Fetcher
Fetches current Bitcoin price from CoinGecko API and saves to /tmp/.btcnow
"""

import requests
import json
import logging
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_bitcoin_price():
    """Fetch current Bitcoin price from CoinGecko API."""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin",
            "vs_currencies": "usd"
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        price = data["bitcoin"]["usd"]
        
        logger.info(f"Successfully fetched Bitcoin price: ${price:,.2f}")
        return price
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch Bitcoin price: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Failed to parse Bitcoin price response: {e}")
        return None

def save_price_to_file(price):
    """Save the Bitcoin price to /tmp/.btcnow file."""
    try:
        output_file = Path("/tmp/.btcnow")
        
        # Create data structure
        data = {
            "timestamp": datetime.now().isoformat(),
            "price_usd": price,
            "formatted_price": f"${price:,.2f}"
        }
        
        # Write to file
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Price saved to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save price to file: {e}")
        return False

def main():
    """Main function."""
    logger.info("Starting BTCNow price fetch...")
    
    # Fetch current price
    price = fetch_bitcoin_price()
    
    if price is not None:
        # Save to file
        success = save_price_to_file(price)
        if success:
            logger.info("BTCNow completed successfully")
            return 0
        else:
            logger.error("BTCNow failed to save price")
            return 1
    else:
        logger.error("BTCNow failed to fetch price")
        return 1

if __name__ == "__main__":
    exit(main()) 