# BTCNow - Bitcoin Price Fetcher

A simple cron job that fetches the current Bitcoin price from the CoinGecko API and saves it to `/tmp/.btcnow`.

## Features

- ✅ Fetches real-time Bitcoin price from CoinGecko API
- ✅ Saves price data to `/tmp/.btcnow` in JSON format
- ✅ Configurable cron job scheduling
- ✅ Easy installation and setup scripts
- ✅ Comprehensive error handling and logging

## Installation

1. **Run the installation script:**
   ```bash
   cd btcnow
   chmod +x install.sh
   ./install.sh
   ```

2. **The installation script will:**
   - Install required Python dependencies (`requests`)
   - Make the script executable
   - Create a system-wide symlink (if possible)
   - Test the script functionality

## Setup (Cron Job Configuration)

1. **Run the setup script:**
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

2. **Choose your preferred update interval:**
   - Every 5 minutes (recommended for active monitoring)
   - Every 15 minutes
   - Every hour
   - Every 6 hours
   - Once daily

## Manual Usage

### Run once:
```bash
python3 btcnow.py
```

### Check current price:
```bash
cat /tmp/.btcnow
```

### Example output:
```json
{
  "timestamp": "2025-07-11T20:30:45.123456",
  "price_usd": 43250.75,
  "formatted_price": "$43,250.75"
}
```

## File Structure

```
btcnow/
├── btcnow.py      # Main script
├── install.sh     # Installation script
├── setup.sh       # Cron job setup script
└── README.md      # This file
```

## Cron Job Management

### View current cron jobs:
```bash
crontab -l | grep btcnow
```

### Remove all btcnow cron jobs:
```bash
./setup.sh
# Choose option 7
```

### Manual cron job addition:
```bash
# Add to crontab (every 5 minutes)
echo "*/5 * * * * /path/to/btcnow/btcnow.py >/dev/null 2>&1" | crontab -
```

## Troubleshooting

### Script not found:
- Ensure you ran `install.sh` first
- Check that `btcnow.py` is executable: `ls -la btcnow.py`

### API errors:
- Check internet connectivity
- CoinGecko API may be temporarily unavailable
- Script will log errors to console

### Permission errors:
- Ensure `/tmp` directory is writable
- Check file permissions: `ls -la /tmp/.btcnow`

### Cron job not running:
- Check cron service: `sudo systemctl status cron`
- View cron logs: `grep CRON /var/log/syslog`
- Test manually: `python3 btcnow.py`

## Dependencies

- Python 3.6+
- `requests` library (installed automatically)
- Internet connectivity for API access

## API Information

- **Source:** CoinGecko API (free tier)
- **Endpoint:** `https://api.coingecko.com/api/v3/simple/price`
- **Rate Limit:** 50 calls/minute (free tier)
- **No API key required**

## License

Part of the Bitcoin Quantum Vulnerability Scanner project. 