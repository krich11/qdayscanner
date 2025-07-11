# Bitcoin Quantum Vulnerability Scanner

A comprehensive blockchain scanner designed to identify and track quantum vulnerable addresses on the Bitcoin blockchain, specifically targeting P2PK and exposed P2PKH addresses.

## Project Overview

This project aims to scan the Bitcoin blockchain to identify addresses that are vulnerable to quantum attacks. The scanner focuses on:

- **P2PK (Pay-to-Public-Key) addresses**: These are directly vulnerable to quantum attacks as they expose the public key
- **Exposed P2PKH (Pay-to-Public-Key-Hash) addresses**: These become vulnerable when the public key is revealed in spending transactions

## Architecture

The project is organized into multiple subprojects, each building upon the previous ones:

```
qdayscanner/
├── p2pk_scanner/          # P2PK address identification
├── p2pkh_exposure/        # P2PKH public key exposure tracking
├── quantum_analysis/      # Quantum vulnerability analysis
├── reporting/             # Reporting and visualization tools
├── btcnow/               # Bitcoin price monitoring service
└── utils/                 # Shared utilities and database schemas
```

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Bitcoin Core node (fully synchronized)
- Access to Bitcoin Core RPC interface

## Environment Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd qdayscanner
   ```

2. **Set up environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your specific configuration
   ```

3. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

4. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Create a `.env` file in the main project directory with the following variables:

```env
# Bitcoin Core RPC Configuration
BITCOIN_RPC_HOST=127.0.0.1
BITCOIN_RPC_PORT=8332
BITCOIN_RPC_COOKIE_PATH=/home/user/.bitcoin/.cookie

# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bitcoin_scanner
DB_USER=scanneruser
DB_PASSWORD=abc123

# Scanner Configuration
SCAN_BATCH_SIZE=1000
PROGRESS_UPDATE_INTERVAL=100
```

## Database Setup

Each subproject includes its own database schema and setup scripts. Run the database initialization for each subproject:

```bash
# For P2PK scanner
cd p2pk_scanner
python setup_database.py

# For P2PKH exposure scanner
cd ../p2pkh_exposure
python setup_database.py
```

## Usage

### P2PK Scanner

The P2PK scanner identifies addresses that directly expose public keys:

```bash
cd p2pk_scanner
python scanner.py
```

### P2PKH Exposure Scanner

The P2PKH exposure scanner tracks when public keys are revealed:

```bash
cd p2pkh_exposure
python scanner.py
```

### BTCNow - Bitcoin Price Monitor

A lightweight service that fetches current Bitcoin prices and saves them to `/tmp/.btcnow`:

```bash
cd btcnow
./install.sh          # Install dependencies and setup
./setup.sh            # Configure cron job for automatic updates
cat /tmp/.btcnow      # Check current price
```

**Features:**
- Fetches real-time Bitcoin price from CoinGecko API
- Configurable cron job scheduling (5min to daily)
- JSON output with timestamp and formatted price
- Easy installation and management scripts

## Project Status

- [x] Repository setup and documentation
- [x] Environment configuration
- [x] P2PK scanner implementation
- [x] BTCNow price monitoring service
- [ ] P2PKH exposure scanner
- [ ] Quantum vulnerability analysis
- [ ] Reporting and visualization tools

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Security Notes

- Never commit credentials to version control
- Use environment variables for all sensitive configuration
- Regularly update dependencies for security patches
- Monitor scanner performance and resource usage

## Troubleshooting

### Common Issues

1. **Bitcoin Core RPC connection failed**
   - Verify Bitcoin Core is running
   - Check RPC credentials and cookie file
   - Ensure firewall allows local connections

2. **Database connection issues**
   - Verify PostgreSQL is running
   - Check database credentials in .env file
   - Ensure database exists and user has proper permissions

3. **Scanner stops unexpectedly**
   - Check logs for error messages
   - Verify sufficient disk space
   - Monitor system resources

### Logs

All scanners generate detailed logs in their respective `logs/` directories. Check these for debugging information.

## Performance Considerations

- The scanner is designed to resume from where it left off if interrupted
- Progress is saved regularly to prevent data loss
- Large blockchain scans may take several hours or days
- Monitor system resources during long-running scans 