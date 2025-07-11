# Quick Start Guide

This guide will help you get the Bitcoin Quantum Vulnerability Scanner up and running quickly.

## Prerequisites

Before starting, ensure you have:

1. **Python 3.8+** installed
2. **PostgreSQL** server running locally
3. **Bitcoin Core** node running and fully synchronized
4. **Access to Bitcoin Core RPC** (cookie authentication)

## Step 1: Clone and Setup

```bash
# Clone the repository (if not already done)
git clone <repository-url>
cd qdayscanner

# Run the automated setup script
./setup.sh
```

The setup script will:
- Create a Python virtual environment
- Install all dependencies
- Create necessary directories
- Set up the database schema
- Create a `.env` file from template

## Step 2: Configure Environment

Edit the `.env` file with your specific configuration:

```bash
# Edit the environment file (in the main project directory)
nano .env
```

Update these key values:
- `BITCOIN_RPC_COOKIE_PATH`: Path to your Bitcoin Core `.cookie` file
- `DB_PASSWORD`: Your PostgreSQL password
- `DB_HOST`: PostgreSQL host (usually `localhost`)

**Note**: The `.env` file should be in the main project directory (`qdayscanner/`), not in subproject directories.

## Step 3: Test the Setup

```bash
# Test the P2PK scanner setup
cd p2pk_scanner
python test_scanner.py
```

This will verify:
- Configuration loading
- Database connection
- Bitcoin RPC connection
- Sample scanning functionality

## Step 4: Run the Scanner

```bash
# Run the P2PK scanner
./run_scanner.sh
```

The scanner will:
- Resume from where it left off if interrupted
- Process blocks in batches
- Log progress and statistics
- Store results in the database

## Common Commands

### Reset Database
```bash
cd p2pk_scanner
./reset_database.sh
```

### Run with Custom Range
```bash
cd p2pk_scanner
./run_scanner.sh --start-block 500000 --end-block 501000
```

### Reset Progress and Start Over
```bash
cd p2pk_scanner
./run_scanner.sh --reset
```

## Monitoring Progress

The scanner provides several ways to monitor progress:

1. **Console Output**: Real-time progress updates
2. **Log Files**: Detailed logs in `p2pk_scanner/logs/`
3. **Database**: Query results directly from PostgreSQL

### Check Database Statistics
```sql
-- Connect to your database
psql -U scanneruser -d bitcoin_scanner

-- Check P2PK addresses found
SELECT COUNT(*) FROM p2pk_addresses;

-- Check scan progress
SELECT * FROM scan_progress WHERE scanner_name = 'p2pk_scanner';

-- View recent P2PK addresses
SELECT address, first_seen_block, total_received_satoshi 
FROM p2pk_addresses 
ORDER BY first_seen_block DESC 
LIMIT 10;
```

## Troubleshooting

### Connection Issues

**Bitcoin Core RPC failed:**
- Verify Bitcoin Core is running: `bitcoin-cli getblockchaininfo`
- Check cookie file exists and is readable
- Ensure RPC is enabled in `bitcoin.conf`

**Database connection failed:**
- Verify PostgreSQL is running: `sudo systemctl status postgresql`
- Check credentials in `.env` file
- Ensure database exists: `createdb bitcoin_scanner`

### Performance Issues

- Adjust `SCAN_BATCH_SIZE` in `.env` for your system
- Monitor system resources during scanning
- Consider running during off-peak hours

### Scanner Stops Unexpectedly

- Check logs in `p2pk_scanner/logs/`
- Verify sufficient disk space
- Check system memory usage

## Next Steps

Once the P2PK scanner is running successfully:

1. **Monitor Results**: Check the database for found P2PK addresses
2. **Analyze Data**: Use the reporting tools (coming soon)
3. **Extend Functionality**: Add P2PKH exposure scanning
4. **Contribute**: Submit improvements and bug reports

## Support

For issues and questions:
- Check the main README.md for detailed documentation
- Review the troubleshooting section
- Check log files for error details
- Submit issues to the project repository 