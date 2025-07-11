# P2PK Scanner

This subproject identifies and tracks P2PK (Pay-to-Public-Key) addresses on the Bitcoin blockchain. P2PK addresses are directly vulnerable to quantum attacks because they expose the public key in the locking script.

## Overview

P2PK addresses use the format `OP_PUSHDATA <public_key> OP_CHECKSIG` in their locking script. This means the public key is directly visible on the blockchain, making these addresses vulnerable to quantum attacks that can derive the private key from the public key.

## Database Schema

The P2PK scanner uses the following database tables:

### p2pk_addresses
Stores identified P2PK addresses and their metadata.

```sql
CREATE TABLE p2pk_addresses (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL UNIQUE,
    public_key_hex VARCHAR(130) NOT NULL,
    first_seen_block INTEGER NOT NULL,
    first_seen_txid VARCHAR(64) NOT NULL,
    last_seen_block INTEGER NOT NULL,
    total_received_satoshi BIGINT DEFAULT 0,
    current_balance_satoshi BIGINT DEFAULT 0,
    is_spent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### p2pk_transactions
Tracks all transactions involving P2PK addresses.

```sql
CREATE TABLE p2pk_transactions (
    id SERIAL PRIMARY KEY,
    txid VARCHAR(64) NOT NULL,
    block_height INTEGER NOT NULL,
    block_time TIMESTAMP NOT NULL,
    address_id INTEGER REFERENCES p2pk_addresses(id),
    is_input BOOLEAN NOT NULL,
    amount_satoshi BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### p2pk_address_blocks
Efficiently tracks transaction blocks for each P2PK address to enable fast balance calculation.

```sql
CREATE TABLE p2pk_address_blocks (
    id SERIAL PRIMARY KEY,
    address_id INTEGER REFERENCES p2pk_addresses(id) ON DELETE CASCADE,
    block_height INTEGER NOT NULL,
    is_input BOOLEAN NOT NULL,
    amount_satoshi BIGINT NOT NULL,
    txid VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### scan_progress
Tracks scanning progress to enable resumption.

```sql
CREATE TABLE scan_progress (
    id SERIAL PRIMARY KEY,
    scanner_name VARCHAR(50) NOT NULL UNIQUE,
    last_scanned_block INTEGER NOT NULL,
    total_blocks_scanned INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage

### Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment**:
   ```bash
   cp ../env.example ../.env
   # Edit ../.env with your configuration
   ```

3. **Initialize database**:
   ```bash
   python setup_database.py
   ```

### Running the Scanner

```bash
python scanner.py
```

The scanner will:
- Resume from the last scanned block if interrupted
- Process blocks in batches for efficiency
- Log progress and statistics
- Store identified P2PK addresses and transactions

### Configuration

Key configuration options in `.env`:

- `SCAN_BATCH_SIZE`: Number of blocks to process in each batch (default: 1000)
- `PROGRESS_UPDATE_INTERVAL`: How often to log progress (default: 100 blocks)
- `START_BLOCK`: Block to start scanning from (optional, defaults to 0)
- `END_BLOCK`: Block to stop scanning at (optional, defaults to latest)

**Note**: The `.env` file should be placed in the main project directory (`qdayscanner/`), not in the subproject directories.

## Output

The scanner provides:

1. **Console output**: Progress updates and statistics
2. **Database storage**: All P2PK addresses and transactions
3. **Log files**: Detailed logging in `logs/` directory

### Sample Output

```
[INFO] Starting P2PK scanner...
[INFO] Resuming from block 500000
[INFO] Processing blocks 500000-501000 (1000 blocks)
[INFO] Found 5 P2PK addresses in this batch
[INFO] Progress: 500000/750000 blocks (66.7%)
[INFO] Total P2PK addresses found: 1,234
[INFO] Batch completed in 45.2 seconds
```

## Analysis

### Identifying P2PK Addresses

The scanner identifies P2PK addresses by:

1. Parsing transaction outputs (receiving funds)
2. Parsing transaction inputs (spending funds) by looking up previous transactions
3. Looking for scripts with pattern: `OP_PUSHDATA <public_key> OP_CHECKSIG`
4. Extracting the public key and deriving the address
5. Recording metadata (first seen, balance, etc.)

### Balance Calculation

The scanner uses an efficient block tracking system:

1. **Transaction Tracking**: Records all P2PK transactions with block heights
2. **Fast Balance Calculation**: Uses `p2pk_address_blocks` table for O(m) balance calculation instead of O(n) blockchain rescan
3. **Historical Balances**: Can calculate balance at any point in time
4. **Automatic Updates**: Balances are calculated from transaction history

**Example Balance Query:**
```sql
-- Calculate current balance for an address
SELECT SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END) as balance
FROM p2pk_address_blocks 
WHERE address_id = ?;

-- Calculate balance at specific block height
SELECT SUM(CASE WHEN is_input THEN -amount_satoshi ELSE amount_satoshi END) as balance
FROM p2pk_address_blocks 
WHERE address_id = ? AND block_height <= ?;
```

### Vulnerability Assessment

P2PK addresses are considered **highly vulnerable** to quantum attacks because:

- Public keys are directly exposed on the blockchain
- No additional cryptographic protection exists
- Private keys can be derived using quantum algorithms (when available)

## Troubleshooting

### Common Issues

1. **Bitcoin Core RPC connection failed**
   - Verify Bitcoin Core is running and accessible
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

### Performance Optimization

- Adjust `SCAN_BATCH_SIZE` based on system performance
- Monitor memory usage during long scans
- Consider running during off-peak hours

## Security Considerations

- P2PK addresses should be considered compromised in a quantum computing environment
- Funds in P2PK addresses should be moved to quantum-resistant addresses
- This scanner helps identify vulnerable funds for migration planning

## Future Enhancements

- [ ] Add support for scanning testnet
- [ ] Implement parallel processing for faster scanning
- [ ] Add export functionality for analysis tools
- [ ] Create web interface for results visualization 