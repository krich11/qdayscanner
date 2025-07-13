# Hydra Mode P2PK Scanner

A high-performance Bitcoin P2PK (Pay-to-Public-Key) vulnerability scanner using multi-threaded processing with write-behind caching.

## Features

- **Multi-threaded processing**: 64 worker threads for maximum throughput
- **Write-behind caching**: Batched database writes for optimal performance
- **Auto-pause functionality**: Automatically pauses when output queue depth exceeds threshold
- **Graceful shutdown**: Ensures no blocks are lost during shutdown
- **Real-time monitoring**: Live performance metrics and status reporting
- **Pause/Resume**: Manual control over worker processing
- **Quick scan mode**: Optimized scanning for blocks without P2PK signatures

## Graceful Shutdown Improvements

### Problem Fixed
Previously, workers would stop immediately when the stop event was set, potentially stranding blocks in their queues. This could result in missing blocks near the end of scans.

### Solution Implemented
- **Queue-first shutdown**: Workers now continue processing their entire queue until empty, regardless of the stop event
- **No early exits**: Removed break statements that could cause workers to exit while still having work
- **Natural termination**: Workers only exit when their queue is empty AND the stop event is set
- **Simplified shutdown**: No need for sentinel values - workers naturally stop when work is complete

### Key Changes
1. **Worker loop logic**: Changed from `while not stop_event.is_set()` to `while True` with exit condition only when queue is empty and stop event is set
2. **Pause handling**: Workers no longer exit when stop event is set while paused - they complete their current block
3. **Exception handling**: Workers continue processing on exceptions rather than exiting
4. **Shutdown sequence**: Simplified to wait for workers to naturally finish their queues

### Benefits
- **No lost blocks**: All blocks in worker queues are processed before shutdown
- **Faster shutdown**: No need to send sentinel values or wait for artificial completion
- **More reliable**: Workers are more resilient to exceptions and stop events
- **Better monitoring**: Clear logging shows when workers are processing their final queues

## Usage

### Basic Usage
```bash
python hydra_mode_scanner.py --start-block 0 --end-block 100000
```

### Auto-pause Configuration
```bash
# Pause when output queue exceeds 50,000 items, resume when below 10,000
python hydra_mode_scanner.py --auto-pause-threshold 50000 --auto-resume-threshold 10000
```

### Performance Options
```bash
# Enable quick scan mode for faster processing
python hydra_mode_scanner.py --quick-scan

# Enable batch RPC for reduced API calls
python hydra_mode_scanner.py --batch-rpc --rpc-batch-size 25

# Enable worker profiling
python hydra_mode_scanner.py --worker-profile
```

## Interactive Commands

During scanning, you can use these keyboard commands:

- `q` - Quit scanner (graceful shutdown)
- `p` - Pause/Resume workers
- `h` - Show help
- `s` - Show status
- `i` - Show P2PK integrity
- `m` - Show detailed metrics
- `u` - Show queue status

## Configuration

### Auto-pause Settings
- `--auto-pause-threshold`: Queue depth at which to pause (default: 100,000)
- `--auto-resume-threshold`: Queue depth at which to resume (default: 10,000)
- `--no-auto-pause`: Disable auto-pause functionality

### Performance Settings
- `--batch-size`: Database batch size (default: 1,000)
- `--queue-size`: Output queue size (default: 1,000,000)
- `--worker-queue-depth`: Target depth per worker (default: 4)

### Scanning Options
- `--start-block`: Starting block height
- `--end-block`: Ending block height
- `--quick-scan`: Enable quick scan mode
- `--batch-rpc`: Enable batch RPC calls
- `--rpc-batch-size`: RPC batch size (default: 25)

## Database Schema

The scanner creates and uses these tables:

### p2pk_addresses
- `id`: Primary key
- `address`: Bitcoin address (unique)
- `public_key_hex`: Public key in hex format
- `first_seen_block`: First block where address appeared
- `last_seen_block`: Last block where address appeared
- `created_at`: Record creation timestamp
- `updated_at`: Record update timestamp

### p2pk_transactions
- `id`: Primary key
- `txid`: Transaction ID
- `block_height`: Block height
- `address_id`: Foreign key to p2pk_addresses
- `input_index`: Input index in transaction
- `public_key_hex`: Public key in hex format
- `script_sig_hex`: Script signature in hex format
- `created_at`: Record creation timestamp

### p2pk_blocks
- `id`: Primary key
- `address_id`: Foreign key to p2pk_addresses
- `block_height`: Block height
- `block_time`: Block timestamp
- `created_at`: Record creation timestamp

### scan_progress
- `scanner_name`: Scanner identifier
- `last_scanned_block`: Last processed block
- `total_blocks_scanned`: Total blocks scanned
- `last_updated`: Last update timestamp

## Performance Monitoring

The scanner provides comprehensive performance metrics:

- **Throughput**: Blocks per second
- **Queue depths**: Worker and output queue monitoring
- **Database performance**: Batch flush times and operation counts
- **RPC performance**: API call counts and timing
- **Worker status**: Individual worker thread status
- **P2PK integrity**: Found vs stored address tracking

## Troubleshooting

### High Queue Depth
If the output queue depth remains high:
1. Check database connection and performance
2. Consider reducing batch size
3. Enable auto-pause with lower thresholds
4. Monitor system resources (CPU, memory, disk I/O)

### Missing Blocks
If blocks are missing after shutdown:
1. Check the scan_progress table for the last processed block
2. Verify that all workers completed their queues
3. Look for error messages in the logs
4. The improved graceful shutdown should prevent this issue

### Worker Performance
If workers are slow:
1. Check RPC response times
2. Consider enabling quick scan mode
3. Monitor network connectivity to Bitcoin node
4. Check system resources

## Architecture

### Threading Model
- **64 worker threads**: Process blocks in parallel
- **1 distributor thread**: Keeps worker queues filled
- **1 writer thread**: Handles database writes in batches
- **1 keyboard listener**: Handles interactive commands

### Queue Management
- **Main queue**: Holds all blocks to be processed
- **Worker queues**: Individual queues for each worker (target depth: 4)
- **Output queue**: Write-behind cache for database operations

### Database Operations
- **Batched inserts**: Groups operations for efficiency
- **Write-behind caching**: Reduces database contention
- **Upsert logic**: Handles duplicate addresses gracefully
- **Transaction integrity**: Ensures no data loss

## Development

### Adding New Features
1. Follow the existing code structure
2. Add appropriate logging
3. Update performance metrics
4. Test with various block ranges
5. Update documentation

### Testing
- Test with small block ranges first
- Verify database integrity after scans
- Check performance with different configurations
- Test graceful shutdown scenarios

## License

This project is part of the Bitcoin Quantum Vulnerability Scanner suite. 