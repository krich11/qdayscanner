CREATE UNIQUE INDEX idx_p2pk_addresses_address ON p2pk_addresses(address);
CREATE INDEX idx_p2pk_addresses_public_key ON p2pk_addresses(public_key_hex);
CREATE INDEX idx_p2pk_transactions_address_id ON p2pk_transactions(address_id);
CREATE INDEX idx_p2pk_transactions_block_height ON p2pk_transactions(block_height);
CREATE INDEX idx_p2pk_address_blocks_address_id ON p2pk_address_blocks(address_id);
CREATE INDEX idx_p2pk_address_blocks_block_height ON p2pk_address_blocks(block_height);
