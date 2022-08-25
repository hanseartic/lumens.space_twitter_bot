--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operations(
    id TEXT PRIMARY KEY NOT NULL,
    tx_id TEXT NOT NULL,
    timestamp TEXT NOT NULL ON CONFLICT REPLACE DEFAULT CURRENT_TIMESTAMP,
    memo TEXT,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    from_asset TEXT NOT NULL,
    to_asset TEXT NOT NULL,
    from_amount TEXT NOT NULL,
    to_amount TEXT NOT NULL,
    is_burn BOOLEAN NOT NULL ON CONFLICT REPLACE DEFAULT FALSE
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_opsc_tx_id ON operations(tx_id);

CREATE TABLE IF NOT EXISTS opscan(
    worker INT PRIMARY KEY NOT NULL,
    cursor_from  NOT NULL,
    cursor_to TEXT
) WITHOUT ROWID ;

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_opsc_tx_id;
DROP TABLE IF EXISTS operations;
DROP TABLE IF EXISTS opscan;
