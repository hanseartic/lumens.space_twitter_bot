--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
CREATE TABLE payments(
    id TEXT PRIMARY KEY NOT NULL,
    timestamp TEXT NOT NULL,
    asset TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    op_type SMALLINT NOT NULL
) WITHOUT ROWID;
CREATE INDEX idx_ts ON payments(timestamp);

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_ts;
DROP TABLE payments;
