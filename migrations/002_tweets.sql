--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
PRAGMA FOREIGN_KEYS = ON;
CREATE TABLE IF NOT EXISTS tweets(
                         id TEXT PRIMARY KEY NOT NULL,
                         timestamp TEXT NOT NULL ON CONFLICT REPLACE DEFAULT CURRENT_TIMESTAMP,
                         latest_payment TEXT NOT NULL,
                         FOREIGN KEY(latest_payment) REFERENCES payments (id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_tweet_ts ON tweets(timestamp);

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_tweet_ts;
DROP TABLE IF EXISTS tweets;
