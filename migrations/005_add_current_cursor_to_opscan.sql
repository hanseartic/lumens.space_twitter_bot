--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
CREATE TABLE opscan_tmp_up(
    worker         INT NOT NULL,
    cursor_from    INT NOT NULL PRIMARY KEY,
    cursor_current INT,
    cursor_to      INT,
    last_update_ts INT) without rowid;
INSERT INTO opscan_tmp_up(worker, cursor_from, cursor_current, cursor_to)
    SELECT worker, cursor_from, cursor_from, cursor_to
    FROM opscan;
DROP TABLE opscan;
ALTER TABLE opscan_tmp_up RENAME TO opscan;

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
CREATE TABLE opscan_tmp_down(
    worker INT NOT NULL PRIMARY KEY,
    cursor_from TEXT,
    cursor_to TEXT) without rowid;
INSERT INTO opscan_tmp_down(worker, cursor_from, cursor_to)
    SELECT worker, cursor_from, cursor_to
    FROM opscan;
DROP TABLE opscan;
ALTER TABLE opscan_tmp_down RENAME TO opscan;
