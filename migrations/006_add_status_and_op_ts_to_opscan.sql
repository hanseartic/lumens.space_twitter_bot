--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
CREATE TABLE opscan_tmp_up(
    worker         INT NOT NULL,
    cursor_from    INT NOT NULL PRIMARY KEY,
    cursor_current INT,
    cursor_to      INT,
    last_update_ts INT,
    current_ts INT,
    status TEXT DEFAULT 'stopped') without rowid;
INSERT INTO opscan_tmp_up(worker, cursor_from, cursor_current, cursor_to, status)
    SELECT worker, cursor_from, cursor_from, cursor_to, 'stopped' as status
    FROM opscan;
DROP TABLE opscan;
ALTER TABLE opscan_tmp_up RENAME TO opscan;

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
CREATE TABLE opscan_tmp_down(
    worker         INT NOT NULL,
    cursor_from    INT NOT NULL PRIMARY KEY,
    cursor_current INT,
    cursor_to      INT,
    last_update_ts INT) without rowid;
INSERT INTO opscan_tmp_down(worker, cursor_from, cursor_current, cursor_to, last_update_ts)
    SELECT worker, cursor_from, cursor_current, cursor_to, last_update_ts
    FROM opscan;
DROP TABLE opscan;
ALTER TABLE opscan_tmp_down RENAME TO opscan;
