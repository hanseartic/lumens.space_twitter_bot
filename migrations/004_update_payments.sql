--------------------------------------------------------------------------------
-- Up
--------------------------------------------------------------------------------
UPDATE payments SET asset = REPLACE(asset, '-G', ':G');
UPDATE payments SET asset = REPLACE(asset, '-1', '');
UPDATE payments SET asset = REPLACE(asset, '-2', '');

--------------------------------------------------------------------------------
-- Down
--------------------------------------------------------------------------------
UPDATE payments SET asset = REPLACE(asset, ':', '-');
