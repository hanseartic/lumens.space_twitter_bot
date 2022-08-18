const database = require('better-sqlite3')('db.sql');
const migrations = require('@blackglory/better-sqlite3-migrations');

const init = () => {
    migrations.migrate(database, [
        {
            version: 1,
            up: "CREATE TABLE IF NOT EXISTS payments(\n" +
                "    id TEXT PRIMARY KEY NOT NULL,\n" +
                "    timestamp TEXT NOT NULL,\n" +
                "    asset TEXT NOT NULL,\n" +
                "    sender TEXT NOT NULL,\n" +
                "    receiver TEXT NOT NULL,\n" +
                "    amount DOUBLE PRECISION NOT NULL,\n" +
                "    op_type SMALLINT NOT NULL\n" +
                ") WITHOUT ROWID;\n" +
                "CREATE INDEX IF NOT EXISTS idx_ts ON payments(timestamp);\n",
            down: "DROP INDEX IF EXISTS idx_ts;\n" +
                "DROP TABLE payments;",
        },
        {
            version: 2,
            up: "PRAGMA FOREIGN_KEYS = ON;\n" +
                "CREATE TABLE IF NOT EXISTS tweets(\n" +
                "                         id TEXT PRIMARY KEY NOT NULL,\n" +
                "                         timestamp TEXT NOT NULL ON CONFLICT REPLACE DEFAULT CURRENT_TIMESTAMP,\n" +
                "                         latest_payment TEXT NOT NULL,\n" +
                "                         FOREIGN KEY(latest_payment) REFERENCES payments (id)\n" +
                ") WITHOUT ROWID;\n" +
                "CREATE INDEX IF NOT EXISTS idx_tweet_ts ON tweets(timestamp);",
            down: "DROP INDEX IF EXISTS idx_tweet_ts;\n" +
                "DROP TABLE IF EXISTS tweets;"
        }
    ]);
    return database;
};

const getCursor = () => {
    return database?.prepare("SELECT id FROM payments ORDER BY id DESC LIMIT 1")
        .get()?.id;
}

const getLatestTweet = () => {
    return database?.prepare("SELECT * FROM tweets ORDER BY timestamp DESC LIMIT 1")
        .get();
};

const getBurnedCount = (asset) => {
    return database?.prepare(`SELECT count(*) as burned FROM payments WHERE op_type = '1' AND asset LIKE ?`)
        .get(`${asset ?? "%"}-%`).burned;
};

const getBurns = (asset) => {
    return database?.prepare(`SELECT * FROM payments WHERE op_type = '1' AND asset LIKE ?`)
        .all(`${asset ?? "%"}-%`);
}

const getSwappedCount = (asset) => {
    return database?.prepare(`SELECT count(*) as paid FROM payments WHERE op_type = '13' AND asset LIKE ?`)
        .get(`${asset ?? "%"}-%`).paid;
}

const getSwaps = (asset) => {
    return database?.prepare(`SELECT * FROM payments WHERE op_type = '13' AND asset LIKE ?`)
        .all(`${asset ?? "%"}-%`);
};

const matchAssets = (assetCode) => {
    return database?.prepare(`SELECT DISTINCT asset FROM payments WHERE asset like '${assetCode??"%"}-%'`)
        .all().map(row => {
            const rowData = row.asset.split('-');
            return {code: rowData[0], issuer: rowData[1]};
        });
};

const confirmTweet = (tweetId, payment) => {
    return database?.prepare(
        "INSERT INTO tweets (id, timestamp, latest_payment) VALUES(?, ?, ?)"
    ).run(
        tweetId, null, payment
    );
};

module.exports = {
    database,
    init,
    confirmTweet,
    getBurnedCount,
    getBurns,
    getLatestTweet,
    getPaymentsCursor: getCursor,
    getSwappedCount,
    getSwaps,
    matchAssets
};
