const sqlite3 = require("sqlite3");
const sqlite = require("sqlite");


let database;
const init = () => {
    return sqlite
        .open({
            filename: "db.sql",
            driver: sqlite3.Database,
        })
        .then(db => db.migrate({
            migrationsPath: "./migrations",
        }).then(() => database = db));
};

const getCursor = () => {
    return database?.get("SELECT id FROM payments ORDER BY id DESC LIMIT 1")
        .then(r => r?.id);
}

const getBurnedCount = (asset) => {
    return database?.get(`SELECT count(*) as burned FROM payments WHERE op_type = '1' AND asset LIKE '${asset??"%"}-%'`)
        .then(r => r.burned)
};

const getSwappedCount = (asset) => {
    return database?.get(`SELECT count(*) as paid FROM payments WHERE op_type = '13' AND asset LIKE '${asset??"%"}-%'`)
        .then(r => r.paid);
}

const getSwaps = (asset) => {
    return database?.all(`SELECT * FROM payments WHERE op_type = '13' AND asset LIKE '${asset??"%"}-%'`);
};

const matchAssets = (assetCode) => {
    return database?.all(`SELECT DISTINCT asset FROM payments WHERE asset like '${assetCode??"%"}-%'`)
        .then(rows => rows.map(row => {
            const rowData = row.asset.split('-');
            return {code: rowData[0], issuer: rowData[1]};
        }));
};

const getLatestTweet = () => {
    return database?.get("SELECT * FROM tweets ORDER BY timestamp DESC LIMIT 1");
};

const confirmTweet = (tweetId, payment) => {
    return database?.run(
        "INSERT INTO tweets (id, timestamp, latest_payment) VALUES(?, ?, ?)",
        tweetId, null, payment
    );
};

module.exports = {
    init,
    confirmTweet,
    getBurnedCount,
    getLatestTweet,
    getPaymentsCursor: getCursor,
    getSwappedCount,
    getSwaps,
    matchAssets
};
