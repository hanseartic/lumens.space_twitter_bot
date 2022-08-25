const DB = require('better-sqlite3-helper');

const options = {
    path: './db.sql',
    WAL: true,
    migrate: {  // disable completely by setting `migrate: false`
//        force: 'last',
    }
};

const db = () => DB(options);

const getScanCursor = () => {
    return db().queryFirstRow("SELECT id from operations ORDER BY id DESC LIMIT 1")?.id;
}

const getCursor = () => {
    return db().queryFirstRow("SELECT id FROM payments ORDER BY id DESC LIMIT 1")?.id;
};

const getLatestTweet = () => {
    return db().queryFirstRowObject("SELECT id, datetime(timestamp, 'localtime') as timestamp, latest_payment FROM tweets ORDER BY timestamp DESC LIMIT 1");
};

const getBurnedCount = (asset) => {
    return db().queryFirstRow(
        "SELECT count(*) as burned FROM payments WHERE op_type = '1' AND asset LIKE ?",
        `${asset ?? ""}%:%`
    ).burned;
};

const getBurns = (asset) => {
    return db().query(
        "SELECT * FROM payments WHERE op_type = '1' AND asset LIKE ?",
        `${asset ?? ""}%:%`
    );
}

const getSwappedCount = (asset) => {
    return db().queryFirstRow(
        "SELECT count(*) as paid FROM payments WHERE op_type = '13' AND asset LIKE ?",
        `${asset ?? ""}%:%`
    ).paid;
}

const getSwaps = (asset) => {
    return db().query(
        "SELECT * FROM payments WHERE op_type = '13' AND asset LIKE ?",
        `${asset ?? ""}%:%`
    );
};

const matchAssets = (assetCode) => {
    return db().query(
        "SELECT DISTINCT asset FROM payments WHERE asset like ?",
        `${assetCode??""}%:%`
    )
        .map(row => {
            const rowData = row.asset.split(':');
            return {code: rowData[0], issuer: rowData[1]};
        });
};

const insertTweet = (tweetId, payment) => {
    return db().insert("tweets", {
        id: tweetId, latest_payment: payment
    });
};

module.exports = {
    database: db,
    getBurnedCount,
    getBurns,
    getLatestTweet,
    getScanCursor,
    getPaymentsCursor: getCursor,
    getSwappedCount,
    getSwaps,
    insertTweet,
    matchAssets
};
