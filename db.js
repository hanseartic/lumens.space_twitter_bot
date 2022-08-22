const DB = require('better-sqlite3-helper');
DB({
    path: './db.sql',
    WAL: true,
    migrate: {  // disable completely by setting `migrate: false`
//        force: 'last',
    }
});


const getCursor = () => {
    return DB().queryFirstRow("SELECT id FROM payments ORDER BY id DESC LIMIT 1")?.id;
};

const getLatestTweet = () => {
    return DB().queryFirstRowObject("SELECT * FROM tweets ORDER BY timestamp DESC LIMIT 1");
};

const getBurnedCount = (asset) => {
    return DB().queryFirstRow(
        "SELECT count(*) as burned FROM payments WHERE op_type = '1' AND asset LIKE ?",
        `${asset ?? "%"}-%`
    ).burned;
};

const getBurns = (asset) => {
    return DB().query(
        "SELECT * FROM payments WHERE op_type = '1' AND asset LIKE ?",
        `${asset ?? "%"}-%`
    );
}

const getSwappedCount = (asset) => {
    return DB().queryFirstRow(
        "SELECT count(*) as paid FROM payments WHERE op_type = '13' AND asset LIKE ?",
        `${asset ?? "%"}-%`
    ).paid;
}

const getSwaps = (asset) => {
    return DB().query(
        "SELECT * FROM payments WHERE op_type = '13' AND asset LIKE ?",
        `${asset ?? "%"}-%`
    );
};

const matchAssets = (assetCode) => {
    return DB().query(
        "SELECT DISTINCT asset FROM payments WHERE asset like ?",
        `${assetCode??"%"}-%`
    )
        .map(row => {
            const rowData = row.asset.split('-');
            return {code: rowData[0], issuer: rowData[1]};
        });
};

const confirmTweet = (tweetId, payment) => {
    return DB().insert("tweets", {
        id: tweetId, latest_payment: payment
    });
};

module.exports = {
    database: DB(),
    confirmTweet,
    getBurnedCount,
    getBurns,
    getLatestTweet,
    getPaymentsCursor: getCursor,
    getSwappedCount,
    getSwaps,
    matchAssets
};
