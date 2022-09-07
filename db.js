const DB = require('better-sqlite3-helper');

const options = {
    path: './db.sql',
    WAL: true,
    migrate: {  // disable completely by setting `migrate: false`
//        force: 'last',
    },
};

const db = () => DB(options).defaultSafeIntegers(true);

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
        bindAsset(asset)
    ).burned;
};

const getBurns = (asset) => {
    return db().query(
        "SELECT * FROM payments WHERE op_type = '1' AND asset LIKE ?",
        bindAsset(asset)
    );
}

const getSwappedCount = (asset) => {
    return db().queryFirstRow(
        "SELECT count(*) as paid FROM payments WHERE op_type = '13' AND asset LIKE ?",
        bindAsset(asset)
    ).paid;
}

const getSwaps = (asset) => {
    return db().query(
        "SELECT * FROM payments WHERE op_type = '13' AND asset LIKE ?",
        bindAsset(asset)
    );
};

const matchAssets = (assetCode, strict) => {
    if (true === strict && bindAsset(assetCode) !== assetCode) {
        assetCode = assetCode + ":G%";
    }
    return db().query(
        "SELECT DISTINCT asset FROM payments WHERE asset like ?",
        bindAsset(assetCode)
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

const bindAsset = (asset) => {
    if (!asset) {
        return "%";
    }
    const parts = asset.split(':');
    if (parts.length === 1 || parts[1] === "") {
        return parts[0] + "%:%";
    }
    return asset;
}

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
