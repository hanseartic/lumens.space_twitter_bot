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
        .then(r => r.id);
}

const getBurnedCount = () => {
    return database?.get("SELECT count(*) as burned FROM payments WHERE op_type = '1'")
        .then(r => r.burned)
};

const getSwappedCount = () => {
    return database?.get("SELECT count(*) as paid FROM payments WHERE op_type = '13' ")
        .then(r => r.paid);
}

const getSwaps = () => {
    return database?.all("SELECT * from payments WHERE op_type = '13'");
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
    getSwaps
};
