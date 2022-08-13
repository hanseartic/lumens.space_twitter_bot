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

const getPaymentsCount = () => {
    return database?.get("SELECT  count(*) as paid FROM payments WHERE op_type = '13' ")
        .then(r => r.paid);
}

module.exports = {
    init,
    getPaymentsCursor: getCursor,
    getPaymentsCount,
    getBurnedCount
};
