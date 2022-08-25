const {Server} = require("stellar-sdk");
const loopcall = require("@cosmic-plus/loopcall");
const db = require("./db");
const worker = require("node:worker_threads");
const BigNumber = require("bignumber.js");

const server = new Server("https://horizon.stellar.org");

const initialCursor = db.getScanCursor()??"now";
let maxCursor;
let currentCursor;
let lag = 0;

const baseCall = (cursor) => {
    return server.operations()
        //.join("transactions")
        .limit(200)
        .order("asc")
        .cursor(cursor);
}

const getAssetFrom = record => {
    if (record.type === "payment") {
        return getAssetTo(record);
    }
    return record.source_asset_type === 'native'? "XLM:native" : `${record.source_asset_code}:${record.source_asset_issuer}`;
};

const getAssetTo = record => {
    return record.asset_type === "native" ? "XLM:native" : `${record.asset_code}:${record.asset_issuer}`;
}

const cursorBroadcast = new worker.BroadcastChannel("cursors");

const main = async (cursor) => {
    let shouldLoop = true;
    currentCursor = cursor;
    let tx_ts;
    while (shouldLoop) {
         await loopcall(baseCall(currentCursor), {
            limit: 100,
            filter: (record) => {
                return record.type === "payment" || record.type === "path_payment_strict_receive" || record.type === "path_payment_strict_send"
            },
            iterate: async (record) => {
                if (worker.isMainThread && currentCursor === "now") {
                    cursorBroadcast.postMessage(record.id);
                }
                if (!worker.isMainThread) {
                    if (maxCursor && maxCursor <= currentCursor) {
                        cursorBroadcast.postMessage(currentCursor);
                        throw worker.workerData + ": gap closed";
                    }
                }

                currentCursor = record.id;
                const currentRecord = {
                    id: record.id,
                    tx_id: record.transaction_hash,
                    timestamp: record.created_at,
                    memo: record.transaction_attr?.memo??null,
                    from_account: record.from,
                    to_account: record.to,
                    from_asset: getAssetFrom(record),
                    to_asset: getAssetTo(record),
                    from_amount: record.source_amount??record.amount,
                    to_amount: record.amount,
                    is_burn: (record.asset_type !== "native" && record.asset_issuer === record.to)?1:0,
                };
                for (const [k, _] of Object.entries(record)) {
                    delete (record[k]);
                }
                record.id = currentRecord.id;

                if (tx_ts !== currentRecord.timestamp) {
                    const now = new Date();
                    const ledgerTs =  new Date(currentRecord.timestamp);
                    lag = (now - ledgerTs) / 1000;
                    //console.log({ledgerTs, ingested: now, lag, worker: (worker.isMainThread?false:worker.workerData)});
                    tx_ts = currentRecord.timestamp;
                }
                try {
                    //console.log("match", currentRecord.id);
                    server.transactions().transaction(currentRecord.tx_id).call()
                        .then(tx => {
                            currentRecord.memo = tx.memo;
                            if (currentRecord.memo === "stellarclaim:🗑💱💰") {
                                db.database().insert(
                                    "payments",
                                    {
                                        id: currentRecord.id,
                                        timestamp: currentRecord.timestamp,
                                        asset: currentRecord.from_asset,
                                        sender: currentRecord.from_account,
                                        receiver: currentRecord.to_account,
                                        amount: currentRecord.to_amount,
                                        op_type: currentRecord.is_burn ? 13 : 1,
                                    }
                                );
                            }
                            if (currentRecord.is_burn) {
                                try {
                                    db.database().insert('operations', currentRecord);
                                } catch(e) {
                                    if (worker.isMainThread) {
                                        throw e;
                                    }
                                    process.exit(0);
                                }
                            }
                        });
                } catch (e) {
                    throw new Error("DB error");
                }
            },
        })
            .then(() => {
                if (worker.isMainThread) {
                    if (currentCursor !== "now") {
                        cursorBroadcast.postMessage(currentCursor);
                        if (lag > 25) {
                            spawnWorker(currentCursor);
                            currentCursor = "now";
                        } else {
                            //console.log({currentCursor, lag});
                        }
                    }
                } else {
                    if (maxCursor && maxCursor <= currentCursor) {
                        worker.parentPort.postMessage(`${worker.workerData} reached ${currentCursor} of ${maxCursor}`);
                        shouldLoop = false;
                    }
                }
            })
            .catch(e => {
                console.log(e)
                shouldLoop = false;
            });
        if (currentCursor === cursor) {
            if (worker.isMainThread) {
                //console.log("up-to-date")
            }
        }
        cursor = currentCursor;
    }
};

process.on("exit", () => {
    if (!worker.isMainThread) {
        db.database().delete('opscan', {worker: worker.threadId});
        console.log("bye from worker", worker.threadId, worker.workerData);
    }
    db.database().close();
});

const spawnWorker = (cursor) => {
    const w = new worker.Worker(__filename, {workerData: cursor});
    w.on("message", console.log);
};

if (worker.isMainThread) {
    console.log("Starting at " + initialCursor);
    main(initialCursor);
    db.database().query('SELECT * FROM opscan').map(thread => {
        if (new BigNumber(thread.cursor_from).eq(initialCursor)) {
            db.database().delete('opscan', { cursor_from: initialCursor });
        } else {
            console.log("found old worker");
            spawnWorker(thread.cursor_from);
        }
    });
} else {
    let workerCursor = worker.workerData;
    const threads = db.database().query('SELECT * FROM opscan');
    let workerInfo = threads.find(row => {
        const c = new BigNumber(worker.workerData);
        return c.gte(row.cursor_from) && (!row.cursor_to || c.lt(row.cursor_to));
    });
    if (workerInfo) {
        if (workerInfo.worker < worker.threadId) {
            console.log("somebody is already working on this");
            cursorBroadcast.close();
            return;
        }
        workerCursor = workerInfo.cursor_from;
        if (workerInfo.cursor_to) {
            maxCursor = workerInfo.cursor_to;
        }
        db.database().update('opscan', {worker: worker.threadId}, {cursor_from: worker.workerData});
    } else {
        workerInfo = db.database().queryFirstRow('SELECT * FROM opscan WHERE worker = ?', worker.threadId);
        if (!workerInfo) {
            db.database().insert('opscan', {cursor_from: worker.workerData, worker: worker.threadId});
        } else {
            console.log("spawnception - ignore");
        }
    }
    cursorBroadcast.onmessage = event => {
        if (!maxCursor) {
            maxCursor = event.data;
            db.database().update('opscan', {cursor_to: maxCursor}, {worker: worker.threadId});
        }
        try {
            db.database().update('opscan', {cursor_from: currentCursor}, {worker: worker.threadId});
        } catch {}
        worker.parentPort.postMessage(`worker: ${worker.workerData} <=> ${maxCursor} (currently at ${currentCursor} with lag of ${lag})`);
    };
    console.log("filling the gap from", workerCursor)
    main(workerCursor)
        .catch(console.warn)
        .finally(() => {
            cursorBroadcast.close();
        });
}
