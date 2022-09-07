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

const workerThreads = [];

const baseCall = (cursor) => {
    const numberCursor = new BigNumber(cursor);
    return server.operations()
        //.join("transactions")
        .limit(200)
        .order("asc")
        .cursor(numberCursor.isNaN() ? 'now' : numberCursor.toString());
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
                const threadId = worker.isMainThread ? 0 : worker.threadId;
                currentCursor = BigInt(record.id);
                cursorBroadcast.postMessage({from: cursor, to: maxCursor, current: record.id, id: threadId});
                if (!worker.isMainThread) {
                    const {cursor_to} = db.database().queryFirstRowObject('SELECT * FROM opscan WHERE cursor_from = ?', worker.workerData);
                    db.database().update('opscan', {cursor_current: currentCursor, last_update_ts: Date.now()/1000}, {cursor_from: worker.workerData});
                    if (cursor_to && cursor_to <= currentCursor) {
                        throw worker.threadId + ": gap closed at " + currentCursor + " of " + cursor_to;
                    }
                } else {
                    db.database().update('opscan', {
                        cursor_from: currentCursor,
                        cursor_current: currentCursor,
                        last_update_ts: Date.now() / 1000
                    }, {worker: 0, cursor_from: 0}) ||
                    db.database().update('opscan', {
                        cursor_current: currentCursor,
                        last_update_ts: Date.now() / 1000
                    }, {worker: 0});
                }
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
                                        op_type: currentRecord.is_burn ? 1 : 13,
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
                        if (lag > 25) {
                            spawnWorker(currentCursor);
                            db.database().update('opscan', {cursor_from: 0, cursor_current: currentCursor, last_update_ts: Date.now()/1000}, {worker: 0});
                            currentCursor = "now";
                        } else {
                            db.database().query("SELECT worker, cursor_from, datetime(last_update_ts, 'unixepoch', 'localtime') as time from opscan where last_update_ts < unixepoch() - 300")
                                .map(staleWorker => {
                                    if (workerThreads[staleWorker.worker]) {
                                        console.log(`Worker ${staleWorker.worker} has not updated in 5 minutes. Restarting`);
                                        workerThreads[staleWorker.worker]?.terminate().then(() => {
                                            console.log(`${staleWorker.worker} stopped.`);
                                            delete workerThreads[staleWorker.worker];
                                            spawnWorker(staleWorker.cursor_from, false);
                                        });
                                    }
                                });
                        }
                    }
                } else {
                    if (!maxCursor) {
                        return;
                    }
                    if (new BigNumber(maxCursor).lte(currentCursor)) {
                        worker.parentPort.postMessage(`${worker.workerData} reached ${currentCursor} of ${maxCursor}`);
                        shouldLoop = false;
                        return;
                    }
                    const {ts_prev, id_prev} = db.database().queryFirstRowObject('SELECT timestamp as ts_prev, id as id_prev FROM operations JOIN (SELECT MAX(id) AS max_id FROM operations JOIN (SELECT cursor_current from opscan WHERE worker = ? LIMIT 1) WHERE id < cursor_current) WHERE id = max_id', worker.threadId);
                    const {ts_next, id_next} = db.database().queryFirstRowObject('SELECT timestamp as ts_next, id as id_next FROM operations JOIN (SELECT MIN(id) AS min_id FROM operations JOIN (SELECT cursor_to from opscan WHERE worker = ? LIMIT 1) WHERE id > cursor_to) WHERE id = min_id', worker.threadId);
                    if (ts_next && ts_prev) {
                        const nextOpTs = new Date(ts_next);
                        const prevOpTs = new Date(ts_prev);

                        const diff = nextOpTs.getTime()-prevOpTs.getTime();
                        if (diff > 1000 * 60 * 60) {
                            const wCursor = new BigNumber(id_prev).plus(id_next).idiv(2).toString();
                            spawnWorker(wCursor);
                        }
                    }
                }
            })
            .catch(e => {
                console.log(e);
                shouldLoop = false;
            });
        if (currentCursor === cursor) {
            if (worker.isMainThread) {
                await new Promise(res => {
                    setTimeout(() => res(), 3000);
                });

                console.log(`up-to-date @${currentCursor}`)
            }
        }
        cursor = currentCursor;
    }
};

process.on("exit", () => {
    if (!worker.isMainThread) {
        worker.parentPort.postMessage(`bye from worker ${worker.threadId} (${worker.workerData})`);
        try {
            db.database().delete('opscan', ['cursor_current >= cursor_to AND cursor_from = ?', worker.workerData]);
        } catch (e) {
            worker.parentPort.postMessage(e);
        }
    }
    db.database().close();
});

const spawnWorker = (cursor, fromChild) => {
    const {count} = db.database().queryFirstRowObject('SELECT count(worker) as count from opscan');
    if (!worker.isMainThread) {
        worker.parentPort.postMessage({spawn: cursor, worker: worker.threadId});
        return true;
    }
    if (count > 20 && fromChild) {
        //console.log("enough workers running");
        return false;
    }

    const w = new worker.Worker(__filename, {workerData: cursor});
    workerThreads[w.threadId] = w;

    w.on('message', m => {
        if (worker.isMainThread) {
            if (m.spawn) {
                if (spawnWorker(m.spawn, true)) {
                    console.log(`Spawned new worker @${m.spawn} as per worker ${m.worker} request`);
                }
            } else if (-1 !== m.indexOf("bye from worker") || -1 !== m.indexOf("shortening own range")) {
                const workers = {};
                db.database().query('SELECT * FROM opscan').map(w => {
                    const range = new BigNumber(w.cursor_to ?? Number.MAX_SAFE_INTEGER).minus(w.cursor_from);
                    const progress = new BigNumber(w.cursor_current).minus(w.cursor_from);
                    const r = (({ worker, last_update_ts, ...o }) => o)({
                        ...w,
                        progress: new BigNumber(100).div(range).times(progress).decimalPlaces(0, BigNumber.ROUND_DOWN).toNumber(),
                        updated: new Date(new BigNumber(w.last_update_ts).times(1000).toNumber()).toLocaleTimeString(),
                    });
                    workers[new BigNumber(w.worker).toString()] = r;
                });
                console.table(workers);
            } else {
                console.log(m);
            }
        } else {
            console.log("this is impossible")
            worker.parentPort.postMessage(m);
        }
    });
    w.on('error', e => {
        console.dir(e, {depth: 10})
    });
};

if (worker.isMainThread) {
    console.log("Starting at " + initialCursor);
    db.database().delete('opscan', {worker: 0});
    db.database().insert('opscan', {worker: 0, cursor_from: initialCursor, cursor_current: initialCursor, last_update_ts: Date.now()/1000});
    main(initialCursor)
        .finally(() => {
            db.database().close();
            cursorBroadcast.close();
            process.exit(0);
        });

    const workers = db.database().query('SELECT * FROM opscan WHERE worker > 0 ORDER BY cursor_from');
    workers.map(thread => {
        if (new BigNumber(thread.cursor_from).eq(initialCursor)) {
            db.database().delete('opscan', { cursor_from: thread.cursor_from });
        } else {
            setTimeout(() => spawnWorker(thread.cursor_from), 1000);
        }
    });
} else {
    let workerCursor = worker.workerData;
    const workers = db.database().query('SELECT * FROM opscan');
    const currentWorkerFrom = new BigNumber(workerCursor);
    let workerInfo = workers.find(row => {
        return currentWorkerFrom.eq(row.cursor_from) && (!row.cursor_to || currentWorkerFrom.lt(row.cursor_to));
    });
    if (workerInfo) {
        if (workerInfo.cursor_to) {
            maxCursor = workerInfo.cursor_to;
        }
        if (workerInfo.cursor_current) {
            workerCursor = workerInfo.cursor_current;
            if (new BigNumber(workerInfo.cursor_current).gte(workerInfo.cursor_to)) {
                console.log(
                    workerInfo.worker + " " +
                    workerInfo.cursor_from + ": worker over range - deleting: ",
                    db.database().delete('opscan', ['cursor_current >= cursor_to AND cursor_from = ?', workerInfo.cursor_from])
                );
                return;
            }
        }
        db.database().update('opscan', {worker: worker.threadId, last_update_ts: Date.now()/1000}, {cursor_from: worker.workerData});
    } else {
        workerInfo = db.database().queryFirstRow('SELECT * FROM opscan WHERE cursor_from = ?', worker.workerData);
        if (!workerInfo) {
            db.database().insert('opscan', {cursor_from: worker.workerData, worker: worker.threadId, cursor_current: worker.workerData, last_update_ts: Date.now()/1000});
        } else {
            console.log("spawnception - ignore");
            return;
        }
    }
    cursorBroadcast.onmessage = event => {
        if (event.data.id === worker.threadId) {
            worker.parentPort.postMessage("ignore my own event " + worker.threadId)
            return;
        }
        if (event.data.from === 'now') {
            return;
        }
        const otherThreadStart = new BigNumber(event.data.from);
        let newMax = maxCursor;
        if (otherThreadStart.lte(currentWorkerFrom)) {
            return;
        }
        if (!maxCursor) {
            newMax = event.data.from;
        } else {
            if (otherThreadStart.gt(currentWorkerFrom) && otherThreadStart.lt(maxCursor)) {
                newMax = otherThreadStart.minus(1).toString();
            }
        }
        if (newMax !== maxCursor) {
            worker.parentPort.postMessage(`${worker.threadId} shortening own range from ${maxCursor} to ${newMax}`);
            maxCursor = newMax;
            db.database().update('opscan', {cursor_to: maxCursor, last_update_ts: Date.now()/1000}, {cursor_from: worker.workerData});
        }
        //worker.parentPort.postMessage(`worker (${worker.threadId}): ${worker.workerData} <=> ${maxCursor} (currently at ${currentCursor} with lag of ${lag})`);
    };
    worker.parentPort.postMessage(`${worker.threadId} filling the gap from ${workerCursor} (started at ${worker.workerData})`);
    main(workerCursor)
        .then(() => {
            worker.parentPort.postMessage("worker " + worker.threadId + " done");
            //db.database().delete('opscan', {cursor_from: worker.workerData});
        })
        .catch(e => {
            worker.parentPort.postMessage(`worker ${worker.threadId} had an issue: '${e}'`);
        })
        .finally(() => {
            cursorBroadcast.close();
        });
}
