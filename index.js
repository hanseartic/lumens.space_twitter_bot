const BigNumber = require("bignumber.js");
const fetch = require("node-fetch");

const paymentLocatorUrl = "https://api.stellar.expert/explorer/public/payments?sort=id&limit=200&order=asc&memo="+encodeURI("stellarclaim:🗑💱💰");
const {init: initDB, getBurnedCount, getPaymentsCursor, getSwappedCount} = require("./db")
let shouldRun = true;

const fetchPayments = async (latestId, db) => {
    await fetch(paymentLocatorUrl + "&cursor=" + latestId)
        .then(res => res.json())
        .then(async response => {
            const records = response["_embedded"]["records"];
            await Promise.all(records.map(async r => {
                console.log("processing", r.id);
                await db.run(
                    "INSERT INTO payments (id, timestamp, asset, sender, receiver, amount, op_type) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    r.id, r.ts, r.source_asset.substring(-2), r.from, r.to, (new BigNumber(r.amount).toNumber()), r.optype
                );
            }));
            if (!shouldRun || response["_links"]["self"]["href"] === response["_links"]["next"]["href"]) {
                return;
            }
            const nextUrl = new URL(response["_links"]["next"]["href"], paymentLocatorUrl);
            return fetchPayments(nextUrl.searchParams.get("cursor"), db);
        })
        .catch(console.log);
};


const list = () => {
    getBurnedCount().then(burnCount => console.log("burn", burnCount));
    getSwappedCount().then(swappedCount => console.log("swap", swappedCount))
};

const run = () => {
    initDB().then(async dab => {
        const latestId = await getPaymentsCursor();
        console.log("fetching new claimable balances. starting at cursor " + latestId);
        await fetchPayments(latestId, dab);
        console.log("synced all available claimable balances");
        await list();
        dab.close();
    });
};

run();
const timer = setInterval(run, 30000);
process.on('SIGINT', () => {
    console.log("Received SIGINT - stopping");
    shouldRun = false;
    clearInterval(timer);
});
