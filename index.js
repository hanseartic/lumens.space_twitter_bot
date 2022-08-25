const BigNumber = require("bignumber.js");
const fetch = require("node-fetch");

const paymentLocatorUrl = "https://api.stellar.expert/explorer/public/payments?sort=id&limit=200&order=asc&memo="+encodeURI("stellarclaim:🗑💱💰");
const {database, getBurnedCount, getPaymentsCursor, getSwappedCount} = require("./db")
let shouldRun = true;

const fetchPayments = async (latestId) => {
    console.log("current cursor", latestId);
    return await fetch(paymentLocatorUrl + "&cursor=" + latestId)
        .then(res => res.json())
        .then( response => {
            const records = response["_embedded"]["records"];

            for (const r of records) {
                if (!shouldRun) {
                    break;
                }
                //process.stdout.write(".");
                database().insert(
                    "payments",
                    {
                        id: r.id,
                        timestamp: r.ts,
                        asset: r.source_asset.substring(-2),
                        sender: r.from,
                        receiver: r.to,
                        amount: (new BigNumber(r.amount).toNumber()),
                        op_type: r.optype,
                    }
                );
            }
            //process.stdout.write("\n");

            if (!shouldRun || response["_links"]["self"]["href"] === response["_links"]["next"]["href"]) {
                return;
            }
            const nextUrl = new URL(response["_links"]["next"]["href"], paymentLocatorUrl);
            return fetchPayments(nextUrl.searchParams.get("cursor"));
        })
        .catch(console.log);
};


const list = () => {
    const burnCount = getBurnedCount();
    const swappedCount = getSwappedCount();
    const total = new BigNumber(burnCount).plus(swappedCount);
    console.log({burns: burnCount, swaps: swappedCount, total: total.toNumber()});
};

let running = false;
const run = () => {
    if (running === true || database.inTransaction) {
        return;
    }
    running = true;
    const latestId = getPaymentsCursor();
    console.log("fetching new claimable balances");

    database().transaction(async () => {
        await fetchPayments(latestId)
            .then(() => console.log("synced all available claimable balances"))
        running = false;
        list();
        database().close();
    }).immediate();

    return run;
};

const main = () => {
    process.on('SIGINT', () => {
        console.log("Received SIGINT - stopping");
        shouldRun = false;
        clearInterval(timer);
    });
    process.on('exit', () => {
        database().close();
    });

    const timer = setInterval(run(), 15000);
};

main();

