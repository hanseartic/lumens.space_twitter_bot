const BigNumber = require("bignumber.js");
const {TwitterApi} = require("twitter-api-v2");
const twitterConfig = require("./twitter_config");
const {sumReducer} = require("./common");
const {getBurnedCount, database, getLatestTweet, getPaymentsCursor, getSwappedCount, getSwaps, insertTweet} = require("./db");

const twitterApi = new TwitterApi(twitterConfig);
const twitterClient = twitterApi.v2;

// log every 5 minutes if there was no update
const notifyAfter = 300;
let notifyAt;
let processing = false;

const shouldNotifyLogs = () => {
    const shouldI = new Date().getTime() >= (notifyAt??0);
    if (shouldI) {
        notifyAt =  new Date().getTime() + notifyAfter * 1000;
    }
    return shouldI;
}

const main = () => {
    const latestTweet = getLatestTweet();
    const cursor = getPaymentsCursor();

    if (cursor === latestTweet?.latest_payment) {
        return Promise.reject("Already published tweet for latest TX " + cursor);
    }

    processing = true;
    const swaps = getSwaps();
    const swapped = new BigNumber(getSwappedCount());
    const burned = new BigNumber(getBurnedCount());
    const total = swapped.plus(burned);
    const amount = swaps.map(swap => swap.amount).reduce(sumReducer);

    const status =
        `🚀 ${total.toFormat()} claimable balances of #spam assets have been cleaned with #stellarclaim:🗑💱💰!\n\n` +
        `🔥 ${burned.toFormat()} of them got burned\n` +
        `💱 ${swapped.toFormat()} have been converted into ~${amount.toFormat(3)} $XLM\n` +
        "\n" +
        "🧹 Clean your #stellar account off spam on https://balances.lumens.space/claim\n" +
        "\n" +
        "#StellarFamily #trashtocash #XLM";

/*
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log(status);
            resolve({data:{id:cursor}});
        }, 2000);
    })
 */
    return twitterClient
        .tweet(status)
        .then(result => result.data.id)
        .then(tweetId => confirmTweet(tweetId, cursor))
        .then(tweetId => console.log("sent tweet:", tweetId))
        .catch(console.warn)
        .finally(() => { processing = false; });
};

const confirmTweet = (tweetId, operationsCursor) => {
    insertTweet(tweetId, operationsCursor);
    return tweetId;
}

const intervalEntry = () => {
    if(!processing) {
        main()
            .then(() => { notifyAt = new Date().getTime() + 15000; })
            .catch(e => {
                if (shouldNotifyLogs()) {
                    console.log(e);
                }
            })
            .finally(() => database().close());
    }
    return intervalEntry;
}

const intervalID = setInterval(intervalEntry(), 1000);
process.on("SIGINT", () => {
    clearInterval(intervalID);
});
