const {init: initDB, getSwaps} = require("./db.js");
const BigNumber = require("bignumber.js");
const {TwitterApi} = require("twitter-api-v2");
const twitterConfig = require("./twitter_config");
const {sumReducer} = require("./common");
const {confirmTweet, getSwappedCount, getBurnedCount, getLatestTweet, getPaymentsCursor} = require("./db");

const twitterApi = new TwitterApi(twitterConfig);
const twitterClient = twitterApi.v2;

const main = async () => {
    await initDB();

    const latestTweet = await getLatestTweet();
    const cursor = await getPaymentsCursor();
    if (cursor === latestTweet?.latest_payment) {
        console.log("Already published for latest TX");
        return;
    }

    const swaps = await getSwaps();
    const swapped = new BigNumber(await getSwappedCount());
    const burned = new BigNumber(await getBurnedCount());
    const total = swapped.plus(burned);
    const amount = swaps.map(swap => swap.amount).reduce(sumReducer);

    const status =
        `🚀 ${total.toFormat()} claimable balances have been cleaned with #stellarclaim:🗑💱💰!\n\n` +
        `🔥 ${burned.toFormat()} of them got burned\n` +
        `💱 ${swapped.toFormat()} have been converted into ~${amount.toFormat(3)} $XLM\n` +
        "\n" +
        "🧹 Clean your #stellar account off spam on https://balances.lumens.space/claim\n" +
        "\n" +
        "#StellarFamily #trashtocash #XLM";

    twitterClient.tweet(status)
        .then(result => {
            console.log("sent tweet:", result.data.id);
            confirmTweet(result.data.id, cursor);
        });
};

main();
process.on("SIGINT", () => {});
