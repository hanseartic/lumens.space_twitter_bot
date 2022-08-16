const twitterConfig = require("./twitter_config");
const {TwitterApi} = require("twitter-api-v2");
const {init: initDB, matchAssets, getSwappedCount, getSwaps, getBurnedCount} = require("./db");
const {sumReducer} = require("./common");
const BigNumber = require("bignumber.js");
const twitterApiApp = new TwitterApi(process.env.TWITTER_APP_BEARER_TOKEN);
const twitterBotApp = new TwitterApi(twitterConfig);
const twitterBotClient = twitterBotApp.v2;


const assetStats = (assetCode) => {
    return matchAssets(assetCode)
        .then(assets => Promise.all(assets.map(async asset => {
            const assetCode = `${asset.code}-${asset.issuer}`;
            return {...asset,
                swappedCount: await getSwappedCount(assetCode),
                burnedCount: await getBurnedCount(assetCode),
                swappedAmount: await getSwaps(assetCode).then(swaps => swaps.map(swap => swap.amount).reduce(sumReducer, new BigNumber(0)).toFormat())
            };
        })));
};
const shortIssuer = (issuer) => issuer.substring(0, 3) + '…' + issuer.substring(53)

const reactToMention = async (data) => {
    return Promise.all(data.entities.cashtags.map(cashtag => assetStats(cashtag.tag)))
        .then(assets => assets.flat())
        .then(assetStats => assetStats.map(assetStat => '' +
                `🧹 ${assetStat.code}*${shortIssuer(assetStat.issuer)} has been cleaned ${assetStat.swappedCount + assetStat.burnedCount} times:\n` +
                `🔥 ${assetStat.burnedCount} burns\n` +
                `💱 ${assetStat.swappedCount} swaps yielded ${assetStat.swappedAmount} $XLM\n\n` +
                '#stellarclaim \n\n' +
                `https://stellar.expert/explorer/public/asset/${assetStat.code}-${assetStat.issuer}`
        ));
};

let twitterStream;

const main = async () => {
    await initDB();

    const me = await twitterBotClient.me();
    console.log("Logged in to twitter as", me.data.username);

    twitterApiApp.v2.updateStreamRules({
        add: [
            {"value": "@" + me.data.username, "tag": "account mentions"},
            {"value": "has:cashtags", "tag": "cashtag"}
        ]
    }).then(() => {
        twitterApiApp.v2.searchStream({
            expansions: "entities.mentions.username,author_id",
            "tweet.fields": "entities"
        }).then(stream => {
            twitterStream = stream;
            stream.on('data event content', (event) => {
                if (event.matching_rules.find(r => r.tag === "account mentions")) {
                    if (event.data.author_id === me.data.id) {
                        return;
                    }
                    reactToMention(event.data).then(stati => {
                        if (stati.length === 0) {
                            twitterBotClient.tweet("I have not processed such asset(s).", {reply: {in_reply_to_tweet_id: event.data.id}});
                        }
                        stati.map(status => {
                            twitterBotClient.tweet(status, {reply: {in_reply_to_tweet_id: event.data.id}});
                        });
                    });
                }
            });
        });
    });
};


process.on("SIGINT", () => {
    console.log("bye");
    stopHorizonStream?.();
    twitterStream?.close();
});

main();
