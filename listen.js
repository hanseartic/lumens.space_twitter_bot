const twitterConfig = require("./twitter_config");
const {TwitterApi} = require("twitter-api-v2");
const {matchAssets, getSwappedCount, getSwaps, getBurnedCount, getBurns} = require("./db");
const {sumReducer} = require("./common");
const BigNumber = require("bignumber.js");
const twitterApiApp = new TwitterApi(process.env.TWITTER_APP_BEARER_TOKEN);
const twitterBotApp = new TwitterApi(twitterConfig);
const twitterBotClient = twitterBotApp.v2;


const assetStats = (assetCode) => {
    return matchAssets(assetCode, true)
        .map(asset => {
            const assetCode = `${asset.code}:${asset.issuer}`;
            return {...asset,
                swappedCount: getSwappedCount(assetCode),
                burnedCount: getBurnedCount(assetCode),
                swappedAmount: getSwaps(assetCode).map(swap => swap.amount).reduce(sumReducer, new BigNumber(0)).toFormat(),
                burnedAmount: getBurns(assetCode).map(burn => burn.amount).reduce(sumReducer, new BigNumber(0)).toFormat()
            };
        });
};
const shortIssuer = (issuer) => issuer.substring(0, 3) + '…' + issuer.substring(53)

const hashtagsToCashtags = (hashtags) => {
    const matches = [...new Set(
        hashtags
        .filter(hashtag => hashtag.tag.length > 6 && hashtag.tag.length <= 12)
        .map(hashtag => matchAssets(hashtag.tag, false).map(a => a.code))
        .flat()
    )];

    return matches.filter(match => hashtags.map(hashtag => hashtag.tag.toLowerCase()).includes(match.toLowerCase()))
        .map(match => ({tag: match}))
};

const renderTag = (tag) => {
    return (tag.length < 6 ? '$' : '#') + tag;
};

const cashtagsToTweets = (cashtags) => {
    return cashtags.map(cashtag => assetStats(cashtag.tag))
        .flat()
        .map(assetStat => '' +
            `🧹 ${assetStat.code} (by ${shortIssuer(assetStat.issuer)}) has been cleaned ${assetStat.swappedCount + assetStat.burnedCount} times on #stellar network:\n` +
            `🔥 ${assetStat.burnedCount} burns` + ((assetStat.burnedAmount === "0") ? "\n" : ` burned ${assetStat.burnedAmount} ${renderTag(assetStat.code)}\n`) +
            `💱 ${assetStat.swappedCount} swaps yielded ${assetStat.swappedAmount} $XLM\n\n` +
            '#trashtocash #stellarclaim\n\n' +
            `https://stellar.expert/explorer/public/asset/${assetStat.code}-${assetStat.issuer}`
        );
}

const getCashtags = (data, repliedTo) => {
    return (((data.entities.cashtags?.length)
        ? data.entities.cashtags
        : repliedTo?.entities.cashtags) ?? [])
        .concat(hashtagsToCashtags({...repliedTo?.entities, ...data.entities}.hashtags ?? []));
};

let twitterStream;

const main = async () => {

    const me = await twitterBotClient.me();
    console.log("Logged in to twitter as", me.data.username, me.data.id);

    const rules = await twitterApiApp.v2.streamRules();
    console.log(rules.data.map(r => r.id))
    await twitterApiApp.v2.updateStreamRules({
        delete: {ids: rules.data.map(r => r.id) }
    });
    console.log("clear")
    const rulesUpdated = await twitterApiApp.v2.updateStreamRules({
        add: [
            {"value": "@" + me.data.username, "tag": "account mentions"},
            //{"value": "has:cashtags", "tag": "cashtag"}
        ]
    });
    console.log("Rules updated", rulesUpdated.meta);

    twitterApiApp.v2.searchStream({
        expansions: "entities.mentions.username,author_id,referenced_tweets.id",
        "tweet.fields": "entities"
    }).then(stream => {
        twitterStream = stream;
        stream.on('data event content', (event) => {
            if (event.matching_rules.find(r => r.tag === "account mentions")) {
                if (event.data.author_id === me.data.id) {
                    console.log("Not reacting to myself");
                    return;
                }
                if (event.data.referenced_tweets?.find(r => r.type === "retweeted")) {
                    console.log("Not replying to retweet", event.data.id);
                    return;
                }
                console.log("Replying to", event.data.id);
                const dontReplyToUsers = event.includes.users.map(u => u.id);

                const cashtags = getCashtags(event.data, getReplyFromIncludes(event));
                if (cashtags.length === 0) {
                    twitterBotClient.tweet(
                        "🤷 I have not processed such asset(s).",
                        {reply: {in_reply_to_tweet_id: event.data.id, exclude_reply_user_ids: dontReplyToUsers}}
                    );
                } else {
                    const stati = cashtagsToTweets(cashtags);
                    // in order to make this a thread, the reply-to id must be updated after each post
                    let replyTo = event.data.id;
                    (async () => {
                        for (const status of stati) {
                            const tweetStatus = await twitterBotClient.tweet(
                                status,
                                {reply: {in_reply_to_tweet_id: replyTo, exclude_reply_user_ids: dontReplyToUsers}}
                            );
                            replyTo = tweetStatus.data.id;
                        }
                    })();
                }
            }
        });
    });
};

const getReplyFromIncludes = event => {
    const isReplyTo = event.data.referenced_tweets.find(ref => ref.type === 'replied_to')?.id;
    return event.includes.tweets?.find(tweet => tweet.id === isReplyTo);
}

process.on("SIGINT", () => {
    console.log("bye");
    twitterStream?.close();
});

main();
