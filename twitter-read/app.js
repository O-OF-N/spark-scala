const Twitter = require('Twitter');
const Config = require('./config');
const FS = require('fs');
const readTweets = function (track) {
    const client = new Twitter({
        consumer_key: Config.consumer_key,
        consumer_secret: Config.consumer_secret,
        access_token_key: Config.access_token_key,
        access_token_secret: Config.access_token_secret
    });
    try {
        const stream = client.stream('statuses/filter', { track });
        stream.on('data', function (event) {
            FS.appendFile('./data/tweets.txt', event.text+']]]]\n', function (err) {
                err ? console.log(err) : console.log('successfully written');
            });
        });
        stream.on('err', function (err) {
            throw err;
        })
    } catch (err) {
        console.log(err);
    }
};
readTweets('modi');
