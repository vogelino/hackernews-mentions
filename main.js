require('dotenv').config();
const stream = require('stream');
const ndjson = require('ndjson');
const through2 = require('through2');
const request = require('request');
const filter = require('through2-filter');
const sentiment = require('sentiment');
const util = require('util');
const pipeline = util.promisify(stream.pipeline);
const { MongoClient } = require('mongodb');

const arguments = process.argv ;
const SEARCH_KEYWORD = arguments[2] || 'Berlin';

const connectToMongoDB = async () => {
	const textRank = new sentiment();
	const client = new MongoClient(process.env.ATLAS_URI, {
		useUnifiedTopology: true,
	});
	try {
		await client.connect();
		const collection = client
			.db(process.env.ATLAS_DB_NAME)
			.collection(process.env.ATLAS_COLLECTION_NAME);
		console.log(`Start searching hackernews for articles about "${SEARCH_KEYWORD}"`);
		await pipeline(
			request(process.env.HACKERNEWS_STREAM_API_URL),
			ndjson.parse({ strict: false }),
			filter({ objectMode: true }, (chunk) => {
				const predicate =
					chunk.body.toLowerCase().includes(SEARCH_KEYWORD) ||
					chunk['article-title'].toLowerCase().includes(SEARCH_KEYWORD);
				if (!predicate) {
					console.log('❌: ', chunk['article-title']);
					return false;
				}
				console.log('✅: ', chunk['article-title']);
				return true;
			}),
			through2.obj((row, enc, next) => {
				let result = textRank.analyze(row.body);
				row.score = result.score;
				next(null, row);
			}),
			through2.obj((row, enc, next) => {
				collection.insertOne({
					...row,
					'user-url': `https://news.ycombinator.com/user?id=${row['author']}`,
					'item-url': `https://news.ycombinator.com/item?id=${row['article-id']}`,
				});
				next();
			}),
		);
		console.log('FINISHED');
	} catch (err) {
		console.log(err);
	}
};

connectToMongoDB();
