"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.notLocalLambda = exports.lambda = exports.s3 = void 0;
exports.detectSentiment = detectSentiment;
exports.detectSentimentBatch = detectSentimentBatch;
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const common_1 = require("@crowd/common");
const conf_1 = require("../conf");
let s3Instance;
let lambdaInstance;
let notLocalLambdaInstance;
if (conf_1.S3_CONFIG.aws) {
    s3Instance = conf_1.IS_DEV_ENV
        ? new aws_sdk_1.default.S3({
            s3ForcePathStyle: true,
            endpoint: `${conf_1.S3_CONFIG.host}:${conf_1.S3_CONFIG.port}`,
            apiVersion: '2012-10-17',
            accessKeyId: conf_1.S3_CONFIG.aws.accessKeyId,
            secretAccessKey: conf_1.S3_CONFIG.aws.secretAccessKey,
            region: conf_1.S3_CONFIG.aws.region,
        })
        : new aws_sdk_1.default.S3({
            accessKeyId: conf_1.S3_CONFIG.aws.accessKeyId,
            secretAccessKey: conf_1.S3_CONFIG.aws.secretAccessKey,
            region: conf_1.S3_CONFIG.aws.region,
            endpoint: conf_1.S3_CONFIG.aws.endpoint,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
        });
}
const comprehendInstance = conf_1.COMPREHEND_CONFIG.aws.accessKeyId
    ? new aws_sdk_1.default.Comprehend({
        accessKeyId: conf_1.COMPREHEND_CONFIG.aws.accessKeyId,
        secretAccessKey: conf_1.COMPREHEND_CONFIG.aws.secretAccessKey,
        region: conf_1.COMPREHEND_CONFIG.aws.region,
    })
    : undefined;
const ALLOWED_MAX_BYTE_LENGTH = 5000;
/**
 * Get sentiment for a text using AWS Comprehend
 * @param text Text to detect sentiment on
 * @returns Sentiment object
 */
async function detectSentiment(text) {
    // Only if we have proper credentials
    if (comprehendInstance) {
        // https://docs.aws.amazon.com/comprehend/latest/APIReference/API_DetectSentiment.html
        // needs to be utf-8 encoded
        text = Buffer.from(text).toString('utf8');
        // from docs - AWS performs analysis on the first 500 characters and ignores the rest
        text = text.slice(0, 500);
        // trim down to max allowed byte length
        text = (0, common_1.trimUtf8ToMaxByteLength)(text, ALLOWED_MAX_BYTE_LENGTH);
        const params = {
            LanguageCode: 'en',
            Text: text,
        };
        const fromAWS = await comprehendInstance.detectSentiment(params).promise();
        const positive = 100 * fromAWS.SentimentScore.Positive;
        const negative = 100 * fromAWS.SentimentScore.Negative;
        return {
            label: fromAWS.Sentiment.toLowerCase(),
            positive,
            negative,
            neutral: 100 * fromAWS.SentimentScore.Neutral,
            mixed: 100 * fromAWS.SentimentScore.Mixed,
            // Mapping the value from -1,1 to 0,100
            // Get a magnitude of the difference between the two values,
            // normalised by how much of the 4 dimensions they take:
            //   (positive - negative) / (positive + negative)
            //   Value between -1 and 1
            // Then scale it to 0,100
            sentiment: Math.round(50 + (positive - negative) / 2),
        };
    }
    return {};
}
async function detectSentimentBatch(textArray) {
    if (comprehendInstance) {
        const params = {
            LanguageCode: 'en',
            TextList: textArray,
        };
        const fromAWSBatch = await comprehendInstance.batchDetectSentiment(params).promise();
        const batchSentimentResults = fromAWSBatch.ResultList.map((i) => {
            const positive = 100 * i.SentimentScore.Positive;
            const negative = 100 * i.SentimentScore.Negative;
            return {
                label: i.Sentiment.toLowerCase(),
                positive,
                negative,
                neutral: 100 * i.SentimentScore.Neutral,
                mixed: 100 * i.SentimentScore.Mixed,
                sentiment: Math.round(50 + (positive - negative) / 2),
            };
        });
        return batchSentimentResults;
    }
    return {};
}
exports.s3 = s3Instance;
exports.lambda = lambdaInstance;
exports.notLocalLambda = notLocalLambdaInstance;
//# sourceMappingURL=aws.js.map