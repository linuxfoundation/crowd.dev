import AWS from 'aws-sdk'

import { trimUtf8ToMaxByteLength } from '@crowd/common'

import { COMPREHEND_CONFIG, IS_DEV_ENV, S3_CONFIG } from '../conf'

let s3Instance

if (S3_CONFIG.aws) {
  s3Instance = IS_DEV_ENV
    ? new AWS.S3({
        s3ForcePathStyle: true,
        endpoint: `${S3_CONFIG.host}:${S3_CONFIG.port}`,
        apiVersion: '2012-10-17',
        accessKeyId: S3_CONFIG.aws.accessKeyId,
        secretAccessKey: S3_CONFIG.aws.secretAccessKey,
        region: S3_CONFIG.aws.region,
      })
    : new AWS.S3({
        accessKeyId: S3_CONFIG.aws.accessKeyId,
        secretAccessKey: S3_CONFIG.aws.secretAccessKey,
        region: S3_CONFIG.aws.region,
        endpoint: S3_CONFIG.aws.endpoint,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
      })
}

const comprehendInstance = COMPREHEND_CONFIG.aws.accessKeyId
  ? new AWS.Comprehend({
      accessKeyId: COMPREHEND_CONFIG.aws.accessKeyId,
      secretAccessKey: COMPREHEND_CONFIG.aws.secretAccessKey,
      region: COMPREHEND_CONFIG.aws.region,
    })
  : undefined

const ALLOWED_MAX_BYTE_LENGTH = 5000

/**
 * Get sentiment for a text using AWS Comprehend
 * @param text Text to detect sentiment on
 * @returns Sentiment object
 */
export async function detectSentiment(text) {
  // Only if we have proper credentials
  if (comprehendInstance) {
    // https://docs.aws.amazon.com/comprehend/latest/APIReference/API_DetectSentiment.html
    // needs to be utf-8 encoded
    text = Buffer.from(text).toString('utf8')
    // from docs - AWS performs analysis on the first 500 characters and ignores the rest
    text = text.slice(0, 500)
    // trim down to max allowed byte length
    text = trimUtf8ToMaxByteLength(text, ALLOWED_MAX_BYTE_LENGTH)

    const params = {
      LanguageCode: 'en',
      Text: text,
    }
    const fromAWS = await comprehendInstance.detectSentiment(params).promise()
    const positive = 100 * fromAWS.SentimentScore.Positive
    const negative = 100 * fromAWS.SentimentScore.Negative
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
    }
  }
  return {}
}

export async function detectSentimentBatch(textArray) {
  if (comprehendInstance) {
    const params = {
      LanguageCode: 'en',
      TextList: textArray,
    }
    const fromAWSBatch = await comprehendInstance.batchDetectSentiment(params).promise()

    const batchSentimentResults = fromAWSBatch.ResultList.map((i) => {
      const positive = 100 * i.SentimentScore.Positive
      const negative = 100 * i.SentimentScore.Negative
      return {
        label: i.Sentiment.toLowerCase(),
        positive,
        negative,
        neutral: 100 * i.SentimentScore.Neutral,
        mixed: 100 * i.SentimentScore.Mixed,
        sentiment: Math.round(50 + (positive - negative) / 2),
      }
    })

    return batchSentimentResults
  }
  return {}
}

export const s3 = s3Instance
export const lambda = undefined
export const notLocalLambda = undefined
