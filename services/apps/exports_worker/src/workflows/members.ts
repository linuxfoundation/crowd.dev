import { proxyActivities } from '@temporalio/workflow'

import { ITriggerCSVExport } from '@crowd/types'

import * as activities from '../activities'
import { ResultS3Upload } from '../types/s3'

// Configure timeouts and retry policies related to CSV and S3.
const { buildAndUploadMembersCSV, getPresignedUrl } = proxyActivities<typeof activities>({
  startToCloseTimeout: '60 seconds',
  retry: {
    maximumAttempts: 5,
  },
})

// Configure timeouts and retry policies to send emails.
const { exportSendEmail } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 seconds',
  retry: {
    maximumAttempts: 5,
  },
})

/*
exportMembersToCSV is a Temporal workflow:
  - [Activity]: Build and upload the CSV to S3 containing all members returned
    with the given filters.
  - [Activity]: If first activity is a success, get a pre-signed S3 URL.
  - [Activity]: If first activity is not a success, decrement usage count of the
    tenant.
  - [Activity]: Send an email to the user. If the first activity is a success the
    email will contain a link. Otherwise, the link will be "null" and the dynamic
    template in SendGrid will fallback to an error message in the email instead
    displaying the link.
*/
export async function exportMembersToCSV(input: ITriggerCSVExport): Promise<void> {
  let hasFailed = false

  let upload: ResultS3Upload
  try {
    upload = await buildAndUploadMembersCSV(input)
  } catch {
    hasFailed = true
  }

  let result: ResultS3Upload
  if (!hasFailed) {
    try {
      result = await getPresignedUrl(upload)
    } catch {
      hasFailed = true
    }
  }

  try {
    await exportSendEmail({
      sendTo: input.sendTo,
      link: result ? result.link : null,
    })
  } catch (err) {
    throw new Error(err)
  }
}
