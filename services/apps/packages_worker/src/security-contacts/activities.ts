import { heartbeat } from '@temporalio/activity'

import { getServiceChildLogger } from '@crowd/logging'

import { getReportingProtocolConfig, getSecurityContactsConfig } from '../config'
import { getCdpDb, getPackagesDb } from '../db'

import { githubApiGet } from './githubToken'
import { IngestSingleResult, ingestSecurityContactsForPurl } from './ingestSingle'
import { BatchResult, processBatch } from './processBatch'
import { runAssembleStage } from './protocol/assembleStage'
import { fetchLinkedPage } from './protocol/fetchContent'
import { llmExtractProtocol } from './protocol/llmExtract'
import { runParseStage } from './protocol/parseStage'
import { AssembleStageResult, ParseStageResult } from './protocol/types'

const log = getServiceChildLogger('security-contacts-activity')

export async function processSecurityContactsBatch(): Promise<BatchResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()
  const cdpQx = await getCdpDb()

  const result = await processBatch(qx, cdpQx, config)
  log.info({ ...result }, 'Security contacts batch activity complete')
  return result
}

export async function ingestSecurityContactsForPurlActivity(
  purl: string,
): Promise<IngestSingleResult> {
  const config = getSecurityContactsConfig()
  const qx = await getPackagesDb()
  const cdpQx = await getCdpDb()

  const result = await ingestSecurityContactsForPurl(qx, cdpQx, config, purl)
  log.info({ purl, ...result }, 'On-demand security contacts ingest activity complete')
  return result
}

// Fixed-cadence heartbeat, same rationale as processBatch.ts: a slow blob (LLM call) can
// outlast the 2-minute heartbeatTimeout on the shared activity proxy.
async function withHeartbeat<T>(fn: () => Promise<T>): Promise<T> {
  const heartbeatTimer = setInterval(() => {
    try {
      heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'Heartbeat failed')
    }
  }, 30_000)
  try {
    return await fn()
  } finally {
    clearInterval(heartbeatTimer)
  }
}

export async function runProtocolParseBatch(): Promise<ParseStageResult> {
  const cfg = getReportingProtocolConfig()
  const qx = await getPackagesDb()
  const result = await withHeartbeat(() =>
    runParseStage(
      qx,
      { githubGet: githubApiGet, fetchPage: fetchLinkedPage, llmExtract: llmExtractProtocol },
      cfg,
    ),
  )
  log.info({ ...result }, 'Reporting protocol parse batch activity complete')
  return result
}

export async function runProtocolAssembleBatch(): Promise<AssembleStageResult> {
  const cfg = getReportingProtocolConfig()
  const qx = await getPackagesDb()
  const result = await withHeartbeat(() => runAssembleStage(qx, cfg))
  log.info({ ...result }, 'Reporting protocol assemble batch activity complete')
  return result
}
