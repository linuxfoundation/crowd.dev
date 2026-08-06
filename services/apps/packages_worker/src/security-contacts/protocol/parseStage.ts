import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getReportingProtocolConfig } from '../../config'
import type { githubApiGet } from '../githubToken'

import { PARSER_VERSION, classifySecurityPolicy } from './classify'
import { fetchBlob, fetchLinkedPage, sha256Hex } from './fetchContent'
import { llmExtractProtocol } from './llmExtract'
import { ParseRowStatus, ParseStageResult, ParsedProtocol } from './types'
import { validateParsedProtocol } from './validate'

const log = getServiceChildLogger('reporting-protocol:parse')

export interface ParseStageDeps {
  githubGet: typeof githubApiGet
  fetchPage: typeof fetchLinkedPage
  llmExtract: typeof llmExtractProtocol
}

type Cfg = ReturnType<typeof getReportingProtocolConfig>

interface BlobJob {
  blob_oid: string
  url: string
}

// Random batch order: failed blobs get no parse row, so a deterministic
// ORDER BY window full of permanent failures would starve everything behind it.
async function selectUnparsedBlobs(qx: QueryExecutor, limit: number): Promise<BlobJob[]> {
  return qx.select(
    `SELECT * FROM (
       SELECT DISTINCT ON (w.blob_oid) w.blob_oid, r.url
       FROM repo_well_known_files w
       JOIN repos r ON r.id = w.repo_id AND r.host = 'github'
       JOIN package_repos pr ON pr.repo_id = w.repo_id
       JOIN packages p ON p.id = pr.package_id AND p.is_critical
       LEFT JOIN security_policy_parses sp
              ON sp.blob_oid = w.blob_oid AND sp.parser_version = $(parserVersion)
       WHERE w.file_type = 'security' AND w.deleted_at IS NULL AND sp.blob_oid IS NULL
       ORDER BY w.blob_oid, w.repo_id
     ) unparsed
     ORDER BY random()
     LIMIT $(limit)`,
    { limit, parserVersion: PARSER_VERSION },
  )
}

async function insertParse(
  qx: QueryExecutor,
  row: {
    blobOid: string
    sourceKind: 'security-file' | 'linked-page'
    url: string | null
    parser: 'deterministic' | 'llm'
    status: ParseRowStatus
    parsed: ParsedProtocol
    linkedUrls: string[]
    costUsd: number | null
  },
): Promise<void> {
  await qx.result(
    `INSERT INTO security_policy_parses
       (blob_oid, source_kind, url, parser, parser_version, status, parsed, linked_urls, llm_cost_usd)
     VALUES ($(blobOid), $(sourceKind), $(url), $(parser), $(parserVersion), $(status), $(parsed), $(linkedUrls), $(costUsd))
     ON CONFLICT (blob_oid) DO UPDATE SET
       source_kind = EXCLUDED.source_kind,
       url = EXCLUDED.url,
       parser = EXCLUDED.parser,
       parser_version = EXCLUDED.parser_version,
       status = EXCLUDED.status,
       parsed = EXCLUDED.parsed,
       linked_urls = EXCLUDED.linked_urls,
       llm_cost_usd = EXCLUDED.llm_cost_usd,
       parsed_at = NOW()`,
    { ...row, parsed: JSON.stringify(row.parsed), parserVersion: PARSER_VERSION },
  )
}

function createLimiter(max: number): <T>(fn: () => Promise<T>) => Promise<T> {
  let active = 0
  const waiters: Array<() => void> = []
  return async <T>(fn: () => Promise<T>): Promise<T> => {
    if (active >= max) {
      await new Promise<void>((resolve) => waiters.push(resolve))
    }
    active++
    try {
      return await fn()
    } finally {
      active--
      const next = waiters.shift()
      if (next) next()
    }
  }
}

async function parseText(
  text: string,
  deps: ParseStageDeps,
  cfg: Cfg,
): Promise<{
  parser: 'deterministic' | 'llm'
  status: ParseRowStatus
  parsed: ParsedProtocol
  linkedUrls: string[]
  costUsd: number | null
}> {
  const verdict = classifySecurityPolicy(text)
  if (verdict.clean) {
    return {
      parser: 'deterministic',
      status: verdict.isTemplate ? 'template' : 'ok',
      parsed: { methods: verdict.methods, guidelines: null },
      linkedUrls: verdict.linkedUrls,
      costUsd: null,
    }
  }
  if (verdict.pointerOnly) {
    return {
      parser: 'deterministic',
      status: 'ok',
      parsed: { methods: verdict.methods, guidelines: null },
      linkedUrls: verdict.linkedUrls,
      costUsd: null,
    }
  }
  const { parsed: llmParsed, costUsd } = await deps.llmExtract(text, {
    modelId: cfg.llmModelId,
    timeoutMs: cfg.llmTimeoutMs,
    accessKeyId: cfg.llmAccessKeyId,
    secretAccessKey: cfg.llmSecretAccessKey,
  })
  if (llmParsed && validateParsedProtocol(llmParsed, text).ok) {
    return {
      parser: 'llm',
      status: 'ok',
      parsed: llmParsed,
      linkedUrls: verdict.linkedUrls,
      costUsd,
    }
  }
  return {
    parser: 'llm',
    status: 'degraded',
    parsed: { methods: verdict.methods, guidelines: null },
    linkedUrls: verdict.linkedUrls,
    costUsd,
  }
}

async function alreadyParsed(qx: QueryExecutor, oid: string): Promise<boolean> {
  const row = await qx.selectOneOrNone(
    'SELECT 1 FROM security_policy_parses WHERE blob_oid = $(oid) AND parser_version = $(parserVersion)',
    { oid, parserVersion: PARSER_VERSION },
  )
  return row !== null
}

// Linked-page rows are keyed by sha256(url), not content hash: two URLs with
// identical content must both stay joinable from linked_urls at assembly.
async function processLinkedPages(
  qx: QueryExecutor,
  deps: ParseStageDeps,
  cfg: Cfg,
  linkedUrls: string[],
  result: ParseStageResult,
): Promise<{ anyFailure: boolean }> {
  let anyFailure = false
  for (const url of linkedUrls) {
    const pageKey = sha256Hex(url)
    if (await alreadyParsed(qx, pageKey)) continue
    const page = await deps.fetchPage(url, cfg.fetchTimeoutMs)
    if (!page) {
      anyFailure = true
      continue
    }
    const pageParse = await parseText(page.text, deps, cfg)
    await insertParse(qx, {
      blobOid: pageKey,
      sourceKind: 'linked-page',
      url,
      parser: pageParse.parser,
      status: pageParse.status,
      parsed: pageParse.parsed,
      linkedUrls: [],
      costUsd: pageParse.costUsd,
    })
    result.linkedPages++
    if (pageParse.costUsd !== null) result.llmCostUsd += pageParse.costUsd
  }
  return { anyFailure }
}

export async function runParseStage(
  qx: QueryExecutor,
  deps: ParseStageDeps,
  cfg: Cfg,
): Promise<ParseStageResult> {
  const jobs = await selectUnparsedBlobs(qx, cfg.parseBatchSize)
  const result: ParseStageResult = {
    blobsParsed: 0,
    deterministic: 0,
    llm: 0,
    degraded: 0,
    template: 0,
    linkedPages: 0,
    failed: 0,
    llmCostUsd: 0,
  }

  const limitLlm = createLimiter(Math.max(1, cfg.llmConcurrency))
  const limitedDeps: ParseStageDeps = {
    ...deps,
    llmExtract: (text, llmCfg) => limitLlm(() => deps.llmExtract(text, llmCfg)),
  }

  let cursor = 0
  const workers = Array.from({ length: Math.min(cfg.concurrency, jobs.length) }, async () => {
    while (cursor < jobs.length) {
      const job = jobs[cursor++]
      try {
        const text = await fetchBlob(
          { githubGet: deps.githubGet },
          job.url,
          job.blob_oid,
          cfg.fetchTimeoutMs,
        )
        if (text === null) {
          result.failed++
          continue
        }
        const fileParse = await parseText(text, limitedDeps, cfg)
        const pointerOnly = fileParse.parsed.methods.length === 0 && fileParse.linkedUrls.length > 0

        if (pointerOnly) {
          // The file row marks the blob done, so it is written only once every
          // linked page has a parse row — a transient page failure leaves the
          // blob unmarked and the next sweep retries the whole unit.
          const { anyFailure } = await processLinkedPages(
            qx,
            limitedDeps,
            cfg,
            fileParse.linkedUrls,
            result,
          )
          if (anyFailure) {
            result.failed++
            continue
          }
        }

        await insertParse(qx, {
          blobOid: job.blob_oid,
          sourceKind: 'security-file',
          url: null,
          ...fileParse,
        })
        result.blobsParsed++
        result[fileParse.parser === 'deterministic' ? 'deterministic' : 'llm']++
        if (fileParse.status === 'degraded') result.degraded++
        if (fileParse.status === 'template') result.template++
        if (fileParse.costUsd !== null) result.llmCostUsd += fileParse.costUsd
      } catch (err) {
        result.failed++
        log.warn({ blobOid: job.blob_oid, errMsg: (err as Error).message }, 'Blob parse failed')
      }
    }
  })
  await Promise.all(workers)

  log.info({ ...result }, 'Reporting protocol parse stage complete')
  return result
}
