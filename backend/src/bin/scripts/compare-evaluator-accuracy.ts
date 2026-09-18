/* eslint-disable no-console */

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'
import * as fs from 'fs'

import {
  IProjectEvaluationRequest,
  IProjectEvaluationResponse,
} from '@crowd/data-access-layer/src/project-catalog/types'

import { databaseInit } from '@/database/databaseConnection'
import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'

// projectCatalog rows evaluated before the CM-1470 TS/Bedrock port went live still hold
// the decisions produced by the old external Python evaluator agent — used here as the
// baseline to measure the new prompt's agreement/accuracy against, without re-running it.
const CM_1470_DEPLOY_CUTOFF = '2026-09-18T00:00:00Z'

// The new TS prompt has no CDP lookup tool, so it structurally cannot reach these two
// reasons the old agent could produce — flagged separately, not counted as prompt bugs.
const OLD_AGENT_ONLY_REASONS = new Set([
  'project is already onboarded',
  'project is already part of LF',
])

interface IBaselineRow {
  id: string
  repoUrl: string
  repoName: string
  projectSlug: string
  lfCriticalityScore: number | null
  source: string | null
  action: string
  evaluationResult: string | null
  evaluationReason: string | null
}

interface IComparisonRow {
  repoUrl: string
  oldAction: string
  oldReason: string | null
  newOutcome: string | null
  newReason: string | null
  newError: string | null
  match: boolean | null
  oldAgentOnlyReason: boolean
  newMetrics: {
    model: string
    inputTokens: number
    outputTokens: number
    seconds: number
  } | null
}

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'apiUrl',
    type: String,
    defaultValue: process.env.CROWD_API_SERVICE_URL,
    description:
      'Base URL of the API for this environment. Defaults to CROWD_API_SERVICE_URL — ' +
      'set in the same .env used by the worker for local dev; pass it explicitly for ' +
      'staging/prod, since those endpoints live in infra config, not this repo.',
  },
  {
    name: 'apiKey',
    type: String,
    defaultValue: process.env.CROWD_PROJECT_EVALUATION_STATIC_API_KEY,
    description:
      'Bearer token with the write:project-evaluation scope. Defaults to ' +
      'CROWD_PROJECT_EVALUATION_STATIC_API_KEY.',
  },
  {
    name: 'limit',
    type: Number,
    defaultValue: 20,
    description:
      'Max number of historical rows to re-evaluate, picked as a stratified sample ' +
      'across decision categories (see fetchBaselineRows). Default 20.',
  },
  {
    name: 'concurrency',
    type: Number,
    defaultValue: 3,
    description: 'Parallel requests against the new endpoint. Default 3.',
  },
  {
    name: 'out',
    type: String,
    defaultValue: 'compare-evaluator-accuracy-report.json',
    description: 'Path to write the full JSON report.',
  },
]

const usage = commandLineUsage([
  {
    header: 'Compare evaluator accuracy',
    content:
      'Re-runs the new POST /v1/project-evaluation endpoint against historical ' +
      'projectCatalog rows already decided by the old external evaluator agent, ' +
      'and reports agreement/accuracy without needing to re-invoke the old agent.',
  },
  { header: 'Options', optionList: options },
])

const argv = process.argv.slice(2).filter((arg) => arg !== '--')
const parameters = commandLineArgs(options, { argv })

async function fetchBaselineRows(
  qx: ReturnType<typeof SequelizeRepository.getQueryExecutor>,
  limit: number,
): Promise<IBaselineRow[]> {
  // Stratified by (action, evaluationReason) so a small sample still covers every decision
  // category. 'unsure' rows are excluded: they hold a raw agent/LLM error, not a decision.
  return qx.select(
    `
    WITH ranked AS (
      SELECT id, "repoUrl", "repoName", "projectSlug", "lfCriticalityScore", source,
             action, "evaluationResult", "evaluationReason",
             ROW_NUMBER() OVER (
               PARTITION BY action, "evaluationReason"
               ORDER BY "evaluatedAt" DESC
             ) AS rn
      FROM "projectCatalog"
      WHERE "evaluatedAt" IS NOT NULL
        AND "evaluatedAt" < $(cutoff)
        AND action IN ('onboard', 'skip')
    )
    SELECT id, "repoUrl", "repoName", "projectSlug", "lfCriticalityScore", source,
           action, "evaluationResult", "evaluationReason"
    FROM ranked
    ORDER BY rn, action, "evaluationReason"
    LIMIT $(limit)
    `,
    { cutoff: CM_1470_DEPLOY_CUTOFF, limit },
  )
}

async function evaluateWithNewEndpoint(
  apiUrl: string,
  apiKey: string,
  row: IBaselineRow,
): Promise<IComparisonRow> {
  const request: IProjectEvaluationRequest = {
    id: row.id,
    repoUrl: row.repoUrl,
    repoName: row.repoName,
    projectSlug: row.projectSlug,
    lfCriticalityScore: row.lfCriticalityScore,
    source: row.source,
  }

  const oldAgentOnlyReason = OLD_AGENT_ONLY_REASONS.has(row.evaluationReason ?? '')

  try {
    const response = await fetch(`${apiUrl}/v1/project-evaluation`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(request),
    })

    if (!response.ok) {
      return {
        repoUrl: row.repoUrl,
        oldAction: row.action,
        oldReason: row.evaluationReason,
        newOutcome: null,
        newReason: null,
        newError: `HTTP ${response.status}`,
        match: null,
        oldAgentOnlyReason,
        newMetrics: null,
      }
    }

    const result: IProjectEvaluationResponse = await response.json()
    const newMetrics = result.metrics
      ? {
          model: result.metrics.model,
          inputTokens: result.metrics.inputTokens,
          outputTokens: result.metrics.outputTokens,
          seconds: result.metrics.seconds,
        }
      : null

    if (result.outcome === 'unsure' && result.evaluationResult === 'error') {
      return {
        repoUrl: row.repoUrl,
        oldAction: row.action,
        oldReason: row.evaluationReason,
        newOutcome: null,
        newReason: null,
        newError: result.evaluationReason,
        match: null,
        oldAgentOnlyReason,
        newMetrics,
      }
    }

    // Null evaluationReason means the old reason was never recorded — score outcome only.
    const match =
      result.outcome === 'onboard'
        ? row.action === 'onboard'
        : row.action === 'skip' &&
          (row.evaluationReason === null || result.evaluationReason === row.evaluationReason)

    return {
      repoUrl: row.repoUrl,
      oldAction: row.action,
      oldReason: row.evaluationReason,
      newOutcome: result.outcome,
      newReason: result.evaluationReason,
      newError: null,
      match,
      oldAgentOnlyReason,
      newMetrics,
    }
  } catch (err) {
    return {
      repoUrl: row.repoUrl,
      oldAction: row.action,
      oldReason: row.evaluationReason,
      newOutcome: null,
      newReason: null,
      newError: err instanceof Error ? err.message : String(err),
      match: null,
      oldAgentOnlyReason,
      newMetrics: null,
    }
  }
}

async function runWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length)
  let nextIndex = 0

  async function worker() {
    while (nextIndex < items.length) {
      const current = nextIndex
      nextIndex += 1
      results[current] = await fn(items[current])
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()))
  return results
}

function average(values: number[]): number | null {
  return values.length ? values.reduce((sum, v) => sum + v, 0) / values.length : null
}

function summarizeNewEndpointCost(rows: IComparisonRow[]) {
  const withMetrics = rows.filter((r) => r.newMetrics !== null).map((r) => r.newMetrics!)

  return {
    callsWithMetrics: withMetrics.length,
    avgInputTokens: average(withMetrics.map((m) => m.inputTokens)),
    avgOutputTokens: average(withMetrics.map((m) => m.outputTokens)),
    avgSeconds: average(withMetrics.map((m) => m.seconds)),
    totalInputTokens: withMetrics.reduce((sum, m) => sum + m.inputTokens, 0),
    totalOutputTokens: withMetrics.reduce((sum, m) => sum + m.outputTokens, 0),
  }
}

function summarize(rows: IComparisonRow[]) {
  const scored = rows.filter((r) => r.match !== null)
  const scoredSupported = scored.filter((r) => !r.oldAgentOnlyReason)
  const errored = rows.filter((r) => r.newError !== null)

  return {
    totalRows: rows.length,
    errored: errored.length,
    scored: scored.length,
    accuracyAllRows: scored.length ? scored.filter((r) => r.match).length / scored.length : null,
    accuracyExcludingOldAgentOnlyReasons: scoredSupported.length
      ? scoredSupported.filter((r) => r.match).length / scoredSupported.length
      : null,
    oldAgentOnlyReasonRows: rows.filter((r) => r.oldAgentOnlyReason).length,
    newEndpointCost: summarizeNewEndpointCost(rows),
  }
}

if (parameters.help || !parameters.apiUrl || !parameters.apiKey) {
  console.log(usage)
  process.exit(parameters.help ? 0 : 1)
} else {
  setImmediate(async () => {
    const db = await databaseInit()
    const qx = SequelizeRepository.getQueryExecutor({ database: db } as IRepositoryOptions)

    const baselineRows = await fetchBaselineRows(qx, parameters.limit)
    console.log(`Comparing ${baselineRows.length} historical rows against the new endpoint...`)

    const comparisonRows = await runWithConcurrency(baselineRows, parameters.concurrency, (row) =>
      evaluateWithNewEndpoint(parameters.apiUrl, parameters.apiKey, row),
    )

    const summary = summarize(comparisonRows)

    fs.writeFileSync(parameters.out, JSON.stringify({ summary, rows: comparisonRows }, null, 2))

    console.log(JSON.stringify(summary, null, 2))
    console.log(`\nFull report written to ${parameters.out}`)

    process.exit(0)
  })
}
