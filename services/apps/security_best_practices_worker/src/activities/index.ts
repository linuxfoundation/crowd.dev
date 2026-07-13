import { ApplicationFailure } from '@temporalio/client'
import { exec, spawn } from 'child_process'
import { existsSync, readFileSync } from 'fs'
import { load as parseYaml } from 'js-yaml'
import { promisify } from 'util'

import {
  addControlEvaluationAssessment,
  addEvaluationSuite,
  addSuiteControlEvaluation,
  findEvaluationSuite,
  findObsoleteReposQx,
  findSuiteControlEvaluation,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'
import { ISecurityInsightsObsoleteRepo } from '@crowd/types'

import { svc } from '../main'
import { ISecurityInsightsPrivateerResult, ITokenInfo } from '../types'

export const BINARY_HOME = '/.privateer'

const execAsync = promisify(exec)

export async function getOSPSBaselineInsights(repoUrl: string, token: string): Promise<string> {
  // get owner and repo name from url
  const [owner, repoName] = repoUrl.split('/').slice(-2)

  const REPORT_OUTPUT_FILE_PATH = `${BINARY_HOME}/evaluation_results/${repoName.toLowerCase()}/${repoName.toLowerCase()}.yaml`

  // prepare config file for privateer
  await execAsync(
    `cp ${BINARY_HOME}/example-config.yml ${BINARY_HOME}/${repoName}.yml &&
     sed -i.bak -e "s/\\$REPO_NAME/${repoName}/g" -e "s/\\$REPO_OWNER/${owner}/g" -e "s/\\$GITHUB_TOKEN/${token}/g" ${BINARY_HOME}/${repoName}.yml && rm ${BINARY_HOME}/${repoName}.yml.bak`,
  )

  try {
    const { stdout, stderr } = await runBinary(`${BINARY_HOME}/bin/privateer`, [
      'run',
      '--config',
      `${BINARY_HOME}/${repoName}.yml`,
    ])

    const combinedOutput = `${stdout}\n${stderr}`

    classifyTokenError(combinedOutput, 'privateer output')
  } catch (err) {
    svc.log.error(`Privateer run failed: ${err.message}`)

    const output = `${err.stdout || ''}\n${err.stderr || ''}`
    classifyTokenError(output, 'failed privateer output')
    throw err
  }

  // check if the output file exists
  if (!existsSync(`${REPORT_OUTPUT_FILE_PATH}`)) {
    throw new Error(`Expected output file not found at ${REPORT_OUTPUT_FILE_PATH}!`)
  }

  let parsedYaml: ISecurityInsightsPrivateerResult
  try {
    const fileContents = readFileSync(REPORT_OUTPUT_FILE_PATH, 'utf8')
    parsedYaml = parseYaml(fileContents) as ISecurityInsightsPrivateerResult
  } catch (err) {
    throw new Error(`Failed to parse YAML from file: ${err}`)
  }

  // save file contents to redis
  const key = Math.random().toString(36).substring(7)
  await saveOSPSBaselineInsightsToRedis(key, parsedYaml)

  // cleanup generated files
  await cleanupFiles(repoName)

  return key
}

export async function saveOSPSBaselineInsightsToDB(
  key: string,
  repo: ISecurityInsightsObsoleteRepo,
): Promise<void> {
  const CATALOG_ID = 'osps-baseline-2026-02'
  const redisCache = new RedisCache(`osps-baseline-insights`, svc.redis, svc.log)
  const result = await redisCache.get(key)
  if (!result) {
    throw new Error(`No cached privateer result found for key: ${key}`)
  }
  const parsedResult: ISecurityInsightsPrivateerResult = JSON.parse(result)
  const evaluationSuite = parsedResult['evaluation-suites']?.find(
    (s) => s['catalog-id'] === CATALOG_ID,
  )
  if (!evaluationSuite) {
    throw new Error(
      `No evaluation suite found for catalog '${CATALOG_ID}' in privateer output for repo ${repo.repoUrl}`,
    )
  }

  const qx = pgpQx(svc.postgres.writer.connection())

  await addEvaluationSuite(qx, {
    repo: repo.repoUrl,
    insightsProjectId: repo.insightsProjectId,
    insightsProjectSlug: repo.insightsProjectSlug,
    catalogId: evaluationSuite['catalog-id'],
    name: evaluationSuite.name,
    result: evaluationSuite.result,
    corruptedState: evaluationSuite['corrupted-state'],
  })

  const suite = await findEvaluationSuite(qx, repo.repoUrl, evaluationSuite['catalog-id'])
  if (!suite) {
    throw new Error(
      `Evaluation suite not found after insert for repo ${repo.repoUrl}, catalog ${evaluationSuite['catalog-id']}`,
    )
  }

  for (const evaluation of evaluationSuite['control-evaluations'].evaluations) {
    const controlId = evaluation.control['entry-id']
    await addSuiteControlEvaluation(qx, {
      controlId,
      name: evaluation.name,
      corruptedState: false,
      message: evaluation.message,
      repo: repo.repoUrl,
      insightsProjectId: repo.insightsProjectId,
      insightsProjectSlug: repo.insightsProjectSlug,
      remediationGuide: '',
      result: evaluation.result,
      securityInsightsEvaluationSuiteId: suite.id,
    })

    const controlEvaluation = await findSuiteControlEvaluation(
      qx,
      repo.repoUrl,
      controlId,
      suite.id,
    )
    if (!controlEvaluation) {
      throw new Error(
        `Control evaluation not found after insert for repo ${repo.repoUrl}, controlId ${controlId}, suiteId ${suite.id}`,
      )
    }
    for (const assessment of evaluation['assessment-logs']) {
      const runDuration = computeRunDuration(assessment.start, assessment.end)
      await addControlEvaluationAssessment(qx, {
        applicability: assessment.applicability,
        description: assessment.description,
        message: assessment.message,
        repo: repo.repoUrl,
        insightsProjectId: repo.insightsProjectId,
        insightsProjectSlug: repo.insightsProjectSlug,
        requirementId: assessment.requirement['entry-id'],
        result: assessment.result,
        runDuration,
        steps: assessment.steps,
        stepsExecuted: assessment['steps-executed'] || 0,
        securityInsightsEvaluationId: controlEvaluation.id,
        recommendation: assessment.recommendation,
        start: assessment.start,
        end: assessment.end,
        value: null,
        changes: null,
      })
    }
  }
}

export async function findObsoleteRepos(
  insightsObsoleteAfterSeconds: number,
  failedRepos: string[],
  limit: number,
): Promise<ISecurityInsightsObsoleteRepo[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findObsoleteReposQx(qx, insightsObsoleteAfterSeconds, failedRepos, limit)
}

export async function saveOSPSBaselineInsightsToRedis(
  key: string,
  insights: ISecurityInsightsPrivateerResult,
): Promise<void> {
  const redisCache = new RedisCache(`osps-baseline-insights`, svc.redis, svc.log)
  await redisCache.set(key, JSON.stringify(insights), 60 * 60 * 24) // 1 day
}

// GitHub returns 403 for both rate-limit exhaustion AND persistent permission/SAML failures.
// Only rate-limit responses should trigger the 1h token-rotation cooldown; permission failures
// must be marked as invalid so the token isn't retried in a loop.
function classifyTokenError(output: string, source: string): void {
  if (output.includes('401 Unauthorized') || output.includes('401 Bad credentials')) {
    svc.log.warn(`Detected 401 error in ${source} - token invalid or expired!`)
    throw ApplicationFailure.create({
      message: 'GitHub token invalid or expired',
      type: 'TokenAuthError',
      nonRetryable: true,
    })
  }
  if (!output.includes('403')) return

  // Rate-limit 403s from GitHub carry a distinctive body: "API rate limit exceeded" or
  // "secondary rate limit". Anything else (SAML enforcement, missing scopes, resource
  // not accessible) is a permission problem and the token won't recover.
  const isRateLimit = /rate limit|rate_limit|secondary rate/i.test(output)
  if (isRateLimit) {
    svc.log.warn(`Detected 403 rate-limit in ${source} - token rate-limited!`)
    throw ApplicationFailure.create({
      message: 'GitHub token rate-limited',
      type: 'Token403Error',
      nonRetryable: true,
    })
  }
  svc.log.warn(`Detected 403 permission error in ${source} - token lacks required access!`)
  throw ApplicationFailure.create({
    message: 'GitHub token lacks required permissions',
    type: 'TokenAuthError',
    nonRetryable: true,
  })
}

function computeRunDuration(start: string | undefined, end: string | undefined): string {
  if (!start || !end) return ''
  const startMs = new Date(start).getTime()
  const endMs = new Date(end).getTime()
  if (isNaN(startMs) || isNaN(endMs) || endMs < startMs) return ''
  return `${endMs - startMs}ms`
}

async function cleanupFiles(repoName: string): Promise<void> {
  // Delete the file
  try {
    await execAsync(
      `rm -rf ${BINARY_HOME}/evaluation_results/${repoName} && rm ${BINARY_HOME}/${repoName}.yml`,
    )

    svc.log.info(`Cleaned generated files for repo: ${repoName}`)
  } catch (err) {
    svc.log.error(`Failed to clean generated files for repo: ${repoName}`)
    throw new Error(`Failed to clean generated files for repo: ${repoName}`)
  }
}

async function runBinary(
  binaryPath: string,
  args: string[] = [],
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    svc.log.info(`Running binary: ${binaryPath} with args: ${args.join(' ')}`)

    const proc = spawn(binaryPath, args, {
      cwd: '/.privateer',
      shell: false,
    })

    let stdout = ''
    let stderr = ''

    proc.stdout?.on('data', (data) => {
      const text = data.toString()
      stdout += text
      svc.log.info(`[stdout] ${text.trim()}`)
    })

    proc.stderr?.on('data', (data) => {
      const text = data.toString()
      stderr += text
      svc.log.warn(`[stderr] ${text.trim()}`)
    })

    proc.on('error', (err) => {
      svc.log.error(`Error running binary: ${err}`)
      reject(err)
    })

    proc.on('close', (code) => {
      // exit code 0 = all tests passed, 1 = some tests failed — both mean the
      // evaluation ran to completion and wrote its output file
      if (code === 0 || code === 1) {
        svc.log.info(`Binary completed with exit code ${code}`)
        resolve({ stdout, stderr })
      } else {
        const truncated = (s: string) => (s.length > 500 ? s.slice(0, 500) + '…' : s)
        const truncStdout = truncated(stdout)
        const truncStderr = truncated(stderr)
        const err = Object.assign(
          new Error(
            `Binary exited with code ${code}\nStderr:\n${truncStderr}\nStdout:\n${truncStdout}`,
          ),
          { stdout: truncStdout, stderr: truncStderr },
        )
        reject(err)
      }
    })
  })
}

export async function initializeTokenInfos(): Promise<ITokenInfo[]> {
  const redisCache = new RedisCache(`osps-baseline-insights`, svc.redis, svc.log)

  const tokenInfosInRedis = await redisCache.get('tokenInfos')

  if (tokenInfosInRedis) {
    const parsed: ITokenInfo[] = JSON.parse(tokenInfosInRedis)
    return parsed.map((t) => ({
      ...t,
      // Reset inUse — workflow processes may have crashed leaving stale in-use flags.
      inUse: false,
      // Backward compat: legacy entries have isRateLimited=true with no rateLimitedAt timestamp.
      // Without a timestamp the 1-hour expiry can't apply and the token would be stuck forever.
      // Clear stale rate-limits that lack a timestamp so they can be retried on this run.
      isRateLimited: t.isRateLimited && !!t.rateLimitedAt,
      rateLimitedAt: t.isRateLimited && t.rateLimitedAt ? t.rateLimitedAt : undefined,
    }))
  }

  return process.env['CROWD_GITHUB_PERSONAL_ACCESS_TOKENS'].split(',').map((token) => ({
    token,
    inUse: false,
    lastUsed: new Date(),
    isRateLimited: false,
  }))
}

export async function updateTokenInfos(tokenInfos: ITokenInfo[]): Promise<void> {
  const redisCache = new RedisCache(`osps-baseline-insights`, svc.redis, svc.log)
  await redisCache.set('tokenInfos', JSON.stringify(tokenInfos), 60 * 60 * 24) // 1 day
}
