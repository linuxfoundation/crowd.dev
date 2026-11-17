import { run, runCaptureStdout } from './gh.ts'
import { consoleLogger } from './log.ts'
import type { Logger } from './log.ts'

export type Check = {
  name: string
  bucket: 'pass' | 'fail' | 'pending' | 'skipping' | 'cancel'
}

export type Verdict = 'pending' | 'pass' | 'fail'

const POLL_MS = 30 * 1000
const TIMEOUT_MS = 45 * 60 * 1000

/** Verdict over every check except the gate's own; no checks yet counts as pending. */
export function checksVerdict(checks: Check[], selfName: string): Verdict {
  const others = checks.filter((c) => c.name !== selfName)
  if (others.length === 0) return 'pending'
  if (others.some((c) => c.bucket === 'fail' || c.bucket === 'cancel')) return 'fail'
  if (others.some((c) => c.bucket === 'pending')) return 'pending'
  return 'pass'
}

export type Deps = {
  /** Runs a command, throws on a non-zero exit. */
  exec: (cmd: string, args: string[]) => string
  /** Runs a command and returns stdout whatever the exit code. */
  execOut: (cmd: string, args: string[]) => string
  now: () => Date
  sleep: (ms: number) => Promise<void>
  log: Logger
}

const defaultDeps: Deps = {
  exec: (cmd, args) => run(cmd, args),
  execOut: runCaptureStdout,
  now: () => new Date(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  log: consoleLogger,
}

function msg(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function readChecks(deps: Deps, pr: string): Check[] {
  try {
    const parsed = JSON.parse(deps.execOut('gh', ['pr', 'checks', pr, '--json', 'name,bucket']))
    return Array.isArray(parsed) ? (parsed as Check[]) : []
  } catch {
    // e.g. "no checks reported": nothing registered yet
    return []
  }
}

export async function runDependabotGate(
  env: NodeJS.ProcessEnv,
  deps: Deps = defaultDeps,
): Promise<number> {
  const { exec, now, sleep, log } = deps
  const pr = (env.INPUT_PR_NUMBER ?? '').trim()
  if (!/^[1-9][0-9]*$/.test(pr)) {
    throw new Error('INPUT_PR_NUMBER must be a positive integer')
  }
  const selfName = (env.INPUT_SELF_CHECK ?? '').trim()
  if (!selfName) throw new Error('INPUT_SELF_CHECK is required')

  const deadline = now().getTime() + TIMEOUT_MS
  let checks = readChecks(deps, pr)
  let verdict = checksVerdict(checks, selfName)
  while (verdict === 'pending') {
    if (now().getTime() >= deadline) {
      log.info(`checks on #${pr} still pending after 45 minutes, not approving`)
      return 0
    }
    await sleep(POLL_MS)
    checks = readChecks(deps, pr)
    verdict = checksVerdict(checks, selfName)
  }

  if (verdict === 'fail') {
    const failing = checks
      .filter((c) => c.name !== selfName && (c.bucket === 'fail' || c.bucket === 'cancel'))
      .map((c) => c.name)
    log.warning(`checks failed on #${pr} (${failing.join(', ')}), not approving`)
    return 0
  }

  try {
    exec('gh', [
      'pr',
      'review',
      pr,
      '--approve',
      '--body',
      'Patch dev-dependency update; all checks green.',
    ])
  } catch (err) {
    log.warning(`could not approve #${pr}: ${msg(err)}`)
    return 0
  }
  try {
    exec('gh', ['pr', 'merge', pr, '--squash'])
    log.info(`approved and merged #${pr}`)
  } catch (err) {
    log.info(`approved #${pr} but could not merge, left for code-owner review: ${msg(err)}`)
  }
  return 0
}
