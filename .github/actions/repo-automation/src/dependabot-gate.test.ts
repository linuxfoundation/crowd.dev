import assert from 'node:assert/strict'
import { test } from 'node:test'

import { checksVerdict, runDependabotGate } from './dependabot-gate.ts'
import type { Check, Deps } from './dependabot-gate.ts'
import type { Logger } from './log.ts'

test('ignores own check', () =>
  assert.equal(
    checksVerdict(
      [
        { name: 'gate', bucket: 'pending' },
        { name: 'test', bucket: 'pass' },
      ],
      'gate',
    ),
    'pass',
  ))
test('skipped path-filtered checks count as pass', () =>
  assert.equal(
    checksVerdict(
      [
        { name: 'build', bucket: 'skipping' },
        { name: 'DCO', bucket: 'pass' },
      ],
      'gate',
    ),
    'pass',
  ))
test('any failure fails', () =>
  assert.equal(checksVerdict([{ name: 'test', bucket: 'fail' }], 'gate'), 'fail'))
test('no checks yet is pending', () => assert.equal(checksVerdict([], 'gate'), 'pending'))

const env = { INPUT_PR_NUMBER: '42', INPUT_SELF_CHECK: 'gate' }

type Harness = {
  deps: Deps
  calls: string[]
  logs: { level: string; msg: string }[]
  slept: number[]
}

/** `polls` are the stdout of successive `gh pr checks` calls; the last one repeats. */
function harness(polls: (Check[] | string)[], opts: { failOn?: string } = {}): Harness {
  const calls: string[] = []
  const logs: { level: string; msg: string }[] = []
  const slept: number[] = []
  let clock = Date.parse('2026-11-17T00:00:00Z')
  let i = 0
  const logger: Logger = {
    info: (m) => logs.push({ level: 'info', msg: m }),
    warning: (m) => logs.push({ level: 'warning', msg: m }),
    error: (m) => logs.push({ level: 'error', msg: m }),
  }
  const deps: Deps = {
    exec: (cmd, args) => {
      const line = `${cmd} ${args.join(' ')}`
      calls.push(line)
      if (opts.failOn && line.includes(opts.failOn)) throw new Error(`${opts.failOn} refused`)
      return ''
    },
    execOut: (cmd, args) => {
      calls.push(`${cmd} ${args.join(' ')}`)
      const p = polls[Math.min(i++, polls.length - 1)]
      return typeof p === 'string' ? p : JSON.stringify(p)
    },
    now: () => new Date(clock),
    sleep: async (ms) => {
      slept.push(ms)
      clock += ms
    },
    log: logger,
  }
  return { deps, calls, logs, slept }
}

const pass: Check[] = [{ name: 'test', bucket: 'pass' }]
const pending: Check[] = [{ name: 'test', bucket: 'pending' }]
const approve = (c: string[]) => c.findIndex((l) => l.startsWith('gh pr review 42 --approve'))
const merge = (c: string[]) => c.findIndex((l) => l === 'gh pr merge 42 --squash')

test('pending then pass approves and then merges', async () => {
  const h = harness([pending, pass])
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.deepEqual(h.slept, [30000])
  assert.ok(approve(h.calls) >= 0)
  assert.ok(merge(h.calls) > approve(h.calls))
  assert.ok(h.calls.some((l) => l.includes('Patch dev-dependency update; all checks green.')))
  assert.deepEqual(
    h.logs.filter((l) => l.level !== 'info'),
    [],
  )
})

test('failing check logs a warning and does not approve', async () => {
  const h = harness([[{ name: 'lint', bucket: 'fail' }, ...pass]])
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(approve(h.calls), -1)
  assert.equal(merge(h.calls), -1)
  assert.equal(h.logs.length, 1)
  assert.equal(h.logs[0].level, 'warning')
  assert.match(h.logs[0].msg, /lint/)
})

test('timeout after 45 minutes does not approve', async () => {
  const h = harness([pending])
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(h.slept.length, 90)
  assert.equal(approve(h.calls), -1)
  assert.equal(h.logs.at(-1)?.level, 'info')
  assert.match(h.logs.at(-1)?.msg ?? '', /45 minutes/)
})

test('merge failure leaves the PR for code-owner review', async () => {
  const h = harness([pass], { failOn: 'gh pr merge' })
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.ok(approve(h.calls) >= 0)
  assert.equal(h.logs.at(-1)?.level, 'info')
  assert.match(h.logs.at(-1)?.msg ?? '', /code-owner review/)
  assert.equal(h.logs.filter((l) => l.level === 'warning').length, 0)
})

test('approve failure warns and does not merge', async () => {
  const h = harness([pass], { failOn: 'gh pr review' })
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(merge(h.calls), -1)
  assert.equal(h.logs.filter((l) => l.level === 'warning').length, 1)
})

test('non-JSON checks output (no checks reported) is pending, then recovers', async () => {
  const h = harness(['no checks reported on the branch', pass])
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(h.slept.length, 1)
  assert.ok(merge(h.calls) >= 0)
})

test('rejects an invalid PR number', async () => {
  for (const bad of ['', 'abc', '0', '-3', '4x', '1.5']) {
    const h = harness([pass])
    await assert.rejects(
      runDependabotGate({ ...env, INPUT_PR_NUMBER: bad }, h.deps),
      /INPUT_PR_NUMBER/,
    )
    assert.deepEqual(h.calls, [])
  }
})

test('requires the self check name', async () => {
  const h = harness([pass])
  await assert.rejects(
    runDependabotGate({ INPUT_PR_NUMBER: '42', INPUT_SELF_CHECK: ' ' }, h.deps),
    /INPUT_SELF_CHECK/,
  )
})
