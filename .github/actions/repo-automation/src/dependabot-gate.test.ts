import assert from 'node:assert/strict'
import { test } from 'node:test'

import { checksVerdict, runDependabotGate, shouldApprove } from './dependabot-gate.ts'
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
test('cancel fails', () =>
  assert.equal(checksVerdict([{ name: 'test', bucket: 'cancel' }], 'gate'), 'fail'))
test('unknown bucket is pending', () =>
  assert.equal(
    checksVerdict([{ name: 'test', bucket: 'weird' as unknown as Check['bucket'] }], 'gate'),
    'pending',
  ))
test('no checks yet is pending', () => assert.equal(checksVerdict([], 'gate'), 'pending'))

const settled = { verdict: 'pass' as const, names: ['a', 'b'], elapsedMs: 120000 }
test('shouldApprove: early pass is not acted on', () =>
  assert.equal(shouldApprove({ ...settled, previousNames: ['a', 'b'], elapsedMs: 119999 }), false))
test('shouldApprove: changed check names are not acted on', () =>
  assert.equal(shouldApprove({ ...settled, previousNames: ['a'] }), false))
test('shouldApprove: first poll is not acted on', () =>
  assert.equal(shouldApprove({ ...settled, previousNames: null }), false))
test('shouldApprove: stable and 120s elapsed is acted on', () =>
  assert.equal(shouldApprove({ ...settled, previousNames: ['b', 'a'] }), true))
test('shouldApprove: non-pass verdict is not acted on', () =>
  assert.equal(
    shouldApprove({ ...settled, verdict: 'pending' as never, previousNames: ['a', 'b'] }),
    false,
  ))

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
  assert.equal(h.slept.length, 4)
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

test('a pass is held until the check set stops changing', async () => {
  const h = harness([
    [{ name: 'DCO', bucket: 'pass' }],
    [
      { name: 'DCO', bucket: 'pass' },
      { name: 'build', bucket: 'pass' },
    ],
  ])
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(h.slept.length, 4)
  assert.ok(merge(h.calls) >= 0)
})

test('a read error is logged as a warning and never approves', async () => {
  const h = harness([pass])
  h.deps.execOut = () => {
    throw new Error('spawn gh ENOENT')
  }
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.equal(approve(h.calls), -1)
  assert.ok(h.logs.some((l) => l.level === 'warning' && /ENOENT/.test(l.msg)))
})

test('merge failure falls back to auto-merge', async () => {
  const h = harness([pass], { failOn: 'gh pr merge 42 --squash' })
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.ok(approve(h.calls) >= 0)
  assert.ok(h.calls.includes('gh pr merge 42 --auto --squash'))
  assert.equal(h.logs.at(-1)?.level, 'info')
  assert.equal(h.logs.at(-1)?.msg, 'auto-merge enabled; waiting for code-owner review')
  assert.equal(h.logs.filter((l) => l.level === 'warning').length, 0)
})

test('merge and auto-merge both failing leaves the PR for code-owner review', async () => {
  const h = harness([pass], { failOn: 'gh pr merge' })
  assert.equal(await runDependabotGate(env, h.deps), 0)
  assert.ok(approve(h.calls) >= 0)
  assert.ok(h.calls.includes('gh pr merge 42 --auto --squash'))
  assert.equal(h.logs.at(-1)?.level, 'info')
  assert.match(h.logs.at(-1)?.msg ?? '', /^left for code-owner review \(auto-merge unavailable\)/)
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
  assert.equal(h.slept.length, 4)
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
