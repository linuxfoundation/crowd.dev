import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'

import { normalizeSeverity, overdue, runSecuritySla, toAlerts } from './security-sla.ts'
import type { Alert, Deps } from './security-sla.ts'

const now = new Date('2026-11-30T00:00:00Z')
const a = (severity: Alert['severity'], createdAt: string) => ({
  source: 'dependabot' as const,
  severity,
  createdAt,
  url: 'u',
})
test('critical older than 7 days is overdue', () =>
  assert.equal(
    overdue([a('critical', '2026-11-20T00:00:00Z'), a('critical', '2026-11-25T00:00:00Z')], now, {
      critical: 7,
      high: 30,
    }).critical.length,
    1,
  ))
test('high within 30 days is not overdue', () =>
  assert.equal(
    overdue([a('high', '2026-11-05T00:00:00Z')], now, { critical: 7, high: 30 }).high.length,
    0,
  ))
test('medium is never overdue', () =>
  assert.deepEqual(overdue([a('medium', '2020-01-01T00:00:00Z')], now, { critical: 7, high: 30 }), {
    critical: [],
    high: [],
  }))
test('exactly the SLA age is not overdue', () =>
  assert.equal(
    overdue([a('critical', '2026-11-23T00:00:00Z')], now, { critical: 7, high: 30 }).critical
      .length,
    0,
  ))

test('moderate maps to medium and labels are lowercased', () => {
  assert.equal(normalizeSeverity('moderate'), 'medium')
  assert.equal(normalizeSeverity('HIGH'), 'high')
  assert.equal(normalizeSeverity('weird'), null)
})
test('alerts without a severity are skipped', () =>
  assert.deepEqual(
    toAlerts('code-scanning', [
      { created_at: 'c', html_url: 'u1', severity: null },
      { created_at: 'c', html_url: 'u2', severity: 'moderate' },
    ]),
    [{ source: 'code-scanning', severity: 'medium', createdAt: 'c', url: 'u2' }],
  ))

const row = (severity: string | null, created: string, url: string) =>
  JSON.stringify({ created_at: created, html_url: url, severity })

function harness(
  responses: { dependabot: string | Error; scanning: string | Error },
  delims = ['SAME'],
) {
  const dir = mkdtempSync(join(tmpdir(), 'sla-'))
  const summary = join(dir, 'summary.md')
  const output = join(dir, 'output.txt')
  writeFileSync(summary, '')
  writeFileSync(output, '')
  const logs: { level: string; msg: string }[] = []
  const calls: string[][] = []
  const deps: Deps = {
    exec: (_cmd, args) => {
      calls.push(args)
      const r = args[1].includes('/dependabot/') ? responses.dependabot : responses.scanning
      if (r instanceof Error) throw r
      return r
    },
    now: () => now,
    log: {
      info: (m) => logs.push({ level: 'info', msg: m }),
      warning: (m) => logs.push({ level: 'warning', msg: m }),
      error: (m) => logs.push({ level: 'error', msg: m }),
    },
    delimiter: () => delims.shift() ?? 'LAST',
  }
  const env = {
    GITHUB_REPOSITORY: 'o/r',
    GITHUB_STEP_SUMMARY: summary,
    GITHUB_OUTPUT: output,
  }
  return {
    deps,
    env,
    logs,
    calls,
    summary: () => readFileSync(summary, 'utf8'),
    output: () => readFileSync(output, 'utf8'),
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  }
}

test('both sources readable: counts, overdue links and heredoc output', async () => {
  const h = harness({
    dependabot: [
      row('critical', '2026-11-01T00:00:00Z', 'https://x/d1'),
      row('moderate', '2026-11-01T00:00:00Z', 'https://x/d2'),
    ].join('\n'),
    scanning: [
      row('high', '2026-09-01T00:00:00Z', 'https://x/c1'),
      row(null, '2026-09-01T00:00:00Z', 'https://x/c2'),
    ].join('\n'),
  })
  try {
    assert.equal(await runSecuritySla(h.env, h.deps), 0)
    assert.ok(h.calls.every((c) => c.includes('--paginate')))
    const s = h.summary()
    assert.match(s, /Dependabot alerts: 2 open \(critical 1, high 0, medium 1, low 0\)/)
    assert.match(s, /Code scanning alerts: 1 open/)
    assert.match(s, /https:\/\/x\/d1/)
    assert.match(s, /https:\/\/x\/c1/)
    assert.equal(h.logs.filter((l) => l.level === 'warning').length, 0)
    const out = h.output()
    assert.match(out, /^slack_text<<SAME\n/)
    assert.ok(out.endsWith('\nSAME\n'))
    assert.ok(out.split('\n').length <= 14)
  } finally {
    h.cleanup()
  }
})

test('dependabot 403 warns, notes in summary and still exits 0', async () => {
  const h = harness({
    dependabot: new Error('HTTP 403: Resource not accessible by integration'),
    scanning: row('high', '2026-11-29T00:00:00Z', 'https://x/c1'),
  })
  try {
    assert.equal(await runSecuritySla(h.env, h.deps), 0)
    const warnings = h.logs.filter((l) => l.level === 'warning')
    assert.equal(warnings.length, 1)
    assert.match(warnings[0].msg, /dependabot alerts.*403/i)
    assert.match(
      h.summary(),
      /Dependabot alerts: not readable with this token — set SECURITY_REPORT_TOKEN/,
    )
    assert.match(h.summary(), /Code scanning alerts: 1 open/)
    assert.match(h.output(), /not readable with this token/)
  } finally {
    h.cleanup()
  }
})

test('the heredoc delimiter never appears in the value', async () => {
  const h = harness(
    {
      dependabot: row('critical', '2026-11-01T00:00:00Z', 'https://x/d1'),
      scanning: '',
    },
    ['Overdue', 'FRESH'],
  )
  try {
    await runSecuritySla(h.env, h.deps)
    assert.match(h.output(), /^slack_text<<FRESH\n/)
  } finally {
    h.cleanup()
  }
})

test('missing GITHUB_REPOSITORY throws', async () => {
  const h = harness({ dependabot: '', scanning: '' })
  try {
    await assert.rejects(runSecuritySla({}, h.deps), /GITHUB_REPOSITORY/)
  } finally {
    h.cleanup()
  }
})
