import { randomBytes } from 'node:crypto'
import { appendFileSync } from 'node:fs'

import { run } from './gh.ts'
import { consoleLogger } from './log.ts'
import type { Logger } from './log.ts'

export type Severity = 'critical' | 'high' | 'medium' | 'low'
export type Source = 'dependabot' | 'code-scanning'
export type Alert = { source: Source; severity: Severity; createdAt: string; url: string }

export const SLA_DAYS = { critical: 7, high: 30 }

const DAY_MS = 24 * 60 * 60 * 1000
const SEVERITIES: Severity[] = ['critical', 'high', 'medium', 'low']
const MAX_LINKS = 5
export const DEPENDABOT_UNREADABLE =
  'Dependabot alerts: not readable with this token — set SECURITY_REPORT_TOKEN'

/** Alerts older than the SLA in whole days (strictly more than the SLA days). Only critical and high have an SLA. */
export function overdue(
  alerts: Alert[],
  now: Date,
  sla: { critical: number; high: number },
): { critical: Alert[]; high: Alert[] } {
  const result: { critical: Alert[]; high: Alert[] } = { critical: [], high: [] }
  for (const alert of alerts) {
    if (alert.severity !== 'critical' && alert.severity !== 'high') continue
    const ageDays = Math.floor((now.getTime() - new Date(alert.createdAt).getTime()) / DAY_MS)
    if (ageDays > sla[alert.severity]) result[alert.severity].push(alert)
  }
  return result
}

/** Lowercases a GitHub severity label; Dependabot's `moderate` is `medium`. Null for anything else. */
export function normalizeSeverity(value: unknown): Severity | null {
  if (typeof value !== 'string') return null
  const v = value.trim().toLowerCase()
  if (v === 'moderate') return 'medium'
  return (SEVERITIES as string[]).includes(v) ? (v as Severity) : null
}

type RawAlert = { created_at: string; html_url: string; severity: unknown }

/** Converts projected API rows into alerts, skipping rows without a usable severity. */
export function toAlerts(source: Source, rows: RawAlert[]): Alert[] {
  const alerts: Alert[] = []
  for (const row of rows) {
    const severity = normalizeSeverity(row.severity)
    if (!severity) continue
    alerts.push({ source, severity, createdAt: row.created_at, url: row.html_url })
  }
  return alerts
}

export type Runner = (cmd: string, args: string[]) => string

export type Deps = {
  exec: Runner
  now: () => Date
  log: Logger
  delimiter: () => string
}

const defaultDeps: Deps = {
  exec: (cmd, args) => run(cmd, args),
  now: () => new Date(),
  log: consoleLogger,
  delimiter: () => `EOF_${randomBytes(16).toString('hex')}`,
}

function msg(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function fetchAlerts(exec: Runner, repo: string, source: Source): Alert[] {
  const [path, severityPath] =
    source === 'dependabot'
      ? ['dependabot', '.security_advisory.severity']
      : ['code-scanning', '.rule.security_severity_level']
  const out = exec('gh', [
    'api',
    `repos/${repo}/${path}/alerts?state=open`,
    '--paginate',
    '--jq',
    `.[] | {created_at, html_url, severity: ${severityPath}}`,
  ])
  const rows = out
    .split('\n')
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l) as RawAlert)
  return toAlerts(source, rows)
}

function countBySeverity(alerts: Alert[]): Record<Severity, number> {
  const counts: Record<Severity, number> = { critical: 0, high: 0, medium: 0, low: 0 }
  for (const a of alerts) counts[a.severity]++
  return counts
}

function formatCounts(alerts: Alert[]): string {
  const c = countBySeverity(alerts)
  return SEVERITIES.map((s) => `${s} ${c[s]}`).join(', ')
}

export type SlaReport = { summary: string; slackText: string }

export function buildReport(i: {
  repo: string
  results: { source: Source; label: string; alerts: Alert[] | null }[]
  now: Date
}): SlaReport {
  const all: Alert[] = i.results.flatMap((r) => r.alerts ?? [])
  const od = overdue(all, i.now, SLA_DAYS)

  const summary: string[] = [`## Security alerts: ${i.repo}`, '']
  const slack: string[] = [`Security alerts for ${i.repo}`]

  for (const r of i.results) {
    if (r.alerts === null) {
      const note = r.source === 'dependabot' ? DEPENDABOT_UNREADABLE : `${r.label}: not readable`
      summary.push(`- ${note}`)
      slack.push(note)
    } else {
      const line = `${r.label}: ${r.alerts.length} open (${formatCounts(r.alerts)})`
      summary.push(`- ${line}`)
      slack.push(line)
    }
  }

  const overdueLine = `Overdue (critical > ${SLA_DAYS.critical}d, high > ${SLA_DAYS.high}d): critical ${od.critical.length}, high ${od.high.length}`
  summary.push('', `### ${overdueLine}`)
  slack.push(overdueLine)
  for (const sev of ['critical', 'high'] as const) {
    if (od[sev].length === 0) continue
    summary.push('', `**${sev}**`)
    for (const a of od[sev].slice(0, MAX_LINKS)) summary.push(`- [${a.source}] ${a.url}`)
    if (od[sev].length > MAX_LINKS) summary.push(`- and ${od[sev].length - MAX_LINKS} more`)
  }
  return { summary: summary.join('\n') + '\n', slackText: slack.join('\n') }
}

function setOutput(file: string, name: string, value: string, delimiter: () => string): void {
  let delim = delimiter()
  while (value.includes(delim)) delim = delimiter()
  appendFileSync(file, `${name}<<${delim}\n${value}\n${delim}\n`)
}

export async function runSecuritySla(
  env: NodeJS.ProcessEnv,
  deps: Deps = defaultDeps,
): Promise<number> {
  const { exec, now, log } = deps
  const repo = env.GITHUB_REPOSITORY ?? ''
  if (!repo) throw new Error('GITHUB_REPOSITORY is required')

  const sources: { source: Source; label: string }[] = [
    { source: 'dependabot', label: 'Dependabot alerts' },
    { source: 'code-scanning', label: 'Code scanning alerts' },
  ]
  const results = sources.map((s) => {
    try {
      return { ...s, alerts: fetchAlerts(exec, repo, s.source) }
    } catch (err) {
      log.warning(`could not read ${s.label.toLowerCase()} for ${repo}: ${msg(err)}`)
      return { ...s, alerts: null }
    }
  })

  const report = buildReport({ repo, results, now: now() })
  log.info(report.slackText)
  if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, report.summary)
  if (env.GITHUB_OUTPUT)
    setOutput(env.GITHUB_OUTPUT, 'slack_text', report.slackText, deps.delimiter)
  return 0
}
