import type { CountWindow, ValidationTarget } from '@crowd/data-access-layer/src/connectors/types'

import { parseRepoArg } from './repoArg'

export type ValidateCompletenessCommand = 'run' | 'enable' | 'disable'

export interface IValidateCompletenessArgs {
  command: ValidateCompletenessCommand
  target: ValidationTarget
  window?: CountWindow
  counterKeys: string[] | null
}

const COMMANDS: ValidateCompletenessCommand[] = ['run', 'enable', 'disable']

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return undefined
  }
  const value = argv[flagIndex + 1]
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${flag} requires a value`)
  }
  argv.splice(flagIndex, 2)
  return value
}

function takeSwitch(argv: string[], flag: string): boolean {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return false
  }
  argv.splice(flagIndex, 1)
  return true
}

function splitList(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function parseTarget(argv: string[]): ValidationTarget {
  const unitIds = takeFlag(argv, '--unit-id')
  const repo = takeFlag(argv, '--repo')
  const integrationId = takeFlag(argv, '--integration-id')
  const given = [unitIds, repo, integrationId].filter((value) => value !== undefined)
  if (given.length !== 1) {
    throw new Error('exactly one of --unit-id, --repo, --integration-id is required')
  }
  if (unitIds !== undefined) {
    return { kind: 'units', unitIds: splitList(unitIds) }
  }
  if (repo !== undefined) {
    const { owner, name } = parseRepoArg(repo)
    return { kind: 'channel', channelName: `https://github.com/${owner}/${name}` }
  }
  return { kind: 'integration', integrationId: integrationId as string }
}

function parseIsoDate(flag: string, value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    throw new Error(`${flag} must be an ISO date, got "${value}"`)
  }
  return date.toISOString()
}

function parseWindow(argv: string[]): CountWindow {
  const allTime = takeSwitch(argv, '--all-time')
  const from = takeFlag(argv, '--from')
  const to = takeFlag(argv, '--to')
  if (allTime) {
    if (from !== undefined || to !== undefined) {
      throw new Error('--all-time cannot be combined with --from/--to')
    }
    return { kind: 'all-time' }
  }
  if (from === undefined || to === undefined) {
    throw new Error('run requires --all-time or both --from and --to')
  }
  const window: CountWindow = {
    kind: 'range',
    from: parseIsoDate('--from', from),
    to: parseIsoDate('--to', to),
  }
  if (window.from >= window.to) {
    throw new Error('--from must be before --to')
  }
  return window
}

export function parseValidateCompletenessArgs(rawArgv: string[]): IValidateCompletenessArgs {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const command = argv.shift() as ValidateCompletenessCommand
  if (!COMMANDS.includes(command)) {
    throw new Error(`command must be one of ${COMMANDS.join(', ')}`)
  }
  const target = parseTarget(argv)
  const window = command === 'run' ? parseWindow(argv) : undefined
  const counters = takeFlag(argv, '--counters')
  if (argv.length > 0) {
    throw new Error(`unexpected arguments: ${argv.join(' ')}`)
  }
  if (command === 'disable' && counters !== undefined) {
    throw new Error('disable does not take --counters')
  }
  return { command, target, window, counterKeys: counters ? splitList(counters) : null }
}
