import { describe, expect, it } from 'vitest'

import { nextCounterKeys, parseValidateCompletenessArgs } from './validateCompletenessArgs'

describe('parseValidateCompletenessArgs', () => {
  it('maps --repo and a range window', () => {
    expect(
      parseValidateCompletenessArgs([
        'run',
        '--repo',
        'foo/bar',
        '--from',
        '2026-10-17T00:00:00Z',
        '--to',
        '2026-10-24T00:00:00Z',
      ]),
    ).toEqual({
      command: 'run',
      target: { kind: 'channel', channelName: 'https://github.com/foo/bar' },
      window: {
        kind: 'range',
        from: '2026-10-17T00:00:00.000Z',
        to: '2026-10-24T00:00:00.000Z',
      },
      counterKeys: null,
    })
  })

  it('maps --all-time and a unit id list', () => {
    const args = parseValidateCompletenessArgs(['run', '--unit-id', 'a,b', '--all-time'])
    expect(args.target).toEqual({ kind: 'units', unitIds: ['a', 'b'] })
    expect(args.window).toEqual({ kind: 'all-time' })
  })

  it('rejects a run without a window, or with both kinds', () => {
    expect(() => parseValidateCompletenessArgs(['run', '--repo', 'foo/bar'])).toThrow()
    expect(() =>
      parseValidateCompletenessArgs([
        'run',
        '--repo',
        'foo/bar',
        '--all-time',
        '--from',
        '2026-10-17',
        '--to',
        '2026-10-24',
      ]),
    ).toThrow()
  })

  it('rejects --from after --to and non-ISO dates', () => {
    const base = ['run', '--repo', 'foo/bar']
    expect(() =>
      parseValidateCompletenessArgs([...base, '--from', '2026-10-24', '--to', '2026-10-17']),
    ).toThrow()
    expect(() =>
      parseValidateCompletenessArgs([...base, '--from', 'nope', '--to', '2026-10-17']),
    ).toThrow()
  })

  it('rejects zero or several targets', () => {
    expect(() => parseValidateCompletenessArgs(['enable'])).toThrow()
    expect(() =>
      parseValidateCompletenessArgs(['enable', '--repo', 'foo/bar', '--unit-id', 'a']),
    ).toThrow()
  })

  it('parses enable with an integration target and counters', () => {
    const args = parseValidateCompletenessArgs([
      'enable',
      '--integration-id',
      '11111111-1111-1111-1111-111111111111',
      '--counters',
      'fork',
    ])
    expect(args).toEqual({
      command: 'enable',
      target: { kind: 'integration', integrationId: '11111111-1111-1111-1111-111111111111' },
      window: undefined,
      counterKeys: ['fork'],
    })
  })
})

describe('disable --counters', () => {
  it('parses counter keys for disable', () => {
    expect(
      parseValidateCompletenessArgs(['disable', '--repo', 'foo/bar', '--counters', 'fork']),
    ).toMatchObject({ command: 'disable', counterKeys: ['fork'] })
  })
})

describe('nextCounterKeys', () => {
  it('removes keys on disable', () => {
    expect(nextCounterKeys(['fork', 'issues-opened'], ['fork'], 'disable')).toEqual([
      'issues-opened',
    ])
  })

  it('unions keys on enable keeping order and deduping', () => {
    expect(nextCounterKeys(['fork', 'issues-opened'], ['stars', 'fork'], 'enable')).toEqual([
      'fork',
      'issues-opened',
      'stars',
    ])
  })
})
