import { describe, expect, it } from 'vitest'

import { buildCandidates } from '../candidates'

describe('buildCandidates', () => {
  it('lowercases owner and repo into a single <owner>/<repo> candidate', () => {
    expect(buildCandidates('Grafana', 'Grafana')).toEqual(['grafana/grafana'])
  })

  it('passes through already-valid lowercase slugs', () => {
    expect(buildCandidates('prometheus', 'node_exporter')).toEqual(['prometheus/node_exporter'])
  })

  it('rejects components with characters Docker Hub does not accept', () => {
    // GitHub allows '+' in org names via renames; Hub would 400.
    expect(buildCandidates('foo+bar', 'baz')).toEqual([])
  })

  it('rejects components that start or end with a separator', () => {
    expect(buildCandidates('-leading', 'repo')).toEqual([])
    expect(buildCandidates('owner', 'trailing.')).toEqual([])
  })

  it('does not emit a library/<repo> candidate', () => {
    // Guard against accidental reintroduction — see comment in candidates.ts.
    expect(buildCandidates('nodejs', 'node')).toEqual(['nodejs/node'])
  })
})
