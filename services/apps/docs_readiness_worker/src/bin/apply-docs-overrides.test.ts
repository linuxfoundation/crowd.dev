// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { beforeEach, describe, expect, test, vi } from 'vitest'

import { parseCsv, parseDocsUrl, run } from './apply-docs-overrides'

const ID_A = '11111111-1111-4111-8111-111111111111'
const ID_B = '22222222-2222-4222-8222-222222222222'

const state = vi.hoisted(() => ({
  active: new Map<string, { docsUrl: string | null }>(),
  created: [] as { projectId: string; docsUrl: string | null; submittedBy: string }[],
  failFor: new Set<string>(),
}))

vi.mock('@crowd/data-access-layer/src/database', () => ({
  READ_DB_CONFIG: vi.fn(),
  WRITE_DB_CONFIG: vi.fn(),
  getDbConnection: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({ pgpQx: vi.fn() }))

vi.mock('@crowd/data-access-layer/src/project-doc-overrides', () => ({
  findActiveProjectDocOverride: vi.fn(async (_qx, projectId: string) => {
    return state.active.get(projectId) ?? null
  }),
  createProjectDocOverride: vi.fn(async (_qx, data) => {
    if (state.failFor.has(data.projectId)) throw new Error('boom')
    state.created.push(data)
    state.active.set(data.projectId, { docsUrl: data.docsUrl })
    return data
  }),
}))

const PROJECTS = [
  { id: ID_A, slug: 'kyverno' },
  { id: ID_B, slug: 'openfga' },
]

// Mimics the lookup query: only the requested keys that match an id or slug come back.
function makeQx() {
  const select = vi.fn(async (_sql: string, { keys }: { keys: string[] }) =>
    PROJECTS.filter((p) => keys.includes(p.id) || keys.includes(p.slug)),
  )
  const qx = { select, tx: vi.fn(), result: vi.fn(), selectOne: vi.fn() }
  return { qx: qx as never, select, tx: qx.tx }
}

const exec = (csv: string, apply: boolean) => {
  const lines: string[] = []
  const { qx, select, tx } = makeQx()
  return run(qx, csv, { apply, out: (l) => lines.push(l) }).then((summary) => ({
    summary,
    output: lines.join('\n'),
    select,
    tx,
  }))
}

beforeEach(() => {
  state.active.clear()
  state.created.length = 0
  state.failFor.clear()
})

describe('parseCsv', () => {
  test('handles quoted fields, escaped quotes, embedded newlines, CRLF and a BOM', () => {
    const text = '﻿project,docsUrl\r\n"a,b","say ""hi"""\r\nx,"multi\nline"\r\n'
    expect(parseCsv(text)).toEqual([
      ['project', 'docsUrl'],
      ['a,b', 'say "hi"'],
      ['x', 'multi\nline'],
    ])
  })

  test('keeps the last record when there is no trailing newline', () => {
    expect(parseCsv('a,b\nc,d')).toEqual([
      ['a', 'b'],
      ['c', 'd'],
    ])
  })

  test('rejects an unterminated quote', () => {
    expect(() => parseCsv('a,"b')).toThrow(/unterminated/)
  })
})

describe('parseDocsUrl', () => {
  test('maps the literal none to null', () => {
    expect(parseDocsUrl('none')).toEqual({ url: null })
    expect(parseDocsUrl(' none ')).toEqual({ url: null })
  })

  test('accepts http and https URLs', () => {
    expect(parseDocsUrl(' https://kyverno.io/docs/ ')).toEqual({ url: 'https://kyverno.io/docs/' })
    expect(parseDocsUrl('http://example.org')).toEqual({ url: 'http://example.org' })
  })

  test.each(['', '  ', 'kyverno.io/docs', 'ftp://example.org', 'javascript:alert(1)', 'None'])(
    'rejects %j',
    (value) => {
      expect(parseDocsUrl(value)).toHaveProperty('error')
    },
  )
})

describe('run', () => {
  test('rejects a header without the required columns', async () => {
    await expect(exec('id,url\n', false)).rejects.toThrow(/header/)
  })

  test('dry run lists every change and never writes', async () => {
    state.active.set(ID_B, { docsUrl: 'https://old.example/docs' })
    const { summary, output, tx } = await exec(
      `project,docsUrl\nkyverno,https://kyverno.io/docs/\n${ID_B},none\n`,
      false,
    )

    expect(summary).toMatchObject({ change: 2, applied: 0, failed: 0 })
    expect(state.created).toEqual([])
    expect(tx).not.toHaveBeenCalled()
    expect(output).toContain('Dry run')
    expect(output).toContain('(no override)')
    expect(output).toContain('https://old.example/docs')
  })

  test('--apply creates one override per row with none stored as NULL', async () => {
    const { summary } = await exec(
      `project,docsUrl\nkyverno,https://kyverno.io/docs/\n${ID_B.toUpperCase()},none\n`,
      true,
    )

    expect(summary).toMatchObject({ applied: 2, failed: 0, error: 0 })
    expect(state.created).toEqual([
      { projectId: ID_A, docsUrl: 'https://kyverno.io/docs/', submittedBy: 'script:IN-1425' },
      { projectId: ID_B, docsUrl: null, submittedBy: 'script:IN-1425' },
    ])
  })

  test('re-running the same file skips rows whose active override already matches', async () => {
    const csv = 'project,docsUrl\nkyverno,https://kyverno.io/docs/\nopenfga,none\n'

    await exec(csv, true)
    expect(state.created).toHaveLength(2)

    const second = await exec(csv, true)
    expect(second.summary).toMatchObject({ unchanged: 2, applied: 0 })
    expect(state.created).toHaveLength(2)
  })

  test('an existing NULL override equals none but differs from a URL', async () => {
    state.active.set(ID_A, { docsUrl: null })
    const { summary } = await exec(
      'project,docsUrl\nkyverno,none\nopenfga,https://openfga.dev/docs\n',
      true,
    )

    expect(summary).toMatchObject({ unchanged: 1, applied: 1 })
    expect(state.created.map((c) => c.projectId)).toEqual([ID_B])
  })

  test('reports unknown projects and bad URLs, and keeps going', async () => {
    const { summary, output } = await exec(
      [
        'project,docsUrl',
        'kyverno,https://kyverno.io/docs/',
        'ghost,https://ghost.example/docs',
        'openfga,not a url',
        ',https://no-project.example',
        'openfga,https://openfga.dev/docs',
      ].join('\n'),
      true,
    )

    expect(summary).toMatchObject({ applied: 2, error: 3, failed: 0 })
    expect(output).toContain('project not found')
    expect(output).toContain('invalid URL')
    expect(output).toContain('empty project')
    expect(state.created.map((c) => c.projectId)).toEqual([ID_A, ID_B])
  })

  test('for duplicate project rows the last one wins, including id and slug of one project', async () => {
    const { summary, output } = await exec(
      `project,docsUrl\nkyverno,https://first.example\n${ID_A},https://second.example\nkyverno,none\n`,
      true,
    )

    expect(summary).toMatchObject({ superseded: 2, applied: 1 })
    expect(output).toContain('replaced by row 4')
    expect(state.created).toEqual([
      { projectId: ID_A, docsUrl: null, submittedBy: 'script:IN-1425' },
    ])
  })

  test('a failed write is reported and does not stop the batch', async () => {
    state.failFor.add(ID_A)
    const { summary, output } = await exec(
      'project,docsUrl\nkyverno,https://kyverno.io/docs/\nopenfga,none\n',
      true,
    )

    expect(summary).toMatchObject({ failed: 1, applied: 1 })
    expect(output).toContain('failed: boom')
    expect(state.created.map((c) => c.projectId)).toEqual([ID_B])
  })

  test('the project lookup only matches enabled, non-deleted projects', async () => {
    const { select } = await exec('project,docsUrl\nkyverno,none\n', false)
    expect(select.mock.calls[0][0]).toMatch(/"enabled" AND "deletedAt" IS NULL/)
  })
})
