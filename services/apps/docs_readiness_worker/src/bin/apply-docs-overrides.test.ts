// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { beforeEach, describe, expect, test, vi } from 'vitest'

import { main, parseCsv, parseDocsUrl, run } from './apply-docs-overrides'

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
function makeQx(projects = PROJECTS) {
  const select = vi.fn(async (_sql: string, { keys }: { keys: string[] }) =>
    projects.filter((p) => keys.includes(p.id) || keys.includes(p.slug)),
  )
  const qx = { select, tx: vi.fn(), result: vi.fn(), selectOne: vi.fn() }
  return { qx: qx as never, select, tx: qx.tx }
}

const exec = (csv: string, apply: boolean, projects = PROJECTS) => {
  const lines: string[] = []
  const { qx, select, tx } = makeQx(projects)
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

  test.each(['a"b"c', 'ab"c', '"ab"c', ' "ab"'])('rejects a stray quote in %j', (field) => {
    expect(() => parseCsv(`project,docsUrl\nx,${field}\n`)).toThrow(/CSV row 2/)
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

  test.each([
    '',
    '  ',
    'kyverno.io/docs',
    'ftp://example.org',
    'javascript:alert(1)',
    'None',
    'http:example.com',
    'https:/example.com',
    'https:///example.com',
    'https://x.io/a b',
    'https://x.io/a\nb',
    'https://x.io/a\tb',
    'https://x.io/a\n',
    'https://x.io/a\u0000b',
  ])('rejects %j', (value) => {
    expect(parseDocsUrl(value)).toHaveProperty('error')
  })

  test('accepts an uppercase scheme and stores the validated string', () => {
    expect(parseDocsUrl('  HTTPS://x.io/a,b  ')).toEqual({ url: 'HTTPS://x.io/a,b' })
  })
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

    // The valid openfga row is withheld because another openfga row is invalid.
    expect(summary).toMatchObject({ applied: 1, error: 4, failed: 0 })
    expect(output).toContain('project not found')
    expect(output).toContain('invalid URL')
    expect(output).toContain('empty project')
    expect(state.created.map((c) => c.projectId)).toEqual([ID_A])
  })

  test('an unquoted URL with a comma is an invalid row and is not applied', async () => {
    const { summary, output } = await exec(
      'project,docsUrl\nkyverno,https://x.io/a,b\nopenfga,none\n',
      true,
    )

    expect(summary).toMatchObject({ applied: 1, error: 1 })
    expect(output).toContain('expected 2 fields, got 3')
    expect(output).toMatch(/^2 +kyverno .*error: expected 2 fields/m)
    expect(state.created.map((c) => c.projectId)).toEqual([ID_B])
  })

  test('a quoted URL with a comma is kept whole', async () => {
    await exec('project,docsUrl\nkyverno,"https://x.io/a,b"\n', true)
    expect(state.created[0].docsUrl).toBe('https://x.io/a,b')
  })

  test('a quoted URL with an embedded newline is rejected', async () => {
    const { summary } = await exec('project,docsUrl\nkyverno,"https://x.io/a\nb"\n', true)
    expect(summary).toMatchObject({ applied: 0, error: 1 })
  })

  test('a later invalid row for a project blocks an earlier valid row, by slug or id', async () => {
    const { summary, output } = await exec(
      `project,docsUrl\nkyverno,https://kyverno.io/docs/\n${ID_A},not a url\n`,
      true,
    )

    expect(summary).toMatchObject({ applied: 0, error: 2 })
    expect(output).toContain('project has invalid row 3; nothing applied')
    expect(state.created).toEqual([])
  })

  test('a key matching more than one project is reported as ambiguous and skipped', async () => {
    const projects = [
      { id: ID_A, slug: 'kyverno' },
      { id: ID_B, slug: ID_A },
    ]
    const { summary, output } = await exec(
      `project,docsUrl\n${ID_A},https://x.io/docs\nkyverno,none\n`,
      true,
      projects,
    )

    expect(summary).toMatchObject({ applied: 1, error: 1 })
    expect(output).toContain('ambiguous')
    expect(state.created).toEqual([
      { projectId: ID_A, docsUrl: null, submittedBy: 'script:IN-1425' },
    ])
  })

  test('--apply prints the final statuses again and the re-run hint', async () => {
    state.active.set(ID_B, { docsUrl: null })
    state.failFor.add(ID_A)
    const { output } = await exec(
      'project,docsUrl\nkyverno,https://kyverno.io/docs/\nopenfga,none\n',
      true,
    )

    const final = output.slice(output.indexOf('Final statuses:'))
    expect(final).toMatch(/kyverno .*failed: boom/)
    expect(final).toMatch(/openfga .*unchanged/)
    expect(
      output
        .trimEnd()
        .endsWith('Re-run the same file; applied rows show as unchanged and are skipped.'),
    ).toBe(true)
  })

  test('a dry run does not print the final statuses', async () => {
    const { output } = await exec('project,docsUrl\nkyverno,none\n', false)
    expect(output).not.toContain('Final statuses')
    expect(output).not.toContain('Re-run')
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

describe('main', () => {
  const csvFile = (text: string) => {
    const file = join(mkdtempSync(join(tmpdir(), 'docs-overrides-')), 'in.csv')
    writeFileSync(file, text)
    return file
  }
  const connect = () => vi.fn(async () => makeQx().qx)

  beforeEach(() => {
    vi.spyOn(console, 'log').mockImplementation(() => undefined)
    vi.spyOn(console, 'error').mockImplementation(() => undefined)
  })

  test('a missing file fails before any connection is opened', async () => {
    const connectDb = connect()
    await expect(main(['/no/such/file.csv', '--apply'], connectDb)).rejects.toThrow(/ENOENT/)
    expect(connectDb).not.toHaveBeenCalled()
  })

  test('usage errors exit 2 without connecting', async () => {
    const connectDb = connect()
    expect(await main([], connectDb)).toBe(2)
    expect(await main(['a.csv', 'b.csv'], connectDb)).toBe(2)
    expect(connectDb).not.toHaveBeenCalled()
  })

  test('a dry run with an invalid row exits 1 and writes nothing', async () => {
    const connectDb = connect()
    const file = csvFile('project,docsUrl\nkyverno,https://kyverno.io/docs/\nopenfga,nope\n')

    expect(await main([file], connectDb)).toBe(1)
    expect(connectDb).toHaveBeenCalledWith(false)
    expect(state.created).toEqual([])
  })

  test('a clean dry run exits 0 and writes nothing', async () => {
    const file = csvFile('project,docsUrl\nkyverno,https://kyverno.io/docs/\n')

    expect(await main([file], connect())).toBe(0)
    expect(state.created).toEqual([])
  })

  test('--apply opens the write connection and exits 0 when every row is applied', async () => {
    const connectDb = connect()
    const file = csvFile('project,docsUrl\nkyverno,https://kyverno.io/docs/\n')

    expect(await main([file, '--apply'], connectDb)).toBe(0)
    expect(connectDb).toHaveBeenCalledWith(true)
    expect(state.created).toHaveLength(1)
  })
})
