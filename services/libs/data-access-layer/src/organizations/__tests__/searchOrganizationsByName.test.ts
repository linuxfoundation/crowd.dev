import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { QueryExecutor } from '../../queryExecutor'
import { searchOrganizationsByName } from '../base'

vi.mock('@crowd/database', () => ({}))
vi.mock('@crowd/logging', () => ({
  getServiceLogger: () => ({ info: vi.fn(), debug: vi.fn(), warn: vi.fn(), error: vi.fn() }),
  getServiceChildLogger: () => ({
    info: vi.fn(),
    debug: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  }),
  LoggerBase: class {},
}))
vi.mock('@crowd/redis', () => ({}))
// segments/index.ts imports @crowd/integrations which requires filesystem integration folders
vi.mock('../../segments', () => ({ findLfSegmentByName: vi.fn() }))

describe('searchOrganizationsByName', () => {
  let mockSelect: ReturnType<typeof vi.fn>
  let mockSelectOne: ReturnType<typeof vi.fn>
  let mockQx: QueryExecutor

  beforeEach(() => {
    mockSelect = vi.fn()
    mockSelectOne = vi.fn()
    mockQx = { select: mockSelect, selectOne: mockSelectOne } as unknown as QueryExecutor
  })

  it('returns empty rows and zero total when DB returns no rows at offset 0', async () => {
    mockSelect.mockResolvedValueOnce([])

    const result = await searchOrganizationsByName(mockQx, 'acme', { limit: 20, offset: 0 })

    expect(result).toEqual({ rows: [], total: 0 })
    expect(mockSelectOne).not.toHaveBeenCalled()
  })

  it('runs a COUNT fallback when the page is empty but offset > 0', async () => {
    mockSelect.mockResolvedValueOnce([])
    mockSelectOne.mockResolvedValueOnce({ total: '42' })

    const result = await searchOrganizationsByName(mockQx, 'acme', { limit: 20, offset: 100 })

    expect(mockSelectOne).toHaveBeenCalledOnce()
    expect(result).toEqual({ rows: [], total: 42 })
  })

  it('strips the internal total column from returned rows', async () => {
    mockSelect.mockResolvedValueOnce([{ id: 'org-1', displayName: 'Acme Corp', total: '1' }])

    const { rows } = await searchOrganizationsByName(mockQx, 'acme', { limit: 20, offset: 0 })

    expect(rows[0]).not.toHaveProperty('total')
    expect(rows[0]).toEqual({ id: 'org-1', displayName: 'Acme Corp' })
  })

  it('reads total from the first row', async () => {
    mockSelect.mockResolvedValueOnce([
      { id: 'org-1', displayName: 'Acme Corp', total: '42' },
      { id: 'org-2', displayName: 'Acme Ltd', total: '42' },
    ])

    const { total } = await searchOrganizationsByName(mockQx, 'acme', { limit: 20, offset: 0 })

    expect(total).toBe(42)
  })

  it('passes ILIKE pattern wrapping the name in % wildcards', async () => {
    mockSelect.mockResolvedValueOnce([])

    await searchOrganizationsByName(mockQx, 'linux', { limit: 10, offset: 0 })

    const [, params] = mockSelect.mock.calls[0]
    expect(params.pattern).toBe('%linux%')
  })

  it('escapes % and _ wildcards in the user input', async () => {
    mockSelect.mockResolvedValueOnce([])

    await searchOrganizationsByName(mockQx, '50%_off', { limit: 10, offset: 0 })

    const [, params] = mockSelect.mock.calls[0]
    expect(params.pattern).toBe('%50\\%\\_off%')
  })

  it('passes limit and offset to the query', async () => {
    mockSelect.mockResolvedValueOnce([])
    mockSelectOne.mockResolvedValueOnce({ total: '0' })

    await searchOrganizationsByName(mockQx, 'linux', { limit: 5, offset: 40 })

    const [, params] = mockSelect.mock.calls[0]
    expect(params.limit).toBe(5)
    expect(params.offset).toBe(40)
  })

  it('propagates errors thrown by qx.select', async () => {
    mockSelect.mockRejectedValueOnce(new Error('DB unavailable'))

    await expect(
      searchOrganizationsByName(mockQx, 'acme', { limit: 20, offset: 0 }),
    ).rejects.toThrow('DB unavailable')
  })
})
