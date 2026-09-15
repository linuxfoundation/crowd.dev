import { describe, expect, it, vi } from 'vitest'

import { FetchNangoPage, INangoWindowPage, fetchNangoRecordsInWindow } from './nangoWindowFetch'

interface ITestRecord {
  id: string
  metadata: { lastModifiedAt: string }
}

function page(records: ITestRecord[], nextCursor?: string): INangoWindowPage<ITestRecord> {
  return { records, nextCursor }
}

describe('fetchNangoRecordsInWindow', () => {
  const windowStart = new Date('2026-09-10T00:00:00.000Z')
  const windowEnd = new Date('2026-09-11T00:00:00.000Z')

  it('filters out records before the window and stops once a record reaches the window end', async () => {
    const fetchPage = vi.fn<FetchNangoPage<ITestRecord>>().mockResolvedValueOnce(
      page([
        { id: 'before', metadata: { lastModifiedAt: '2026-09-09T00:00:00.000Z' } },
        { id: 'in-window', metadata: { lastModifiedAt: '2026-09-10T12:00:00.000Z' } },
        { id: 'at-end', metadata: { lastModifiedAt: '2026-09-11T00:00:00.000Z' } },
        { id: 'after', metadata: { lastModifiedAt: '2026-09-12T00:00:00.000Z' } },
      ]),
    )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['in-window'])
    expect(fetchPage).toHaveBeenCalledTimes(1)
  })

  it('paginates via cursor across multiple pages until the window end is reached', async () => {
    const fetchPage = vi
      .fn<FetchNangoPage<ITestRecord>>()
      .mockResolvedValueOnce(
        page(
          [{ id: 'page1-before', metadata: { lastModifiedAt: '2026-09-09T00:00:00.000Z' } }],
          'cursor-1',
        ),
      )
      .mockResolvedValueOnce(
        page(
          [{ id: 'page2-in-window', metadata: { lastModifiedAt: '2026-09-10T06:00:00.000Z' } }],
          'cursor-2',
        ),
      )
      .mockResolvedValueOnce(
        page([{ id: 'page3-after', metadata: { lastModifiedAt: '2026-09-11T00:00:00.000Z' } }]),
      )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['page2-in-window'])
    expect(fetchPage).toHaveBeenCalledTimes(3)
    expect(fetchPage).toHaveBeenNthCalledWith(2, 'cursor-1')
    expect(fetchPage).toHaveBeenNthCalledWith(3, 'cursor-2')
  })

  it('stops when there is no next cursor even if the window end was never reached', async () => {
    const fetchPage = vi
      .fn<FetchNangoPage<ITestRecord>>()
      .mockResolvedValueOnce(
        page([{ id: 'only', metadata: { lastModifiedAt: '2026-09-10T06:00:00.000Z' } }]),
      )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['only'])
    expect(fetchPage).toHaveBeenCalledTimes(1)
  })
})
