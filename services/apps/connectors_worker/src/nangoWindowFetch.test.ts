import { describe, expect, it, vi } from 'vitest'

import { FetchNangoPage, INangoWindowPage, fetchNangoRecordsInWindow } from './nangoWindowFetch'

interface ITestRecord {
  id: string
  timestamp: number
  metadata: { lastModifiedAt: string }
}

function page(records: ITestRecord[], nextCursor?: string): INangoWindowPage<ITestRecord> {
  return { records, nextCursor }
}

describe('fetchNangoRecordsInWindow', () => {
  const windowStart = new Date('2026-09-10T00:00:00.000Z')
  const windowEnd = new Date('2026-09-11T00:00:00.000Z')

  it('filters records by their own event timestamp, not lastModifiedAt', async () => {
    const fetchPage = vi.fn<FetchNangoPage<ITestRecord>>().mockResolvedValueOnce(
      page([
        {
          id: 'before',
          timestamp: new Date('2026-09-09T00:00:00.000Z').getTime(),
          metadata: { lastModifiedAt: '2026-09-09T00:05:00.000Z' },
        },
        {
          id: 'in-window',
          timestamp: new Date('2026-09-10T12:00:00.000Z').getTime(),
          metadata: { lastModifiedAt: '2026-09-10T12:05:00.000Z' },
        },
        {
          id: 'at-end',
          timestamp: new Date('2026-09-11T00:00:00.000Z').getTime(),
          metadata: { lastModifiedAt: '2026-09-11T00:05:00.000Z' },
        },
        {
          id: 'after',
          timestamp: new Date('2026-09-12T00:00:00.000Z').getTime(),
          metadata: { lastModifiedAt: '2026-09-12T00:05:00.000Z' },
        },
      ]),
    )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['in-window'])
    expect(fetchPage).toHaveBeenCalledTimes(1)
  })

  it('includes a delayed-sync record whose lastModifiedAt lags its event timestamp', async () => {
    const fetchPage = vi.fn<FetchNangoPage<ITestRecord>>().mockResolvedValueOnce(
      page([
        {
          id: 'delayed-sync',
          timestamp: new Date('2026-09-10T12:00:00.000Z').getTime(),
          metadata: { lastModifiedAt: '2026-09-12T00:00:00.000Z' },
        },
      ]),
    )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['delayed-sync'])
  })

  it('stops paginating once lastModifiedAt passes the window end plus the grace period', async () => {
    const fetchPage = vi
      .fn<FetchNangoPage<ITestRecord>>()
      .mockResolvedValueOnce(
        page(
          [
            {
              id: 'page1-in-window',
              timestamp: new Date('2026-09-10T06:00:00.000Z').getTime(),
              metadata: { lastModifiedAt: '2026-09-10T06:05:00.000Z' },
            },
          ],
          'cursor-1',
        ),
      )
      .mockResolvedValueOnce(
        page([
          {
            id: 'page2-past-grace-period',
            timestamp: new Date('2026-09-20T00:00:00.000Z').getTime(),
            metadata: { lastModifiedAt: '2026-09-20T00:00:00.000Z' },
          },
        ]),
      )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['page1-in-window'])
    expect(fetchPage).toHaveBeenCalledTimes(2)
    expect(fetchPage).toHaveBeenNthCalledWith(2, 'cursor-1')
  })

  it('paginates via cursor across multiple pages until a next cursor is absent', async () => {
    const fetchPage = vi
      .fn<FetchNangoPage<ITestRecord>>()
      .mockResolvedValueOnce(
        page(
          [
            {
              id: 'page1',
              timestamp: new Date('2026-09-10T01:00:00.000Z').getTime(),
              metadata: { lastModifiedAt: '2026-09-10T01:05:00.000Z' },
            },
          ],
          'cursor-1',
        ),
      )
      .mockResolvedValueOnce(
        page([
          {
            id: 'page2',
            timestamp: new Date('2026-09-10T06:00:00.000Z').getTime(),
            metadata: { lastModifiedAt: '2026-09-10T06:05:00.000Z' },
          },
        ]),
      )

    const result = await fetchNangoRecordsInWindow(fetchPage, windowStart, windowEnd)

    expect(result.map((r) => r.id)).toEqual(['page1', 'page2'])
    expect(fetchPage).toHaveBeenCalledTimes(2)
    expect(fetchPage).toHaveBeenNthCalledWith(2, 'cursor-1')
  })
})
