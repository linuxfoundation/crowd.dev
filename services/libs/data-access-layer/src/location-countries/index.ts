import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'
import { ILocationCountry } from './types'

export * from './members'
export * from './organizations'
export * from './types'

export async function findLocationCountries(
  qx: QueryExecutor,
  locations: string[],
): Promise<ILocationCountry[]> {
  if (locations.length === 0) {
    return []
  }

  return qx.select(
    `
      select "location", "status", "countryCode"
      from "locationCountries"
      where "location" in ($(locations:csv))
    `,
    { locations },
  )
}

export async function insertLocationCountries(
  qx: QueryExecutor,
  rows: ILocationCountry[],
): Promise<void> {
  if (rows.length === 0) {
    return
  }

  await qx.result(
    prepareBulkInsert(
      'locationCountries',
      ['location', 'status', 'countryCode'],
      rows,
      '("location") do nothing',
    ),
  )
}
