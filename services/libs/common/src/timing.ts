import { IMemberOrganization } from '@crowd/types'

export const timeout = async (delayMilliseconds: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, delayMilliseconds)
  })

export const addSeconds = (date: Date, seconds: number): Date => {
  const newDate = new Date(date)
  newDate.setSeconds(newDate.getSeconds() + seconds)
  return newDate
}

export const EPOCH_DATE = new Date('1970-01-01T00:00:00+00:00')

export const getSecondsTillEndOfMonth = () => {
  const now = new Date()
  const endOfMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59) // Set to last day of current month, at 23:59:59

  const diffInSeconds = (endOfMonth.getTime() - now.getTime()) / 1000
  return Math.round(diffInSeconds)
}

export const getLongestDateRange = <T extends IMemberOrganization>(orgs: T[]) => {
  const orgsWithDates = orgs.filter((row) => !!row.dateStart)
  if (orgsWithDates.length === 0) {
    // all orgs have no dates, return the first one
    return orgs[0]
  }

  const sortedByDateRange = orgsWithDates.sort((a, b) => {
    return (
      new Date(b.dateEnd || '9999-12-31').getTime() -
      new Date(b.dateStart).getTime() -
      (new Date(a.dateEnd || '9999-12-31').getTime() - new Date(a.dateStart).getTime())
    )
  })

  return sortedByDateRange[0]
}

export const dateIntersects = (
  d1Start?: string | null,
  d1End?: string | null,
  d2Start?: string | null,
  d2End?: string | null,
): boolean => {
  // If both periods have no dates at all, we consider they span all time
  if ((!d1Start && !d1End) || (!d2Start && !d2End)) {
    return true
  }

  // Convert strings to timestamps, using fallbacks for missing dates
  const start1 = d1Start ? new Date(d1Start).getTime() : -Infinity
  const end1 = d1End ? new Date(d1End).getTime() : Infinity
  const start2 = d2Start ? new Date(d2Start).getTime() : -Infinity
  const end2 = d2End ? new Date(d2End).getTime() : Infinity

  // Periods intersect if one period's start is before other period's end
  // and that same period's end is after the other period's start
  return start1 <= end2 && end1 >= start2
}
