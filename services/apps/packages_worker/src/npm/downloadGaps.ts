export const NPM_EARLIEST = '2015-01-10'
const MAX_CHUNK_DAYS = 540

// Group a sorted list of YYYY-MM-DD strings into contiguous windows capped at maxDays.
export function computeChunks(
  missingDates: string[],
  maxDays: number = MAX_CHUNK_DAYS,
): Array<{ start: string; end: string }> {
  if (missingDates.length === 0) return []

  const windows: Array<{ start: string; end: string }> = []
  let winStart = missingDates[0]
  let winEnd = missingDates[0]
  let winSize = 1

  for (let i = 1; i < missingDates.length; i++) {
    const isConsecutive = daysDiff(missingDates[i - 1], missingDates[i]) === 1
    if (isConsecutive && winSize < maxDays) {
      winEnd = missingDates[i]
      winSize++
    } else {
      windows.push({ start: winStart, end: winEnd })
      winStart = missingDates[i]
      winEnd = missingDates[i]
      winSize = 1
    }
  }
  windows.push({ start: winStart, end: winEnd })
  return windows
}

function daysDiff(a: string, b: string): number {
  return (Date.parse(b) - Date.parse(a)) / 86_400_000
}
