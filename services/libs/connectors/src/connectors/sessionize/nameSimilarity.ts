import { normalizeEventName } from './matching'

const YEAR_TOKEN = /^(19|20)\d\d$/
const MIN_CONTAINED_TOKENS = 3

interface Block {
  firstStart: number
  secondStart: number
  length: number
}

function longestCommonBlock(
  first: string,
  second: string,
  firstRange: [number, number],
  secondRange: [number, number],
): Block {
  let best: Block = { firstStart: firstRange[0], secondStart: secondRange[0], length: 0 }
  let previous = new Map<number, number>()
  for (let i = firstRange[0]; i < firstRange[1]; i++) {
    const current = new Map<number, number>()
    for (let j = secondRange[0]; j < secondRange[1]; j++) {
      if (first[i] === second[j]) {
        const length = (previous.get(j - 1) ?? 0) + 1
        current.set(j, length)
        if (length > best.length) {
          best = { firstStart: i - length + 1, secondStart: j - length + 1, length }
        }
      }
    }
    previous = current
  }
  return best
}

function matchedLength(
  first: string,
  second: string,
  firstRange: [number, number],
  secondRange: [number, number],
): number {
  const block = longestCommonBlock(first, second, firstRange, secondRange)
  if (block.length === 0) {
    return 0
  }
  return (
    block.length +
    matchedLength(
      first,
      second,
      [firstRange[0], block.firstStart],
      [secondRange[0], block.secondStart],
    ) +
    matchedLength(
      first,
      second,
      [block.firstStart + block.length, firstRange[1]],
      [block.secondStart + block.length, secondRange[1]],
    )
  )
}

export function similarityRatio(first: string, second: string): number {
  const total = first.length + second.length
  if (total === 0) {
    return 1
  }
  return (2 * matchedLength(first, second, [0, first.length], [0, second.length])) / total
}

function withoutYears(normalizedName: string): string {
  return normalizedName
    .split(' ')
    .filter((token) => !YEAR_TOKEN.test(token))
    .join(' ')
}

export function nameSimilarity(first: string, second: string): number {
  const normalizedFirst = withoutYears(normalizeEventName(first))
  const normalizedSecond = withoutYears(normalizeEventName(second))
  if (normalizedFirst === '' || normalizedSecond === '') {
    return 0
  }
  return similarityRatio(normalizedFirst, normalizedSecond)
}

export function isNameContained(first: string, second: string): boolean {
  const firstTokens = new Set(normalizeEventName(first).split(' ').filter(Boolean))
  if (firstTokens.size < MIN_CONTAINED_TOKENS) {
    return false
  }
  const secondTokens = new Set(normalizeEventName(second).split(' '))
  return [...firstTokens].every((token) => secondTokens.has(token))
}
