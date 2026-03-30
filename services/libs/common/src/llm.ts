export function parseLlmJson<T>(answer: string): T {
  const raw = answer.trim()

  let lastError: string | undefined

  const tryParse = (str: string): T | undefined => {
    try {
      return JSON.parse(str) as T
    } catch (e) {
      lastError = (e as Error).message
      return undefined
    }
  }

  // 1. Direct parse
  const direct = tryParse(raw)
  if (direct !== undefined) return direct

  // 2. Fenced ```json``` block
  const fenced = raw.match(/^```(?:json)?\n([\s\S]*?)\n```$/i)?.[1]
  if (fenced) {
    const parsed = tryParse(fenced.trim())
    if (parsed !== undefined) return parsed
  }

  // 3. Balanced extraction — try each delimiter type, scanning forward on failed parse
  const delimiters = [
    ...([
      ['{', '}'],
      ['[', ']'],
    ] as const),
  ].sort((a, b) => {
    const ia = raw.indexOf(a[0])
    const ib = raw.indexOf(b[0])
    if (ia === -1) return 1
    if (ib === -1) return -1
    return ia - ib
  })

  for (const [open, close] of delimiters) {
    let start = raw.indexOf(open)

    while (start >= 0) {
      let depth = 0,
        inString = false,
        escaped = false

      for (let i = start; i < raw.length; i++) {
        const char = raw[i]

        if (inString) {
          if (escaped) escaped = false
          else if (char === '\\') escaped = true
          else if (char === '"') inString = false
          continue
        }

        if (char === '"') {
          inString = true
        } else if (char === open) {
          depth++
        } else if (char === close && --depth === 0) {
          const parsed = tryParse(raw.slice(start, i + 1))
          if (parsed !== undefined) return parsed
          start = raw.indexOf(open, i + 1)
          break
        }
      }

      if (depth !== 0) break
    }
  }

  const preview = raw.length > 200 ? `${raw.slice(0, 200)}…` : raw
  throw new SyntaxError(
    `LLM response does not contain valid JSON: ${lastError} | Input: ${preview}`,
  )
}
