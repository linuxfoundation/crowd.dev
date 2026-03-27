export function parseLlmJson<T>(answer: string): T {
  const raw = answer.trim()

  const tryParse = (str: string): T | undefined => {
    try {
      return JSON.parse(str) as T
    } catch {
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

  // 3. Balanced extraction
  const starts = [
    { idx: raw.indexOf('{'), open: '{', close: '}' },
    { idx: raw.indexOf('['), open: '[', close: ']' },
  ]
    .filter((s) => s.idx >= 0)
    .sort((a, b) => a.idx - b.idx)

  for (const { idx, open, close } of starts) {
    let depth = 0,
      inString = false,
      escaped = false

    for (let i = idx; i < raw.length; i++) {
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
        const parsed = tryParse(raw.slice(idx, i + 1))
        if (parsed !== undefined) return parsed
        break
      }
    }
  }

  throw new SyntaxError('LLM response does not contain valid JSON')
}
