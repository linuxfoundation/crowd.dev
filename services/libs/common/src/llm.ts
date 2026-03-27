export function parseLlmJson<T>(answer: string): T {
  const raw = answer.trim()

  try {
    return JSON.parse(raw) as T
  } catch {
    // continue with normalization
  }

  const fenced = raw.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i)
  if (fenced?.[1]) {
    return JSON.parse(fenced[1].trim()) as T
  }

  const starts = [
    { idx: raw.indexOf('{'), open: '{', close: '}' },
    { idx: raw.indexOf('['), open: '[', close: ']' },
  ]
    .filter((s) => s.idx >= 0)
    .sort((a, b) => a.idx - b.idx)

  for (const candidate of starts) {
    let depth = 0
    let inString = false
    let escaped = false

    for (let i = candidate.idx; i < raw.length; i++) {
      const char = raw[i]

      if (escaped) {
        escaped = false
        continue
      }

      if (char === '\\') {
        escaped = true
        continue
      }

      if (char === '"') {
        inString = !inString
        continue
      }

      if (!inString) {
        if (char === candidate.open) {
          depth += 1
        } else if (char === candidate.close) {
          depth -= 1
          if (depth === 0) {
            return JSON.parse(raw.slice(candidate.idx, i + 1)) as T
          }
        }
      }
    }
  }

  throw new SyntaxError('LLM response does not contain valid JSON content')
}
