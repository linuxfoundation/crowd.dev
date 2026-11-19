import type { SchedEvent } from './transform'

export interface RunInput {
  events: SchedEvent[]
  keys: Map<string, string>
}

const EVENT_FIELDS = ['subdomain', 'name', 'slug', 'sourceId'] as const

function parseEvent(line: string, lineNumber: number): SchedEvent {
  let parsed: unknown
  try {
    parsed = JSON.parse(line)
  } catch {
    throw new Error(`line ${lineNumber}: event is not valid JSON`)
  }
  const fields = (typeof parsed === 'object' && parsed !== null ? parsed : {}) as Record<
    string,
    unknown
  >
  const missing = EVENT_FIELDS.filter(
    (field) => typeof fields[field] !== 'string' || !(fields[field] as string).trim(),
  )
  if (missing.length > 0) {
    throw new Error(`line ${lineNumber}: event is missing ${missing.join(', ')}`)
  }
  const value = (field: (typeof EVENT_FIELDS)[number]) => (fields[field] as string).trim()
  return {
    subdomain: value('subdomain'),
    name: value('name'),
    segment: { slug: value('slug'), sourceId: value('sourceId') },
  }
}

// Key lines carry secrets: errors name the line number only, never its content.
export function parseRunInput(text: string): RunInput {
  const events = new Map<string, SchedEvent>()
  const keys = new Map<string, string>()

  text.split('\n').forEach((raw, index) => {
    const line = raw.trim()
    const lineNumber = index + 1
    if (!line) {
      return
    }
    if (line.startsWith('{')) {
      const event = parseEvent(line, lineNumber)
      if (events.has(event.subdomain)) {
        throw new Error(`line ${lineNumber}: event ${event.subdomain} appears twice`)
      }
      events.set(event.subdomain, event)
      return
    }
    const separator = line.indexOf('=')
    if (separator <= 0 || separator === line.length - 1) {
      throw new Error(`line ${lineNumber}: neither an event nor a "subdomain=key" line`)
    }
    keys.set(line.slice(0, separator).trim(), line.slice(separator + 1).trim())
  })

  return { events: [...events.values()], keys }
}

export function selectEvents(all: SchedEvent[], wanted: string[] | null): SchedEvent[] {
  if (!wanted) {
    return all
  }
  const bySubdomain = new Map(all.map((event) => [event.subdomain, event]))
  const unknown = wanted.filter((subdomain) => !bySubdomain.has(subdomain))
  if (unknown.length > 0) {
    throw new Error(`events not in the input: ${unknown.join(', ')}`)
  }
  return wanted.map((subdomain) => bySubdomain.get(subdomain) as SchedEvent)
}
