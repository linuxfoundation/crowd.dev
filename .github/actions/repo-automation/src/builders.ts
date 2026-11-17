import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

/** Maps each k8s service name to the builder (`<name>.env` file name) that builds it. */
export function loadBuilderServiceMap(dir: string): Map<string, string> {
  const map = new Map<string, string>()
  for (const file of readdirSync(dir).sort()) {
    if (!file.endsWith('.env')) continue
    const builder = file.slice(0, -'.env'.length)
    const content = readFileSync(join(dir, file), 'utf8')
    const line = content.split('\n').find((l) => /^\s*SERVICES\s*=/.test(l))
    if (!line) continue
    const value = line
      .slice(line.indexOf('=') + 1)
      .trim()
      .replace(/^(["'])(.*)\1$/, '$2')
    for (const service of value.split(/\s+/).filter(Boolean)) {
      if (!map.has(service)) map.set(service, builder)
    }
  }
  return map
}

/** Translates service names to builder names; unknown names pass through unchanged. */
export function toBuilders(
  services: string[],
  map: Map<string, string>,
): { builders: string[]; unknown: string[] } {
  const builders = new Set<string>()
  const unknown: string[] = []
  for (const service of services) {
    const builder = map.get(service)
    if (builder === undefined) {
      unknown.push(service)
      builders.add(service)
    } else {
      builders.add(builder)
    }
  }
  return { builders: [...builders].sort(), unknown }
}
