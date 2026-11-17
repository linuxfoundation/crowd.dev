import { readFileSync } from 'node:fs'

export type Rule = { prefix: string; services: string[] | '*' }

/** Reads `{ "rules": Rule[] }` from a JSON file. */
export function loadRules(path: string): Rule[] {
  const parsed = JSON.parse(readFileSync(path, 'utf8')) as { rules: Rule[] }
  return parsed.rules
}

/**
 * Maps changed paths to service names. The first rule whose prefix matches a path applies.
 * Returns null when any matched rule is '*' (unknown), otherwise the sorted union of services.
 * Paths matching no rule are ignored.
 */
export function servicesForPaths(paths: string[], rules: Rule[]): string[] | null {
  const services = new Set<string>()
  for (const path of paths) {
    const rule = rules.find((r) => path.startsWith(r.prefix))
    if (!rule) continue
    if (rule.services === '*') return null
    for (const s of rule.services) services.add(s)
  }
  return [...services].sort()
}
