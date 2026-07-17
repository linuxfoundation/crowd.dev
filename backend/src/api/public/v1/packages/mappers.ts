export function snakeToCamelKeys(
  obj: Record<string, unknown> | null,
): Record<string, unknown> | null {
  if (obj === null) return null
  return Object.fromEntries(
    Object.entries(obj).map(([k, v]) => [k.replace(/_([a-z])/g, (_, c) => c.toUpperCase()), v]),
  )
}

export function repoMappingLabel(confidence: number | null): 'High' | 'Medium' | 'Low' | null {
  if (confidence === null) return null
  if (confidence >= 0.8) return 'High'
  if (confidence >= 0.5) return 'Medium'
  return 'Low'
}
