// Postgres text columns cannot store NUL (U+0000). Recursively strip NUL bytes from every string
const NUL_GLOBAL = new RegExp(String.fromCharCode(0), 'g')

export function stripNullBytesDeep<T>(value: T): T {
  if (typeof value === 'string') {
    return value.replace(NUL_GLOBAL, '') as T
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = stripNullBytesDeep(value[i])
    return value
  }
  if (value !== null && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    for (const k of Object.keys(obj)) obj[k] = stripNullBytesDeep(obj[k])
    return value
  }
  return value
}
