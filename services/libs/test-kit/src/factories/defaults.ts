type DefaultValue<V> = V | (() => V)

type DefaultsFor<T extends object, K extends keyof T> = {
  [P in K]: DefaultValue<T[P]>
}

/** Required keys of T stay required; keys covered by defaults become optional. */
type WithDefaultsRow<T extends object, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>

function resolveDefault<V>(value: DefaultValue<V>): V {
  if (typeof value === 'function') {
    return (value as () => V)()
  }
  return value as V
}

function applyDefaults<T extends object, K extends keyof T>(
  row: WithDefaultsRow<T, K>,
  defaults: DefaultsFor<T, K>,
): T {
  const result = { ...row } as T

  for (const key of Object.keys(defaults) as K[]) {
    if (result[key] === undefined) {
      result[key] = resolveDefault(defaults[key]) as T[K]
    }
  }

  return result
}

/**
 * Fills missing allowlisted fields on each row. Explicit values always win.
 * Keys provided in `defaults` become optional on input; all other required
 * keys of T remain required at compile time.
 *
 * Usage: `withDefaults<MemberDbInsert>()({ id: () => ..., displayName: () => ... })`
 */
export function withDefaults<T extends object>() {
  return <K extends keyof T>(defaults: DefaultsFor<T, K>) => {
    return (rows: Array<WithDefaultsRow<T, K>>): T[] =>
      rows.map((row) => applyDefaults(row, defaults))
  }
}
