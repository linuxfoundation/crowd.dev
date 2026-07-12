type DefaultValue<T> = T | (() => T)

type Defaults<T> = {
  [K in keyof T]?: DefaultValue<T[K]>
}

function resolveDefault<T>(value: DefaultValue<T>): T {
  return typeof value === 'function' ? (value as () => T)() : value
}

function applyDefaults<T extends object>(row: Partial<T>, defaults: Defaults<T>): T {
  const result = { ...row } as T

  for (const key of Object.keys(defaults) as (keyof T)[]) {
    const defaultValue = defaults[key]
    if (result[key] === undefined && defaultValue !== undefined) {
      result[key] = resolveDefault(defaultValue as DefaultValue<T[typeof key]>)
    }
  }

  return result
}

/**
 * Fills missing allowlisted fields on each row. Explicit values always win.
 * Vue-style: primitives as values, objects/arrays (and per-row values) as factories.
 */
export function withDefaults<T extends object>(defaults: Defaults<T>) {
  return (rows: Partial<T>[]): T[] => rows.map((row) => applyDefaults(row, defaults))
}
