// afdocs ships ESM only while the workers compile to CommonJS, so it is loaded
// with a dynamic import at call time instead of a static import.
export function loadAfdocs() {
  return import('afdocs')
}

export type Afdocs = Awaited<ReturnType<typeof loadAfdocs>>
