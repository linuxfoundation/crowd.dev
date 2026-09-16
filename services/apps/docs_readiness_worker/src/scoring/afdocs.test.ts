import { describe, expect, test } from 'vitest'

import { loadAfdocs } from './afdocs'

describe('loadAfdocs', () => {
  test('loads the ESM package from CommonJS and exposes the runner', async () => {
    const afdocs = await loadAfdocs()

    expect(typeof afdocs.runChecks).toBe('function')
    expect(afdocs.getAllChecks().length).toBeGreaterThan(0)
  })
})
