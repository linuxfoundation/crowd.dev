import { execFileSync } from 'node:child_process'
import path from 'node:path'
import { describe, expect, test } from 'vitest'

const tsxCli = require.resolve('tsx/cli')
const smokeScript = path.join(__dirname, 'afdocs.smoke.ts')

describe('loadAfdocs', () => {
  test('loads the ESM package from CommonJS under the same tsx invocation the worker uses', () => {
    const output = execFileSync(process.execPath, [tsxCli, smokeScript], { encoding: 'utf-8' })

    expect(output.trim()).toBe('ok')
  })
})
