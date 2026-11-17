import assert from 'node:assert/strict'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { loadRules } from './services.ts'

const root = fileURLToPath(new URL('../../../../', import.meta.url))
const buildersDir = join(root, 'scripts/builders')

test('every service builder with an app dir is mapped', { skip: !existsSync(buildersDir) }, () => {
  const rules = loadRules(join(root, '.github/release-services.json'))
  const mapped = new Set(rules.flatMap((r) => (r.services === '*' ? [] : r.services)))
  for (const file of readdirSync(buildersDir).filter((f) => f.endsWith('.env'))) {
    const name = file.replace(/\.env$/, '')
    const dockerfile = readFileSync(join(buildersDir, file), 'utf8').match(
      /^DOCKERFILE="?([^"\n]+)"?/m,
    )?.[1]
    const snake = dockerfile?.match(/Dockerfile\.(\w+)$/)?.[1]
    if (!snake || !existsSync(join(root, 'services/apps', snake))) continue
    assert.ok(mapped.has(name), `builder ${name} has no rule in .github/release-services.json`)
  }
})
