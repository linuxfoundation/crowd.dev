import assert from 'node:assert/strict'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { loadRules } from './services.ts'

const root = fileURLToPath(new URL('../../../../', import.meta.url))
const buildersDir = join(root, 'scripts/builders')

/** App dirs a Dockerfile copies from services/apps (any build stage). */
function appDirsCopiedBy(dockerfilePath: string): string[] {
  const dirs = new Set<string>()
  for (const line of readFileSync(dockerfilePath, 'utf8').split('\n')) {
    if (!/^\s*COPY\b/.test(line)) continue
    for (const m of line.matchAll(/services\/apps\/([\w-]+)/g)) dirs.add(m[1])
  }
  return [...dirs]
}

test('every builder that copies an app dir is mapped', { skip: !existsSync(buildersDir) }, () => {
  const rules = loadRules(join(root, '.github/release-services.json'))
  for (const file of readdirSync(buildersDir).filter((f) => f.endsWith('.env'))) {
    const name = file.replace(/\.env$/, '')
    const declared = readFileSync(join(buildersDir, file), 'utf8').match(
      /^DOCKERFILE="?([^"\n]+)"?/m,
    )?.[1]
    if (!declared) continue
    const dockerfile = [join(root, 'scripts', declared), join(root, declared)].find(existsSync)
    // Infra builders (db, nginx, redis) build from scaffold dirs outside this checkout layout.
    if (!dockerfile) continue
    for (const dir of appDirsCopiedBy(dockerfile)) {
      const rule = rules.find((r) => r.prefix === `services/apps/${dir}/`)
      assert.ok(
        rule && rule.services !== '*' && rule.services.includes(name),
        `builder ${name} copies services/apps/${dir} but no rule maps it in .github/release-services.json`,
      )
    }
  }
})
