import {Args, Command} from '@oclif/core'
import {existsSync, readdirSync, readFileSync} from 'fs'
import {resolve, join} from 'path'
import {REPO_ROOT} from '../../lib/paths.js'

interface PkgDeps {
  name: string          // @crowd/foo
  shortName: string     // foo
  kind: 'app' | 'lib'
  crowdDeps: string[]   // @crowd/* deps
}

function readPkgDeps(dir: string, kind: 'app' | 'lib'): PkgDeps[] {
  const results: PkgDeps[] = []
  try {
    for (const entry of readdirSync(dir, {withFileTypes: true})) {
      if (!entry.isDirectory()) continue
      const pkgPath = join(dir, entry.name, 'package.json')
      if (!existsSync(pkgPath)) continue
      try {
        const pkg = JSON.parse(readFileSync(pkgPath, 'utf-8'))
        const allDeps = {...pkg.dependencies, ...pkg.devDependencies}
        const crowdDeps = Object.keys(allDeps).filter(d => d.startsWith('@crowd/'))
        results.push({
          name: pkg.name ?? `@crowd/${entry.name}`,
          shortName: entry.name.replace(/_/g, '-'),
          kind,
          crowdDeps,
        })
      } catch { continue }
    }
  } catch { /* dir missing */ }
  return results
}

export default class ServiceDeps extends Command {
  static description = 'Show @crowd/* dependency graph for services and libs'
  static args = {name: Args.string({description: 'service or lib name (kebab-case)', required: false})}

  async run() {
    const {args} = await this.parse(ServiceDeps)

    const appsDir = resolve(REPO_ROOT, 'services', 'apps')
    const libsDir = resolve(REPO_ROOT, 'services', 'libs')

    const apps = readPkgDeps(appsDir, 'app')
    const libs = readPkgDeps(libsDir, 'lib')
    const all = [...apps, ...libs]

    if (!args.name) {
      // Summary: most-used libs
      const usageCount: Record<string, number> = {}
      for (const pkg of all) {
        for (const dep of pkg.crowdDeps) {
          usageCount[dep] = (usageCount[dep] ?? 0) + 1
        }
      }
      const sorted = Object.entries(usageCount).sort((a, b) => b[1] - a[1])

      this.log('\n@crowd/* usage across all services and libs:\n')
      for (const [dep, count] of sorted) {
        this.log(`  ${count.toString().padStart(3)}×  ${dep}`)
      }
      this.log(`\n${apps.length} services, ${libs.length} libs`)
      this.log('\nTip: crowd service deps <name> — show deps for a specific service or lib')
      return
    }

    const target = all.find(p => p.shortName === args.name || p.name === args.name || p.name === `@crowd/${args.name}`)

    if (!target) {
      this.error(`"${args.name}" not found. Run "crowd service deps" to list all.`)
    }

    if (target.kind === 'app') {
      // Forward deps
      this.log(`\n${target.name} depends on:\n`)
      if (target.crowdDeps.length === 0) {
        this.log('  (no @crowd/* dependencies)')
      } else {
        for (const dep of target.crowdDeps.sort()) {
          this.log(`  ${dep}`)
        }
      }
    } else {
      // Reverse deps — who uses this lib?
      const pkg = `@crowd/${target.shortName}`
      const consumers = all.filter(p => p.crowdDeps.includes(pkg))
      const appConsumers = consumers.filter(p => p.kind === 'app')
      const libConsumers = consumers.filter(p => p.kind === 'lib')

      this.log(`\n${target.name} is used by:\n`)

      if (consumers.length === 0) {
        this.log('  (no services or libs depend on this)')
      } else {
        if (appConsumers.length > 0) {
          this.log('  services:')
          for (const c of appConsumers.sort((a, b) => a.name.localeCompare(b.name))) {
            this.log(`    ${c.name}`)
          }
        }
        if (libConsumers.length > 0) {
          this.log('  libs:')
          for (const c of libConsumers.sort((a, b) => a.name.localeCompare(b.name))) {
            this.log(`    ${c.name}`)
          }
        }
      }

      // Also show what this lib itself depends on
      if (target.crowdDeps.length > 0) {
        this.log(`\n${target.name} depends on:\n`)
        for (const dep of target.crowdDeps.sort()) {
          this.log(`  ${dep}`)
        }
      }
    }
    this.log('')
  }
}
