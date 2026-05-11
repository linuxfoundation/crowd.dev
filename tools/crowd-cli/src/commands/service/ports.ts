import {Command} from '@oclif/core'
import {existsSync, readdirSync, readFileSync} from 'fs'
import {join, resolve} from 'path'
import {REPO_ROOT} from '../../lib/paths.js'

interface PortEntry {
  service: string
  port: number
}

function extractDebugPort(script: string): number | null {
  const m = script.match(/--inspect=\S+:(\d+)/)
  return m ? parseInt(m[1], 10) : null
}

export default class ServicePorts extends Command {
  static description = 'List debug ports for all services and flag conflicts'

  async run() {
    const appsDir = resolve(REPO_ROOT, 'services', 'apps')
    const entries: PortEntry[] = []
    const missing: string[] = []

    for (const entry of readdirSync(appsDir, {withFileTypes: true})) {
      if (!entry.isDirectory()) continue
      const pkgPath = join(appsDir, entry.name, 'package.json')
      if (!existsSync(pkgPath)) continue
      const pkg = JSON.parse(readFileSync(pkgPath, 'utf-8'))
      const script: string = pkg.scripts?.['start:debug:local'] ?? ''
      const serviceName = entry.name.replace(/_/g, '-')
      if (!script) {
        missing.push(serviceName)
        continue
      }
      const port = extractDebugPort(script)
      if (port === null) {
        missing.push(serviceName)
      } else {
        entries.push({service: serviceName, port})
      }
    }

    entries.sort((a, b) => a.port - b.port)

    const byPort = new Map<number, string[]>()
    for (const {service, port} of entries) {
      const list = byPort.get(port) ?? []
      list.push(service)
      byPort.set(port, list)
    }

    const conflicts = [...byPort.entries()].filter(([, svcs]) => svcs.length > 1)

    this.log('\nDebug ports (start:debug:local):')
    for (const {service, port} of entries) {
      const conflict = (byPort.get(port)?.length ?? 0) > 1
      this.log(`  ${conflict ? '✗' : '✓'} ${service.padEnd(40)} :${port}${conflict ? '  ← CONFLICT' : ''}`)
    }

    if (missing.length > 0) {
      this.log('\nMissing --inspect in start:debug:local:')
      for (const svc of missing) this.log(`  ⚠  ${svc}`)
    }

    if (conflicts.length > 0) {
      this.log('\nConflicts:')
      for (const [port, svcs] of conflicts) {
        this.log(`  port ${port}: ${svcs.join(', ')}`)
      }
      this.error(`${conflicts.length} port conflict(s) found — fix start:debug:local in the affected package.json files`)
    } else if (missing.length === 0) {
      this.log('\nAll ports unique. No conflicts.')
    } else {
      this.log('\nNo port conflicts, but some services missing --inspect (see above).')
    }
  }
}
