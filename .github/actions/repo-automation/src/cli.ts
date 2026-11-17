import { fileURLToPath } from 'node:url'

import { runDependabotGate } from './dependabot-gate.ts'
import { runDeployed } from './deployed.ts'
import { runTag } from './tag.ts'

export type Handler = (env: NodeJS.ProcessEnv) => Promise<number>

const handlers = new Map<string, Handler>()

/** Registers the handler for a command name; command modules call this. */
export function register(name: string, handler: Handler): void {
  handlers.set(name, handler)
}

export async function main(argv: string[], env: NodeJS.ProcessEnv): Promise<number> {
  const name = argv[0] ?? ''
  const handler = handlers.get(name)
  if (!handler) {
    console.error(`unknown command: ${name}`)
    return 2
  }
  try {
    return await handler(env)
  } catch (err) {
    console.error(`error: ${err instanceof Error ? err.message : String(err)}`)
    return 1
  }
}

register('tag', runTag)
register('deployed', runDeployed)
register('dependabot-gate', runDependabotGate)

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.exit(await main(process.argv.slice(2), process.env))
}
