import { fileURLToPath } from 'node:url'

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
  return handler(env)
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.exit(await main(process.argv.slice(2), process.env))
}
