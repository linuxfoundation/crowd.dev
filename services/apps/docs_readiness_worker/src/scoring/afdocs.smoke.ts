// Run under the same tsx/CommonJS path the worker uses in production, not vitest's ESM runner.
import { loadAfdocs } from './afdocs'

async function main() {
  const afdocs = await loadAfdocs()

  if (typeof afdocs.runChecks !== 'function') {
    throw new Error('runChecks is not a function')
  }

  if (afdocs.getAllChecks().length === 0) {
    throw new Error('getAllChecks() returned no checks')
  }

  console.log('ok')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
