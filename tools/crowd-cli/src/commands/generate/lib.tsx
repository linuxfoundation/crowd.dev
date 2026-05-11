import {useState, useCallback} from 'react'
import {Box, useApp, useInput} from 'ink'
import {Command} from '@oclif/core'
import {render} from 'ink'
import {LibGeneratorWizard} from '../../ui/LibGeneratorWizard.js'
import {OperationOverlay} from '../../ui/OperationOverlay.js'
import {generateLib, LibSpec} from '../../lib/generateService.js'

function GenerateLibApp() {
  const {exit} = useApp()
  const [phase, setPhase] = useState<'wizard' | 'running' | 'done'>('wizard')
  const [spec, setSpec] = useState<LibSpec | null>(null)
  const [logs, setLogs] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)

  useInput((_input, key) => {
    if (phase === 'done' && (key.return || key.escape)) exit()
  })

  const handleConfirm = useCallback((s: LibSpec) => {
    setSpec(s)
    setPhase('running')
    generateLib(s, line => setLogs(prev => [...prev, line]))
      .then(() => setPhase('done'))
      .catch(err => {
        setError(err instanceof Error ? err.message : String(err))
        setPhase('done')
      })
  }, [])

  if (phase === 'wizard') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <LibGeneratorWizard onConfirm={handleConfirm} onCancel={exit} />
      </Box>
    )
  }

  return (
    <OperationOverlay
      title={`Generating @crowd/${spec?.name ?? 'lib'}...`}
      logs={logs}
      done={phase === 'done'}
      error={error}
    />
  )
}

export default class GenerateLibCommand extends Command {
  static description = 'Scaffold a new shared library in services/libs'

  async run() {
    const {waitUntilExit} = render(<GenerateLibApp />)
    await waitUntilExit()
  }
}
