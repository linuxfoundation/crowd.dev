import {useState, useCallback} from 'react'
import {Box, useApp, useInput} from 'ink'
import {Command} from '@oclif/core'
import {render} from 'ink'
import {ServiceGeneratorWizard} from '../../ui/ServiceGeneratorWizard.js'
import {OperationOverlay} from '../../ui/OperationOverlay.js'
import {generateService, ServiceSpec} from '../../lib/generateService.js'

function GenerateServiceApp() {
  const {exit} = useApp()
  const [phase, setPhase] = useState<'wizard' | 'running' | 'done'>('wizard')
  const [spec, setSpec] = useState<ServiceSpec | null>(null)
  const [logs, setLogs] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)

  useInput((_input, key) => {
    if (phase === 'done' && (key.return || key.escape)) exit()
  })

  const handleConfirm = useCallback((s: ServiceSpec, debugPort: number) => {
    setSpec(s)
    setPhase('running')
    generateService(s, debugPort, line => setLogs(prev => [...prev, line]))
      .then(() => setPhase('done'))
      .catch(err => {
        setError(err instanceof Error ? err.message : String(err))
        setPhase('done')
      })
  }, [])

  if (phase === 'wizard') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <ServiceGeneratorWizard onConfirm={handleConfirm} onCancel={exit} />
      </Box>
    )
  }

  return (
    <OperationOverlay
      title={`Generating ${spec?.name ?? 'service'}...`}
      logs={logs}
      done={phase === 'done'}
      error={error}
    />
  )
}

export default class GenerateServiceCommand extends Command {
  static description = 'Scaffold a new microservice (worker / consumer / api)'

  async run() {
    const {waitUntilExit} = render(<GenerateServiceApp />)
    await waitUntilExit()
  }
}
