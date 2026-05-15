import {useState} from 'react'
import {Box, Text, useInput} from 'ink'
import {
  getExistingServiceNames,
  getNextDebugPort,
  getNextApiPort,
  planServiceFiles,
  ServiceSpec,
  ServiceType,
} from '../lib/generateService.js'

interface Props {
  onConfirm: (spec: ServiceSpec, debugPort: number) => void
  onCancel: () => void
}

type Step = 'name' | 'type' | 'taskQueue' | 'port' | 'confirm'

const TYPES: {value: ServiceType; desc: string}[] = [
  {value: 'worker',   desc: 'Temporal task queue worker'},
  {value: 'consumer', desc: 'Kafka queue consumer'},
  {value: 'api',      desc: 'Express HTTP API'},
]

function validateName(name: string, existing: string[]): string {
  if (!name) return 'name required'
  if (!/^[a-z][a-z0-9-]*$/.test(name)) return 'lowercase letters, numbers, hyphens only'
  if (existing.includes(name)) return `"${name}" already exists`
  return ''
}

export function ServiceGeneratorWizard({onConfirm, onCancel}: Props) {
  const [step, setStep] = useState<Step>('name')
  const [name, setName] = useState('')
  const [typeIdx, setTypeIdx] = useState(0)
  const [taskQueue, setTaskQueue] = useState('')
  const [portInput, setPortInput] = useState('')
  const [nameError, setNameError] = useState('')
  const [debugPort] = useState(() => getNextDebugPort())
  const [defaultApiPort] = useState(() => getNextApiPort())
  const [existing] = useState(() => getExistingServiceNames())

  const selectedType = TYPES[typeIdx].value

  const nextStepAfterType = (): Step => {
    if (selectedType === 'worker') return 'taskQueue'
    if (selectedType === 'api') return 'port'
    return 'confirm'
  }

  useInput((input, key) => {
    if (key.escape) { onCancel(); return }

    if (step === 'name') {
      if (key.return) {
        const err = validateName(name, existing)
        if (err) { setNameError(err); return }
        setNameError('')
        setStep('type')
        return
      }
      if (key.backspace || key.delete) { setName(s => s.slice(0, -1)); setNameError(''); return }
      if (input && /^[a-z0-9-]$/.test(input)) { setName(s => s + input); setNameError(''); return }
      return
    }

    if (step === 'type') {
      if (key.upArrow || input === 'k') { setTypeIdx(i => Math.max(0, i - 1)); return }
      if (key.downArrow || input === 'j') { setTypeIdx(i => Math.min(TYPES.length - 1, i + 1)); return }
      if (key.return) { setStep(nextStepAfterType()); return }
      return
    }

    if (step === 'taskQueue') {
      if (key.return) {
        if (!taskQueue.trim()) return
        setStep('confirm')
        return
      }
      if (key.backspace || key.delete) { setTaskQueue(s => s.slice(0, -1)); return }
      if (input && /^[a-z0-9-]$/.test(input)) { setTaskQueue(s => s + input); return }
      return
    }

    if (step === 'port') {
      if (key.return) {
        const p = portInput.trim() ? parseInt(portInput, 10) : defaultApiPort
        if (isNaN(p) || p < 1024 || p > 65535) return
        setStep('confirm')
        return
      }
      if (key.backspace || key.delete) { setPortInput(s => s.slice(0, -1)); return }
      if (input && /^\d$/.test(input)) { setPortInput(s => s + input); return }
      return
    }

    if (step === 'confirm') {
      if (key.return) {
        const resolvedPort = portInput.trim() ? parseInt(portInput, 10) : defaultApiPort
        const spec: ServiceSpec = {
          name,
          type: selectedType,
          ...(selectedType === 'worker' ? {taskQueue} : {}),
          ...(selectedType === 'api' ? {port: resolvedPort} : {}),
        }
        onConfirm(spec, debugPort)
        return
      }
      return
    }
  })

  const resolvedPort = portInput.trim() ? parseInt(portInput, 10) : defaultApiPort
  const previewSpec: ServiceSpec | null =
    name && !validateName(name, existing)
      ? {
          name,
          type: selectedType,
          ...(selectedType === 'worker' && taskQueue ? {taskQueue} : {}),
          ...(selectedType === 'api' ? {port: resolvedPort} : {}),
        }
      : null

  const files = previewSpec && step === 'confirm' ? planServiceFiles(previewSpec, debugPort) : []

  const pastStep = (s: Step): boolean => {
    const order: Step[] = ['name', 'type', 'taskQueue', 'port', 'confirm']
    return order.indexOf(step) > order.indexOf(s)
  }

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text color="cyan" bold>Generate New Service</Text>

      {/* Step 1 — name */}
      <Text color="gray">{'─'.repeat(44)}</Text>
      <Box gap={1}>
        <Text color={step === 'name' ? 'cyan' : 'gray'}>Service name:</Text>
        <Text color="white">
          {name}
          {step === 'name' && <Text color="cyan">█</Text>}
        </Text>
        {step === 'name' && nameError && <Text color="red"> ← {nameError}</Text>}
      </Box>

      {/* Step 2 — type */}
      {step !== 'name' && (
        <>
          <Text color="gray">{'─'.repeat(44)}</Text>
          <Text color={step === 'type' ? 'cyan' : 'gray'}>Service type:</Text>
          <Box flexDirection="column" marginLeft={1}>
            {TYPES.map((t, i) => {
              const active = step === 'type' && i === typeIdx
              const chosen = pastStep('type') && i === typeIdx
              return (
                <Box key={t.value} gap={1}>
                  <Text color={active || chosen ? 'cyan' : 'gray'}>
                    {active ? '►' : chosen ? '✓' : ' '} {t.value}
                  </Text>
                  <Text color="gray" dimColor>{t.desc}</Text>
                </Box>
              )
            })}
          </Box>
        </>
      )}

      {/* Step 3a — task queue (workers only) */}
      {(step === 'taskQueue' || (pastStep('taskQueue') && selectedType === 'worker')) && (
        <>
          <Text color="gray">{'─'.repeat(44)}</Text>
          <Box gap={1}>
            <Text color={step === 'taskQueue' ? 'cyan' : 'gray'}>Task queue name:</Text>
            <Text color="white">
              {taskQueue}
              {step === 'taskQueue' && <Text color="cyan">█</Text>}
            </Text>
          </Box>
        </>
      )}

      {/* Step 3b — HTTP port (api only) */}
      {(step === 'port' || (pastStep('port') && selectedType === 'api')) && (
        <>
          <Text color="gray">{'─'.repeat(44)}</Text>
          <Box gap={1}>
            <Text color={step === 'port' ? 'cyan' : 'gray'}>HTTP port:</Text>
            <Text color="white">
              {portInput || String(defaultApiPort)}
              {step === 'port' && <Text color="cyan">█</Text>}
            </Text>
            {step === 'port' && (
              <Text color="gray" dimColor>(default: {defaultApiPort})</Text>
            )}
          </Box>
        </>
      )}

      {/* Step 4 — confirm */}
      {step === 'confirm' && files.length > 0 && (
        <>
          <Text color="gray">{'─'.repeat(44)}</Text>
          <Text color="green">Files to create:</Text>
          {files.map(f => (
            <Text key={f.path} color="gray">  + {f.path}</Text>
          ))}
          <Box gap={2} marginTop={1}>
            <Box gap={1}><Text color="gray" dimColor>debug port:</Text><Text color="yellow">{debugPort}</Text></Box>
            {selectedType === 'api' && (
              <Box gap={1}><Text color="gray" dimColor>http port:</Text><Text color="yellow">{resolvedPort}</Text></Box>
            )}
          </Box>
        </>
      )}

      {/* Hints */}
      <Text color="gray">{'─'.repeat(44)}</Text>
      {step === 'name'      && <Text color="gray">[a-z0-9-]  [enter] next  [esc] cancel</Text>}
      {step === 'type'      && <Text color="gray">[↑↓/jk] select  [enter] next  [esc] cancel</Text>}
      {step === 'taskQueue' && <Text color="gray">[a-z0-9-]  [enter] next  [esc] cancel</Text>}
      {step === 'port'      && <Text color="gray">[0-9]  [enter] confirm  [esc] cancel</Text>}
      {step === 'confirm'   && <Text color="gray">[enter] generate + pnpm install  [esc] cancel</Text>}
    </Box>
  )
}
