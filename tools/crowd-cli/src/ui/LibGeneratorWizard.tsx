import {useState} from 'react'
import {Box, Text, useInput} from 'ink'
import {getExistingLibNames, planLibFiles, LibSpec} from '../lib/generateService.js'

interface Props {
  onConfirm: (spec: LibSpec) => void
  onCancel: () => void
}

type Step = 'name' | 'confirm'

function validateName(name: string, existing: string[]): string {
  if (!name) return 'name required'
  if (!/^[a-z][a-z0-9-]*$/.test(name)) return 'lowercase letters, numbers, hyphens only'
  if (existing.includes(name)) return `"${name}" already exists`
  return ''
}

export function LibGeneratorWizard({onConfirm, onCancel}: Props) {
  const [step, setStep] = useState<Step>('name')
  const [name, setName] = useState('')
  const [nameError, setNameError] = useState('')
  const [existing] = useState(() => getExistingLibNames())

  useInput((input, key) => {
    if (key.escape) { onCancel(); return }

    if (step === 'name') {
      if (key.return) {
        const err = validateName(name, existing)
        if (err) { setNameError(err); return }
        setNameError('')
        setStep('confirm')
        return
      }
      if (key.backspace || key.delete) { setName(s => s.slice(0, -1)); setNameError(''); return }
      if (input && /^[a-z0-9-]$/.test(input)) { setName(s => s + input); setNameError(''); return }
      return
    }

    if (step === 'confirm') {
      if (key.return) { onConfirm({name}); return }
      return
    }
  })

  const spec: LibSpec | null = name && !validateName(name, existing) ? {name} : null
  const files = spec && step === 'confirm' ? planLibFiles(spec) : []

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2} paddingY={1}>
      <Text color="cyan" bold>Generate New Library</Text>

      <Text color="gray">{'─'.repeat(44)}</Text>
      <Box gap={1}>
        <Text color={step === 'name' ? 'cyan' : 'gray'}>Library name:</Text>
        <Text color="white">
          {name}
          {step === 'name' && <Text color="cyan">█</Text>}
        </Text>
        {step === 'name' && nameError && <Text color="red"> ← {nameError}</Text>}
      </Box>

      {step === 'confirm' && files.length > 0 && (
        <>
          <Text color="gray">{'─'.repeat(44)}</Text>
          <Text color="green">Files to create:</Text>
          {files.map(f => (
            <Text key={f.path} color="gray">  + {f.path}</Text>
          ))}
          <Box marginTop={1}>
            <Text color="gray" dimColor>Add </Text>
            <Text color="white">@crowd/{name}: workspace:*</Text>
            <Text color="gray" dimColor> to any service that needs it</Text>
          </Box>
        </>
      )}

      <Text color="gray">{'─'.repeat(44)}</Text>
      {step === 'name'    && <Text color="gray">[a-z0-9-]  [enter] next  [esc] cancel</Text>}
      {step === 'confirm' && <Text color="gray">[enter] generate + pnpm install  [esc] cancel</Text>}
    </Box>
  )
}
