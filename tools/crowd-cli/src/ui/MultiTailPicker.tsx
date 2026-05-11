import {useState} from 'react'
import {Box, Text, useInput} from 'ink'

interface Service {
  name: string
  containerName: string
  devMode: boolean
}

interface Props {
  runningServices: Service[]
  onConfirm: (selected: Service[]) => void
  onCancel: () => void
}

export function MultiTailPicker({runningServices, onConfirm, onCancel}: Props) {
  const [cursor, setCursor] = useState(0)
  const [selected, setSelected] = useState<Set<number>>(new Set())

  useInput((input, key) => {
    if (key.escape) { onCancel(); return }

    if (input === 'k' || key.upArrow) {
      setCursor(c => (c === 0 ? runningServices.length - 1 : c - 1))
      return
    }
    if (input === 'j' || key.downArrow) {
      setCursor(c => (c === runningServices.length - 1 ? 0 : c + 1))
      return
    }
    if (input === ' ') {
      setSelected(prev => {
        const next = new Set(prev)
        if (next.has(cursor)) {
          next.delete(cursor)
        } else if (next.size < 2) {
          next.add(cursor)
        }
        return next
      })
      return
    }
    if (key.return && selected.size > 0) {
      const svcs = Array.from(selected).sort((a, b) => a - b).map(i => runningServices[i])
      onConfirm(svcs)
    }
  })

  return (
    <Box flexDirection="column" paddingX={1}>
      <Box marginBottom={1}>
        <Text bold color="cyan">Select services to tail (max 2)</Text>
      </Box>
      <Box flexDirection="column" marginBottom={1}>
        {runningServices.map((svc: Service, idx: number) => {
          const isSel = selected.has(idx)
          const isCur = idx === cursor
          return (
            <Box key={svc.name} flexDirection="row">
              <Text>{isCur ? '❯ ' : '  '}</Text>
              <Text color={isCur ? 'cyan' : undefined}>{isSel ? '◼' : '◻'}</Text>
              <Text color={isCur ? 'cyan' : undefined}> {svc.name}{svc.devMode ? ' [dev]' : ''}</Text>
            </Box>
          )
        })}
      </Box>
      <Text color="gray" dimColor>selected: {selected.size}/2  [↑↓/jk] move  [space] toggle  [enter] confirm  [esc] cancel</Text>
    </Box>
  )
}
