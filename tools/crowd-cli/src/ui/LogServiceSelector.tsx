import {useState} from 'react'
import {Box, Text, useInput} from 'ink'
import {ContainerInfo} from '../lib/docker.js'
import {theme} from './theme.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  onSelect: (service: string) => void
  onCancel: () => void
}

export function LogServiceSelector({statuses, onSelect, onCancel}: Props) {
  const services = Object.keys(statuses).sort()
  const [cursor, setCursor] = useState(0)

  useInput((input, key) => {
    if (key.escape || input === 'q') { onCancel(); return }
    if (key.return && services.length > 0) { onSelect(services[cursor]); return }
    if (key.upArrow || input === 'k') setCursor(c => Math.max(0, c - 1))
    if (key.downArrow || input === 'j') setCursor(c => Math.min(services.length - 1, c + 1))
  })

  const VISIBLE = 20
  const start = Math.max(0, Math.min(cursor - Math.floor(VISIBLE / 2), services.length - VISIBLE))
  const visible = services.slice(start, start + VISIBLE)

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={1} paddingY={0}>
      <Text color="cyan" bold>View Logs</Text>
      <Text color="gray">{'─'.repeat(40)}</Text>
      {services.length === 0 ? (
        <Text color="gray">No running containers found.</Text>
      ) : (
        visible.map((svc, i) => {
          const idx = start + i
          const isActive = idx === cursor
          const status = statuses[svc]?.status ?? 'unknown'
          return (
            <Box key={svc} gap={1}>
              <Text color={isActive ? 'cyan' : 'white'} inverse={isActive}>
                <Text color={theme[status] ?? theme.unknown}>{status === 'running' ? '●' : '○'}</Text>
                {' '}{svc.padEnd(36)}
              </Text>
            </Box>
          )
        })
      )}
      <Text color="gray">{'─'.repeat(40)}</Text>
      <Text color="gray">[↑↓/jk] nav  [enter] view logs  [q/esc] cancel</Text>
    </Box>
  )
}
