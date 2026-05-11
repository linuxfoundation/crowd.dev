import {memo} from 'react'
import {Box, Text} from 'ink'
import {ContainerInfo, ContainerStatus} from '../lib/docker.js'
import {getScaffoldServiceNames} from '../lib/services.js'
import {theme} from './theme.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  focused: boolean
  cursor: number
  errorServices: Set<string>
}

function StatusDot({status}: {status: ContainerStatus | 'unknown'}) {
  return <Text color={theme[status] ?? theme.unknown}>{status === 'running' ? '●' : '○'}</Text>
}

function statusLabel(status: ContainerStatus | 'unknown'): string {
  if (status === 'unknown' || status === 'exited') return 'stopped'
  return status
}

export const ScaffoldPanel = memo(function ScaffoldPanel({statuses, focused, cursor, errorServices}: Props) {
  return (
    <Box flexDirection="column" width={32}>
      <Text color={focused ? 'cyan' : 'gray'} bold>INFRASTRUCTURE</Text>
      {getScaffoldServiceNames().map((svc, i) => {
        const info = statuses[svc]
        const status = info?.status ?? 'unknown'
        const active = focused && i === cursor
        const hasError = errorServices.has(svc)
        return (
          <Box key={svc} gap={1}>
            <Text color="cyan">{active ? '▶' : ' '}</Text>
            <StatusDot status={status} />
            <Box width={14}><Text bold={active}>{svc}</Text></Box>
            <Text color="red" bold>{hasError ? '!!' : '  '}</Text>
            <Text color={theme[status] ?? theme.unknown}>{statusLabel(status)}</Text>
          </Box>
        )
      })}
    </Box>
  )
})
