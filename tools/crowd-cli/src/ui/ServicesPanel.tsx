import {memo} from 'react'
import {Box, Text} from 'ink'
import {ContainerInfo, ContainerStatus} from '../lib/docker.js'
import {getAppServiceNames} from '../lib/services.js'
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

const COL_WIDTH = 46

export const ServicesPanel = memo(function ServicesPanel({statuses, focused, cursor, errorServices}: Props) {
  const services = getAppServiceNames()
  const colSize = Math.ceil(services.length / 2)
  const col0 = services.slice(0, colSize)
  const col1 = services.slice(colSize)

  const renderCol = (col: string[], offset: number) =>
    col.map((svc, ri) => {
      const idx = offset + ri
      const info = statuses[svc]
      const status = info?.status ?? 'unknown'
      const dev = info?.devMode ?? false
      const active = focused && idx === cursor
      const hasError = errorServices.has(svc)
      return (
        <Box key={svc} gap={1}>
          <Text color="cyan">{active ? '▶' : ' '}</Text>
          <StatusDot status={status} />
          <Box width={24}><Text bold={active} wrap="truncate">{svc}</Text></Box>
          <Text color="red" bold>{hasError ? '!!' : '  '}</Text>
          {dev
            ? <Text color="yellow" dimColor>[dev]</Text>
            : <Text>     </Text>
          }
          <Text color={theme[status] ?? theme.unknown}>{statusLabel(status)}</Text>
        </Box>
      )
    })

  return (
    <Box flexDirection="column">
      <Text color={focused ? 'cyan' : 'gray'} bold>SERVICES</Text>
      <Box flexDirection="row" gap={2}>
        <Box flexDirection="column" width={COL_WIDTH}>{renderCol(col0, 0)}</Box>
        <Box flexDirection="column" width={COL_WIDTH}>{renderCol(col1, colSize)}</Box>
      </Box>
    </Box>
  )
})
