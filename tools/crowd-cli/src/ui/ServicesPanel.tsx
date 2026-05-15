import {memo} from 'react'
import {Box, Text} from 'ink'
import {ContainerInfo, ContainerStatus} from '../lib/docker.js'
import {theme, statusLabel} from './theme.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  focused: boolean
  cursor: number
  errorServices: Set<string>
  serviceNames: string[]
  height: number
  scrollOffset: number
}

function StatusDot({status}: {status: ContainerStatus | 'unknown'}) {
  return <Text color={theme[status] ?? theme.unknown}>{status === 'running' ? '●' : '○'}</Text>
}

const COL_WIDTH = 46

export const ServicesPanel = memo(function ServicesPanel({statuses, focused, cursor, errorServices, serviceNames, height, scrollOffset}: Props) {
  const colSize = Math.ceil(serviceNames.length / 2)
  const col0 = serviceNames.slice(0, colSize)
  const col1 = serviceNames.slice(colSize)
  const totalRows = colSize

  const totalVisibleRows = Math.max(1, height - 1)  // subtract header row

  const hasTop = scrollOffset > 0
  const itemsIfNoBottom = totalVisibleRows - (hasTop ? 1 : 0)
  const hasBottom = scrollOffset + itemsIfNoBottom < totalRows
  const rowCount = Math.max(0, totalVisibleRows - (hasTop ? 1 : 0) - (hasBottom ? 1 : 0))

  const visibleCol0 = col0.slice(scrollOffset, scrollOffset + rowCount)
  const visibleCol1 = col1.slice(scrollOffset, scrollOffset + rowCount)
  const belowCount = totalRows - scrollOffset - rowCount

  const renderItem = (svc: string, idx: number) => {
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
  }

  return (
    <Box flexDirection="column">
      <Text color={focused ? 'cyan' : 'gray'} bold>SERVICES</Text>
      {hasTop && (
        <Text color="gray" dimColor>  ↑ {scrollOffset} more rows</Text>
      )}
      <Box flexDirection="row" gap={2}>
        <Box flexDirection="column" width={COL_WIDTH}>
          {visibleCol0.map((svc, ri) => renderItem(svc, scrollOffset + ri))}
        </Box>
        <Box flexDirection="column" width={COL_WIDTH}>
          {visibleCol1.map((svc, ri) => renderItem(svc, colSize + scrollOffset + ri))}
        </Box>
      </Box>
      {hasBottom && (
        <Text color="gray" dimColor>  ↓ {belowCount} more rows</Text>
      )}
    </Box>
  )
})
