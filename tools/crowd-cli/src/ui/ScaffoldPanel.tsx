import {memo} from 'react'
import {Box, Text} from 'ink'
import {ContainerInfo, ContainerStatus} from '../lib/docker.js'
import {getScaffoldServiceNames} from '../lib/services.js'
import {theme, statusLabel} from './theme.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  focused: boolean
  cursor: number
  errorServices: Set<string>
  height: number
  scrollOffset: number
}

function StatusDot({status}: {status: ContainerStatus | 'unknown'}) {
  return <Text color={theme[status] ?? theme.unknown}>{status === 'running' ? '●' : '○'}</Text>
}

export const ScaffoldPanel = memo(function ScaffoldPanel({statuses, focused, cursor, errorServices, height, scrollOffset}: Props) {
  const services = getScaffoldServiceNames()
  const total = services.length
  const totalRows = Math.max(1, height - 1)  // subtract header row

  const hasTop = scrollOffset > 0
  const itemsIfNoBottom = totalRows - (hasTop ? 1 : 0)
  const hasBottom = scrollOffset + itemsIfNoBottom < total
  const itemCount = Math.max(0, totalRows - (hasTop ? 1 : 0) - (hasBottom ? 1 : 0))
  const visibleServices = services.slice(scrollOffset, scrollOffset + itemCount)
  const belowCount = total - scrollOffset - itemCount

  return (
    <Box flexDirection="column" width={32}>
      <Text color={focused ? 'cyan' : 'gray'} bold>INFRASTRUCTURE</Text>
      {hasTop && (
        <Text color="gray" dimColor>  ↑ {scrollOffset} more</Text>
      )}
      {visibleServices.map((svc, ri) => {
        const idx = scrollOffset + ri
        const info = statuses[svc]
        const status = info?.status ?? 'unknown'
        const active = focused && idx === cursor
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
      {hasBottom && (
        <Text color="gray" dimColor>  ↓ {belowCount} more</Text>
      )}
    </Box>
  )
})
