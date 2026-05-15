import {useMemo} from 'react'
import {Box, Text, useInput} from 'ink'
import {ContainerInfo} from '../lib/docker.js'
import {getScaffoldPorts, getAppServicePorts, ServicePortInfo, PortEntry} from '../lib/ports.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  onExit: () => void
}

function Section({
  title,
  services,
  statuses,
  emptyLabel,
}: {
  title: string
  services: ServicePortInfo[]
  statuses: Record<string, ContainerInfo>
  emptyLabel?: string
}) {
  return (
    <>
      <Text color="cyan" bold>{title}</Text>
      <Box flexDirection="row">
        <Box width={24}><Text color="gray" dimColor>service</Text></Box>
        <Box width={7}><Text color="gray" dimColor>port</Text></Box>
        <Box width={14}><Text color="gray" dimColor>status</Text></Box>
        <Text color="gray" dimColor>description</Text>
      </Box>
      {services.length === 0 && emptyLabel && (
        <Text color="gray" dimColor>  {emptyLabel}</Text>
      )}
      {services.map(svc => {
        const info = statuses[svc.name]
        const running = info?.status === 'running'
        return svc.ports.map((p: PortEntry, i: number) => (
          <Box key={`${svc.name}-${p.hostPort}`} flexDirection="row">
            <Box width={24}>
              {i === 0 && <Text color="white">{svc.name}</Text>}
            </Box>
            <Box width={7}><Text color="yellow">{p.hostPort}</Text></Box>
            <Box width={14}>
              {i === 0 && (
                <Text color={running ? 'green' : 'gray'}>{running ? '● running' : '○ stopped'}</Text>
              )}
            </Box>
            <Text color="gray" dimColor>{p.description}</Text>
          </Box>
        ))
      })}
    </>
  )
}

export function DebugPortsOverlay({statuses, onExit}: Props) {
  const scaffoldPorts = useMemo(() => getScaffoldPorts(), [])
  const appPorts = useMemo(() => getAppServicePorts(), [])

  useInput((input, key) => {
    if (input === 'q' || key.escape) onExit()
  })

  return (
    <Box flexDirection="column" paddingX={1}>
      <Box flexDirection="row" justifyContent="space-between" marginBottom={1}>
        <Text color="cyan" bold>Ports</Text>
        <Text color="gray" dimColor>[q/esc] back</Text>
      </Box>
      <Text color="gray">{'─'.repeat(process.stdout.columns || 80)}</Text>

      <Section title="INFRASTRUCTURE" services={scaffoldPorts} statuses={statuses} />
      <Text> </Text>
      <Section title="SERVICES" services={appPorts} statuses={statuses} emptyLabel="(no services expose ports)" />

      <Text color="gray">{'─'.repeat(process.stdout.columns || 80)}</Text>
    </Box>
  )
}
