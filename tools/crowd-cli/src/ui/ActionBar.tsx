import {Box, Text} from 'ink'

interface Props {
  focusPanel: 'scaffold' | 'services'
  selectedRunning: boolean
  selectedHasErrors: boolean
  anyErrors: boolean
  anyRunning: boolean
  selectedCanPsql: boolean
}

export function ActionBar({focusPanel, selectedRunning, selectedHasErrors, anyErrors, anyRunning, selectedCanPsql}: Props) {
  const width = Math.min(process.stdout.columns || 80, 120)
  return (
    <Box flexDirection="column">
      <Text color="gray">{'─'.repeat(width)}</Text>
      <Box gap={3} flexWrap="wrap">
        <Text color="gray" dimColor>[↑↓] move  [←→] switch panel</Text>
        {anyErrors && (
          <Box gap={1}><Text color="red" bold>[E]</Text><Text color="red">all error logs</Text></Box>
        )}
        {selectedRunning ? (
          <>
            <Box gap={1}><Text color="yellow" bold>[l]</Text><Text color="gray">logs</Text></Box>
            {selectedHasErrors && (
              <Box gap={1}><Text color="red" bold>[e]</Text><Text color="gray">error logs</Text></Box>
            )}
            {selectedCanPsql && (
              <Box gap={1}><Text color="yellow" bold>[p]</Text><Text color="gray">psql</Text></Box>
            )}
            {focusPanel === 'services' && (
              <>
                <Box gap={1}><Text color="yellow" bold>[k]</Text><Text color="gray">stop</Text></Box>
                <Box gap={1}><Text color="yellow" bold>[r]</Text><Text color="gray">restart</Text></Box>
              </>
            )}
          </>
        ) : (
          focusPanel === 'services' && (
            <>
              <Box gap={1}><Text color="yellow" bold>[u]</Text><Text color="gray">start dev</Text></Box>
              <Box gap={1}><Text color="yellow" bold>[U]</Text><Text color="gray">start normal</Text></Box>
            </>
          )
        )}
      </Box>
      <Box gap={3} flexWrap="wrap">
        <Text color="gray" dimColor>infra:</Text>
        {anyRunning && (
          <Box gap={1}><Text color="cyan" bold>[m]</Text><Text color="gray">multi-tail</Text></Box>
        )}
        {[
          ['U', 'up all'],
          ['d', 'down all'],
          ['R', 'reset'],
          ['F', 'full reset+restore'],
          ['D', 'destroy+vols'],
          ['s', 'start svcs'],
          ['P', 'ports'],
          ['L', 'lint'],
          ['q', 'quit'],
        ].map(([key, label]) => (
          <Box key={key} gap={1}>
            <Text color="cyan" bold>[{key}]</Text>
            <Text color="gray">{label}</Text>
          </Box>
        ))}
      </Box>
    </Box>
  )
}
