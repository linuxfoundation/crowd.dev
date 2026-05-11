import {useState, useEffect, memo} from 'react'
import {Box, Text, useInput} from 'ink'

interface Props {
  errorLines: Record<string, string[]>
  onClearService: (svc: string) => void
  onExit: () => void
}

const MAX_PER_SERVICE = 8

export const AllErrorsOverlay = memo(function AllErrorsOverlay({errorLines, onClearService, onExit}: Props) {
  const [cursor, setCursor] = useState(0)
  const services = Object.entries(errorLines).filter(([, lines]) => lines.length > 0)

  useEffect(() => {
    if (services.length > 0 && cursor >= services.length) setCursor(services.length - 1)
  }, [services.length, cursor])

  useInput((input, key) => {
    if (input === 'q' || key.escape) { onExit(); return }
    if (key.upArrow || input === 'k') { setCursor(c => Math.max(0, c - 1)); return }
    if (key.downArrow || input === 'j') { setCursor(c => Math.min(services.length - 1, c + 1)); return }
    if (input === 'c' && services[cursor]) { onClearService(services[cursor][0]); return }
    if (input === 'C') { for (const [svc] of services) onClearService(svc); return }
  })

  return (
    <Box flexDirection="column" height={process.stdout.rows || 24} paddingX={1}>
      <Box gap={2} marginBottom={1}>
        <Text color="red" bold>All Error Logs</Text>
        {services.length > 0 && (
          <Text color="yellow" dimColor>{services.length} service(s) with errors</Text>
        )}
        <Text color="gray">[↑↓/jk] nav  [c] clear service  [C] clear all  [q/esc] back</Text>
      </Box>
      {services.length === 0 ? (
        <Text color="gray">no errors detected</Text>
      ) : (
        services.map(([svc, lines], idx) => {
          const isSelected = idx === cursor
          const shown = lines.slice(-MAX_PER_SERVICE)
          const hidden = lines.length - shown.length
          return (
            <Box key={svc} flexDirection="column" marginBottom={1}>
              <Box gap={2}>
                <Text color={isSelected ? 'cyan' : 'yellow'} bold inverse={isSelected}>── {svc} ──</Text>
                {hidden > 0 && <Text color="gray" dimColor>(+{hidden} earlier)</Text>}
                {isSelected && <Text color="gray" dimColor>[c] clear this</Text>}
              </Box>
              {shown.map((line, i) => (
                <Text key={i} wrap="truncate">{line}</Text>
              ))}
            </Box>
          )
        })
      )}
    </Box>
  )
})
