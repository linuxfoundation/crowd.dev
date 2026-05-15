import {useState, useEffect} from 'react'
import {Box, Text} from 'ink'

interface Props {
  title: string
  logs: string[]
  done: boolean
  error: string | null
}

const FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
const MAX_VISIBLE = 20

export function OperationOverlay({title, logs, done, error}: Props) {
  const [frame, setFrame] = useState(0)

  useEffect(() => {
    if (done) return
    const t = setInterval(() => setFrame(f => (f + 1) % FRAMES.length), 100)
    return () => clearInterval(t)
  }, [done])

  const visible = logs.slice(-MAX_VISIBLE)
  // Pad to MAX_VISIBLE rows so the status block stays anchored at top
  // and the log area doesn't shift as lines arrive.
  const padded = Array(Math.max(0, MAX_VISIBLE - visible.length)).fill('').concat(visible)

  return (
    <Box flexDirection="column" height={process.stdout.rows || 24} paddingX={1}>
      <Box marginBottom={1} flexDirection="column">
        {!done ? (
          <Text color="yellow">{FRAMES[frame]}  {title}</Text>
        ) : error ? (
          <>
            <Text color="red">✗  Error: {error}</Text>
            <Text color="gray">  Press any key to return.</Text>
          </>
        ) : (
          <>
            <Text color="green">✓  Done.</Text>
            <Text color="gray">  Press any key to return.</Text>
          </>
        )}
      </Box>
      {padded.map((log, i) => (
        <Text key={i} color="gray">{log}</Text>
      ))}
    </Box>
  )
}
