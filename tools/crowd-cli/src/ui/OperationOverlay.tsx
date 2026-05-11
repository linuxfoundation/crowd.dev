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

  return (
    <Box flexDirection="column" height={process.stdout.rows || 24} paddingX={1}>
      {visible.map((log, i) => (
        <Text key={i} color="gray">{log}</Text>
      ))}
      <Box marginTop={1} flexDirection="column">
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
    </Box>
  )
}
