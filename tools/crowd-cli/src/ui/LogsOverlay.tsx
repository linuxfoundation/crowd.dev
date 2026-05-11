import {useState, useEffect, useRef} from 'react'
import {Box, Text, useInput} from 'ink'
import {execa} from 'execa'
import {isErrorLine} from '../lib/errors.js'

interface Props {
  service: string
  devMode: boolean
  containerName: string
  onExit: () => void
  errorFilter?: boolean
  onClear?: () => void
}

const MAX_LINES = 40
const FLUSH_MS = 120

export function LogsOverlay({service, devMode, containerName, onExit, errorFilter, onClear}: Props) {
  const [lines, setLines] = useState<string[]>([])
  const [dead, setDead] = useState(false)
  const procRef = useRef<ReturnType<typeof execa> | null>(null)
  const bufRef = useRef<string[]>([])
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useInput((input, key) => {
    if (input === 'q' || key.escape) {
      procRef.current?.kill()
      if (timerRef.current) clearTimeout(timerRef.current)
      onExit()
    }
    if (input === 'c' && errorFilter && onClear) onClear()
  })

  useEffect(() => {
    let mounted = true
    // Clear previous stream's content when restarting (service/filter change)
    setLines([])
    setDead(false)
    bufRef.current = []
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null }

    const flush = () => {
      const batch = bufRef.current.splice(0)
      if (batch.length > 0 && mounted) setLines(prev => [...prev, ...batch].slice(-MAX_LINES))
      timerRef.current = null
    }

    const addChunk = (d: Buffer) => {
      const newLines = d.toString().split('\n').filter(Boolean)
      bufRef.current.push(...(errorFilter ? newLines.filter(isErrorLine) : newLines))
      if (!timerRef.current) timerRef.current = setTimeout(flush, FLUSH_MS)
    }

    const proc = execa('docker', ['logs', '--tail', '500', '-f', containerName], {reject: false})
    procRef.current = proc
    proc.stdout?.on('data', addChunk)
    proc.stderr?.on('data', addChunk)
    proc.then(() => { flush(); if (mounted) setDead(true) }).catch(() => { flush(); if (mounted) setDead(true) })

    return () => {
      mounted = false
      if (timerRef.current) clearTimeout(timerRef.current)
      proc.kill()
    }
  }, [service, devMode, errorFilter])

  return (
    <Box flexDirection="column" height={process.stdout.rows || 24} paddingX={1}>
      <Box gap={2} marginBottom={1}>
        <Text color={errorFilter ? 'red' : 'cyan'} bold>
          {errorFilter ? 'Error Logs' : 'Logs'}: {service}{devMode ? ' [dev]' : ''}
        </Text>
        {errorFilter && <Text color="yellow" dimColor>errors only</Text>}
        <Text color="gray">[q / esc] back</Text>
        {errorFilter && onClear && <Text color="gray">[c] clear</Text>}
      </Box>
      {lines.map((line, i) => (
        <Text key={i} wrap="truncate">{line}</Text>
      ))}
      {dead && <Text color="yellow">— container stopped —</Text>}
      {lines.length === 0 && !dead && (
        <Text color="gray">{errorFilter ? 'no error lines found in last 500 log lines' : 'waiting for output...'}</Text>
      )}
    </Box>
  )
}
