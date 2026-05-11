import React from 'react'
import {useState, useEffect, useRef} from 'react'
import {Box, Text, useInput} from 'ink'
import {execa} from 'execa'
import {isErrorLine} from '../lib/errors.js'

interface Service {
  name: string
  containerName: string
  devMode: boolean
}

interface Props {
  services: Service[]
  onExit: () => void
}

interface PanelState {
  service: Service
  lines: string[]
  stopped: boolean
}

type ProcMap = Record<string, ReturnType<typeof execa>>

const MAX_LINES = 200
const FLUSH_MS = 120

export function MultiTailOverlay({services, onExit}: Props) {
  const [panels, setPanels] = useState<Map<string, PanelState>>(
    () => new Map<string, PanelState>(
      services.map(svc => [svc.name, {service: svc, lines: [], stopped: false}])
    )
  )

  const procsRef = useRef<ProcMap>({})
  const bufferRef = useRef<Record<string, string[]>>({})
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useInput((input, key) => {
    if (input === 'q' || key.escape) {
      for (const proc of Object.values(procsRef.current)) {
        try { proc.kill() } catch {}
      }
      onExit()
    }
  })

  useEffect(() => {
    bufferRef.current = Object.fromEntries(services.map(s => [s.name, []]))

    for (const svc of services) {
      const proc = execa('docker', ['logs', '--tail', '200', '-f', svc.containerName], {
        reject: false,
      })
      procsRef.current[svc.name] = proc

      let partial = ''
      const onChunk = (chunk: Buffer) => {
        partial += chunk.toString()
        const parts = partial.split('\n')
        partial = parts.pop() ?? ''
        const newLines = parts.filter(l => l.length > 0)
        if (newLines.length > 0) bufferRef.current[svc.name]?.push(...newLines)
      }
      proc.stdout?.on('data', onChunk)
      proc.stderr?.on('data', onChunk)

      proc.on('exit', () => {
        setPanels(prev => {
          const next = new Map(prev)
          const p = next.get(svc.name)
          if (p) next.set(svc.name, {...p, stopped: true})
          return next
        })
      })
    }

    timerRef.current = setInterval(() => {
      let changed = false
      const updates: Record<string, string[]> = {}
      for (const [name, buf] of Object.entries(bufferRef.current)) {
        if (buf.length > 0) {
          updates[name] = buf.splice(0)
          changed = true
        }
      }
      if (!changed) return
      setPanels(prev => {
        const next = new Map(prev)
        for (const [name, newLines] of Object.entries(updates)) {
          const p = next.get(name)
          if (!p) continue
          const merged = [...p.lines, ...newLines]
          next.set(name, {...p, lines: merged.length > MAX_LINES ? merged.slice(-MAX_LINES) : merged})
        }
        return next
      })
    }, FLUSH_MS)

    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
      for (const proc of Object.values(procsRef.current)) {
        try { proc.kill() } catch {}
      }
    }
  }, [])

  const cols = process.stdout.columns || 80
  const rows = process.stdout.rows || 24
  const panelRows = rows - 3
  const isSingle = services.length === 1
  const panelWidth = isSingle ? cols - 2 : Math.floor((cols - 3) / 2)

  const renderPanel = (panel: PanelState): React.ReactNode => {
    const {service, lines, stopped} = panel
    const visibleLines = lines.slice(-panelRows)
    return (
      <Box key={service.name} flexDirection="column" width={panelWidth}>
        <Text color="cyan" bold>{service.name}{service.devMode ? ' [dev]' : ''}</Text>
        <Box flexDirection="column">
          {visibleLines.map((line, i) => (
            <Box key={i} width={panelWidth}>
              <Text color={isErrorLine(line) ? 'red' : undefined} wrap="truncate">{line}</Text>
            </Box>
          ))}
        </Box>
        {stopped && <Text color="yellow">— container stopped —</Text>}
      </Box>
    )
  }

  const panelList = services.map(svc => panels.get(svc.name)!)

  if (isSingle) {
    return (
      <Box flexDirection="column" paddingX={1}>
        {renderPanel(panelList[0])}
        <Text color="gray" dimColor>[q/esc] back</Text>
      </Box>
    )
  }

  return (
    <Box flexDirection="column" paddingX={1}>
      <Box flexDirection="row" width={cols}>
        <Box width={panelWidth}>{renderPanel(panelList[0])}</Box>
        <Box width={1}><Text color="gray">│</Text></Box>
        <Box width={panelWidth}>{renderPanel(panelList[1])}</Box>
      </Box>
      <Text color="gray" dimColor>[q/esc] back</Text>
    </Box>
  )
}
