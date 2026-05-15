import {useState} from 'react'
import {Box, Text, useInput} from 'ink'
import {getAppServiceNames} from '../lib/services.js'
import {loadPresets, savePreset, Preset} from '../lib/presets.js'

interface Props {
  onConfirm: (services: string[], devMode: boolean) => void
  onCancel: () => void
}

export function ServiceSelector({onConfirm, onCancel}: Props) {
  const [services] = useState(() => getAppServiceNames())
  const [cursor, setCursor] = useState(0)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [devMode, setDevMode] = useState(true)
  const [presets, setPresets] = useState<Preset[]>(() => loadPresets())
  const [savingPreset, setSavingPreset] = useState(false)
  const [saveInput, setSaveInput] = useState('')

  useInput((input, key) => {
    if (savingPreset) {
      if (key.escape) { setSavingPreset(false); setSaveInput(''); return }
      if (key.return) {
        const name = saveInput.trim()
        if (name) {
          savePreset({name, services: Array.from(selected), dev: devMode})
          setPresets(loadPresets())
        }
        setSavingPreset(false)
        setSaveInput('')
        return
      }
      if (key.backspace || key.delete) { setSaveInput(s => s.slice(0, -1)); return }
      if (input && !key.ctrl && !key.meta) { setSaveInput(s => s + input); return }
      return
    }

    if (key.escape) { onCancel(); return }
    if (key.return) {
      if (selected.size === 0) return
      onConfirm(Array.from(selected), devMode)
      return
    }
    if (key.upArrow || input === 'k') { setCursor(c => Math.max(0, c - 1)); return }
    if (key.downArrow || input === 'j') { setCursor(c => Math.min(services.length - 1, c + 1)); return }
    if (input === ' ') {
      const svc = services[cursor]
      setSelected(prev => {
        const next = new Set(prev)
        if (next.has(svc)) { next.delete(svc) } else { next.add(svc) }
        return next
      })
      return
    }
    if (input === 'a') { setSelected(new Set(services)); return }
    if (input === 'n') { setSelected(new Set()); return }
    if (input === 'm') { setDevMode(d => !d); return }
    if (input === 's' && selected.size > 0) { setSavingPreset(true); return }

    const num = parseInt(input, 10)
    if (!isNaN(num) && num >= 1 && num <= presets.length) {
      const p = presets[num - 1]
      setSelected(new Set(p.services.filter(s => services.includes(s))))
      setDevMode(p.dev)
      return
    }
  })

  const VISIBLE = 15
  const start = Math.max(0, Math.min(cursor - Math.floor(VISIBLE / 2), services.length - VISIBLE))
  const visible = services.slice(start, start + VISIBLE)

  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={1} paddingY={0}>
      <Text color="cyan" bold>Start Services</Text>

      {presets.length > 0 && (
        <>
          <Text color="gray">{'─'.repeat(40)}</Text>
          <Box gap={1} flexWrap="wrap">
            <Text color="gray" bold>PRESETS</Text>
            {presets.slice(0, 9).map((p, i) => (
              <Box key={p.name} gap={0}>
                <Text color="yellow" bold>[{i + 1}]</Text>
                <Text color="gray"> {p.name}</Text>
                <Text color="gray" dimColor>({p.services.length})</Text>
                <Text color="gray">  </Text>
              </Box>
            ))}
          </Box>
        </>
      )}

      <Text color="gray">{'─'.repeat(40)}</Text>
      {visible.map((svc, i) => {
        const idx = start + i
        const isActive = idx === cursor
        const isSelected = selected.has(svc)
        return (
          <Box key={svc} gap={1}>
            <Text color={isActive ? 'cyan' : 'white'} inverse={isActive}>
              {isSelected ? '[◼]' : '[◻]'} {svc.padEnd(36)}
            </Text>
          </Box>
        )
      })}

      <Text color="gray">{'─'.repeat(40)}</Text>
      <Box gap={2}>
        <Text color="gray">Mode:</Text>
        <Text color={devMode ? 'yellow' : 'green'} bold>{devMode ? 'dev' : 'normal'}</Text>
      </Box>

      {savingPreset ? (
        <Box gap={1} marginTop={1}>
          <Text color="cyan">Save preset as:</Text>
          <Text color="white">{saveInput}<Text color="cyan">█</Text></Text>
          <Text color="gray" dimColor>[enter] save  [esc] cancel</Text>
        </Box>
      ) : (
        <Box flexDirection="column" marginTop={1}>
          <Text color="gray">[space] toggle  [a] all  [n] none  [m] mode  [↑↓/jk] nav  [enter] start  [esc] cancel</Text>
          <Text color="gray">{selected.size > 0 ? `[s] save as preset  ` : ''}{presets.length > 0 ? '[1-9] load preset' : ''}</Text>
        </Box>
      )}

      {selected.size > 0 && !savingPreset && (
        <Text color="cyan">{selected.size} selected</Text>
      )}
    </Box>
  )
}
