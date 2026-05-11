import {useState, useEffect, useCallback, useRef, useMemo} from 'react'
import {Box, Text, useApp, useInput} from 'ink'
import {ScaffoldPanel} from './ScaffoldPanel.js'
import {ServicesPanel} from './ServicesPanel.js'
import {ActionBar} from './ActionBar.js'
import {NetworkPanel} from './NetworkPanel.js'
import {ServiceSelector} from './ServiceSelector.js'
import {OperationOverlay} from './OperationOverlay.js'
import {LogsOverlay} from './LogsOverlay.js'
import {getContainerStatuses, getNetworkInfo, ContainerInfo, NetworkInfo} from '../lib/docker.js'
import {scaffoldUp, scaffoldDown, scaffoldDestroy, scaffoldReset, startService, stopService, restartService} from '../lib/scaffold.js'
import {lintAll} from '../lib/lint.js'
import {getScaffoldServiceNames, getAppServiceNames} from '../lib/services.js'
import {PROJECT_NAME} from '../lib/paths.js'
import {getServiceErrors} from '../lib/errors.js'
import {getImageSizes, getProjectVolumeSizes, getDockerDiskUsage, ImageSize, VolumeSize, DockerDiskUsage} from '../lib/resources.js'
import {ResourcesPanel} from './ResourcesPanel.js'
import {AllErrorsOverlay} from './AllErrorsOverlay.js'
import {DebugPortsOverlay} from './DebugPortsOverlay.js'
import {MultiTailPicker} from './MultiTailPicker.js'
import {MultiTailOverlay} from './MultiTailOverlay.js'

type Mode = 'dashboard' | 'select-services' | 'operation' | 'confirm-destroy' | 'confirm-full-reset' | 'logs' | 'all-errors' | 'debug-ports' | 'multi-tail-pick' | 'multi-tail'
type Panel = 'scaffold' | 'services'

const NETWORK_NAME = `${PROJECT_NAME}-bridge`

export interface PostExitAction {
  psql?: {container: string; db: string}
}

export interface DashboardProps {
  cliRoot: string
  postExit: PostExitAction
}

export function Dashboard({cliRoot: _cliRoot, postExit}: DashboardProps) {
  const {exit} = useApp()
  const [mode, setMode] = useState<Mode>('dashboard')
  const [statuses, setStatuses] = useState<Record<string, ContainerInfo>>({})
  const [networkInfo, setNetworkInfo] = useState<NetworkInfo>({exists: false})
  const [operationTitle, setOperationTitle] = useState('')
  const [operationLogs, setOperationLogs] = useState<string[]>([])
  const [operationDone, setOperationDone] = useState(false)
  const [operationError, setOperationError] = useState<string | null>(null)
  const [logsService, setLogsService] = useState<string>('')
  const [logsDevMode, setLogsDevMode] = useState(false)
  const [logsContainerName, setLogsContainerName] = useState('')
  const [logsErrorFilter, setLogsErrorFilter] = useState(false)
  const [errorLines, setErrorLines] = useState<Record<string, string[]>>({})
  const [ignoredErrors, setIgnoredErrors] = useState<Record<string, Set<string>>>({})
  const [imageSizes, setImageSizes] = useState<Record<string, ImageSize>>({})
  const [volumeSizes, setVolumeSizes] = useState<VolumeSize[] | null>(null)
  const [dockerDisk, setDockerDisk] = useState<DockerDiskUsage | null>(null)
  const [time, setTime] = useState(new Date())
  const [focusPanel, setFocusPanel] = useState<Panel>('scaffold')
  const [scaffoldCursor, setScaffoldCursor] = useState(0)
  const [servicesCursor, setServicesCursor] = useState(0)
  const [pendingRestore, setPendingRestore] = useState<Array<{name: string; dev: boolean}> | null>(null)
  const [multiTailServices, setMultiTailServices] = useState<Array<{name: string; containerName: string; devMode: boolean}>>([])
  const operationInProgress = useRef(false)

  // Service lists don't change while app is running — compute once
  const appServiceNames = useMemo(() => getAppServiceNames(), [])
  const scaffoldServiceNames = useMemo(() => getScaffoldServiceNames(), [])

  useEffect(() => {
    let cancelled = false
    const pollContainers = async () => {
      if (cancelled) return
      const s = await getContainerStatuses()
      if (!cancelled) setStatuses(prev => {
        const prevJson = JSON.stringify(prev)
        const nextJson = JSON.stringify(s)
        return prevJson === nextJson ? prev : s
      })
    }
    const pollNetwork = async () => {
      if (cancelled) return
      const n = await getNetworkInfo(NETWORK_NAME)
      if (!cancelled) setNetworkInfo(prev => {
        const prevJson = JSON.stringify(prev)
        const nextJson = JSON.stringify(n)
        return prevJson === nextJson ? prev : n
      })
    }
    pollContainers()
    pollNetwork()
    const containerInterval = setInterval(pollContainers, 3000)
    const networkInterval = setInterval(pollNetwork, 5000)
    const clock = setInterval(() => setTime(new Date()), 60000)
    return () => {
      cancelled = true
      clearInterval(containerInterval)
      clearInterval(networkInterval)
      clearInterval(clock)
    }
  }, [])

  const statusesRef = useRef(statuses)
  useEffect(() => { statusesRef.current = statuses }, [statuses])

  const errorPollRef = useRef<(() => void) | null>(null)
  const imagePollRef = useRef<(() => void) | null>(null)
  const initialPollFired = useRef(false)

  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      if (cancelled) return
      const running = Object.entries(statusesRef.current).filter(([, i]) => i.status === 'running')
      const raw: Record<string, string[]> = {}
      await Promise.all(running.map(async ([name, info]) => {
        raw[name] = await getServiceErrors(info.containerName)
      }))
      // Sort keys for stable JSON comparison (Promise.all resolves in non-deterministic order)
      const results: Record<string, string[]> = Object.fromEntries(
        Object.keys(raw).sort().map(k => [k, raw[k]])
      )
      if (!cancelled) setErrorLines(prev => {
        const same = JSON.stringify(prev) === JSON.stringify(results)
        return same ? prev : results
      })
    }
    errorPollRef.current = poll
    poll()
    const interval = setInterval(poll, 30000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

  // Image sizes — fast, poll every 30s
  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      if (cancelled) return
      const images = [...new Set(Object.values(statusesRef.current).map(i => i.image).filter(Boolean))]
      if (!images.length) return
      const sizes = await getImageSizes(images)
      if (!cancelled && Object.keys(sizes).length > 0) {
        setImageSizes(prev => JSON.stringify(prev) === JSON.stringify(sizes) ? prev : sizes)
      }
    }
    imagePollRef.current = poll
    poll()
    const interval = setInterval(poll, 30_000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

  // Retrigger image+error polls once statuses first load (they fire before statuses are ready on mount)
  useEffect(() => {
    if (!initialPollFired.current && Object.keys(statuses).length > 0) {
      initialPollFired.current = true
      imagePollRef.current?.()
      errorPollRef.current?.()
    }
  }, [statuses])

  // Volume sizes — slow (disk scan), poll every 120s
  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      if (cancelled) return
      const vols = await getProjectVolumeSizes()
      if (!cancelled) setVolumeSizes(prev => JSON.stringify(prev) === JSON.stringify(vols) ? prev : vols)
    }
    poll()
    const interval = setInterval(poll, 120_000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

  // Docker disk usage — fast summary (no -v), poll every 60s
  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      if (cancelled) return
      const usage = await getDockerDiskUsage()
      if (!cancelled) setDockerDisk(prev => JSON.stringify(prev) === JSON.stringify(usage) ? prev : usage)
    }
    poll()
    const interval = setInterval(poll, 60_000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [])

  const visibleErrorLines = useMemo(() => {
    const result: Record<string, string[]> = {}
    for (const [svc, lines] of Object.entries(errorLines)) {
      const visible = lines.filter(line => !ignoredErrors[svc]?.has(line))
      if (visible.length > 0) result[svc] = visible
    }
    return result
  }, [errorLines, ignoredErrors])

  const errorServices = useMemo(
    () => new Set(Object.keys(visibleErrorLines)),
    [visibleErrorLines],
  )

  const clearServiceErrors = useCallback((svc: string) => {
    setIgnoredErrors(prev => ({...prev, [svc]: new Set(errorLines[svc] ?? [])}))
  }, [errorLines])

  const runOperation = useCallback(async (title: string, fn: (log: (l: string) => void) => Promise<void>) => {
    if (operationInProgress.current) return
    operationInProgress.current = true
    setOperationTitle(title)
    setOperationLogs([])
    setOperationDone(false)
    setOperationError(null)
    setMode('operation')
    try {
      await fn(line => setOperationLogs(prev => [...prev, line]))
      setOperationDone(true)
    } catch (err) {
      setOperationError(err instanceof Error ? err.message : String(err))
      setOperationDone(true)
    }
    const s = await getContainerStatuses()
    statusesRef.current = s  // sync ref before polls so they see fresh state
    setStatuses(s)
    operationInProgress.current = false
    // Immediately refresh error+image state so badges/sizes reflect new container state
    errorPollRef.current?.()
    imagePollRef.current?.()
  }, [])

  useInput((input, key) => {
    if (mode === 'operation' && operationDone) {
      setMode('dashboard')
      return
    }
    if (mode !== 'dashboard') return

    // navigation
    const svcs = appServiceNames
    const services = svcs
    const scaffoldSvcs = scaffoldServiceNames
    const colSize = Math.ceil(svcs.length / 2)
    if (key.upArrow) {
      if (focusPanel === 'scaffold') setScaffoldCursor(c => Math.max(0, c - 1))
      else setServicesCursor(c => Math.max(0, c - 1))
      return
    }
    if (key.downArrow) {
      if (focusPanel === 'scaffold') setScaffoldCursor(c => Math.min(scaffoldSvcs.length - 1, c + 1))
      else setServicesCursor(c => Math.min(svcs.length - 1, c + 1))
      return
    }
    if (key.leftArrow) {
      if (focusPanel === 'scaffold') return
      if (servicesCursor >= colSize) {
        // col1 → col0, same row
        setServicesCursor(c => c - colSize)
      } else {
        setFocusPanel('scaffold')
      }
      return
    }
    if (key.rightArrow) {
      if (focusPanel === 'scaffold') { setFocusPanel('services'); return }
      if (servicesCursor < colSize) {
        // col0 → col1, same row (clamp to last item)
        setServicesCursor(c => Math.min(c + colSize, svcs.length - 1))
      }
      return
    }

    if (focusPanel === 'scaffold') {
      const svc = scaffoldSvcs[scaffoldCursor]
      const info = statuses[svc]
      const running = info?.status === 'running'
      if (input === 'l' && svc && running) {
        setLogsService(svc); setLogsDevMode(info?.devMode ?? false); setLogsContainerName(info?.containerName ?? ''); setLogsErrorFilter(false); setMode('logs')
      }
      if (input === 'e' && svc && running && errorServices.has(svc)) {
        setLogsService(svc); setLogsDevMode(info?.devMode ?? false); setLogsContainerName(info?.containerName ?? ''); setLogsErrorFilter(true); setMode('logs')
      }
    } else {
      const svc = services[servicesCursor]
      const info = statuses[svc]
      const running = info?.status === 'running'
      const dev = info?.devMode ?? false
      if (input === 'l' && svc && running) { setLogsService(svc); setLogsDevMode(dev); setLogsContainerName(info?.containerName ?? ''); setLogsErrorFilter(false); setMode('logs'); return }
      if (input === 'e' && svc && running && errorServices.has(svc)) { setLogsService(svc); setLogsDevMode(dev); setLogsContainerName(info?.containerName ?? ''); setLogsErrorFilter(true); setMode('logs'); return }
      if (input === 'u' && svc && !running) { runOperation(`Starting ${svc} [dev]...`, log => startService(svc, true, log)); return }
      if (input === 'U' && svc && !running) { runOperation(`Starting ${svc}...`, log => startService(svc, false, log)); return }
      if (input === 'k' && svc && running) { runOperation(`Stopping ${svc}...`, log => stopService(svc, log)); return }
      if (input === 'r' && svc && running) { runOperation(`Restarting ${svc}...`, log => restartService(svc, dev, log)); return }
    }

    // global error view
    if (input === 'E' && errorServices.size > 0) { setMode('all-errors'); return }

    // psql — scaffold panel, db or product service, running
    if (input === 'p' && focusPanel === 'scaffold') {
      const svc = scaffoldSvcs[scaffoldCursor]
      if ((svc === 'db' || svc === 'product') && statuses[svc]?.status === 'running') {
        postExit.psql = {
          container: statuses[svc].containerName,
          db: svc === 'db' ? 'crowd-web' : 'product-db',
        }
        exit()
        return
      }
    }

    // global scaffold ops
    if (input === 'L') runOperation('Running lint...', lintAll)
    if (input === 'U') runOperation('Starting infrastructure...', scaffoldUp)
    if (input === 'd') runOperation('Stopping infrastructure...', scaffoldDown)
    if (input === 'D') setMode('confirm-destroy')
    if (input === 'R') runOperation('Resetting infrastructure...', scaffoldReset)
    if (input === 'F') {
      const appNames = new Set(appServiceNames)
      const running = Object.values(statuses)
        .filter(info => info.status === 'running' && appNames.has(info.name))
        .map(info => ({name: info.name, dev: info.devMode}))
      setPendingRestore(running)
      setMode('confirm-full-reset')
    }
    if (input === 's') setMode('select-services')
    if (input === 'P') setMode('debug-ports')
    if (input === 'm') {
      const appNames = new Set(appServiceNames)
      const running = Object.values(statuses)
        .filter(info => info.status === 'running' && appNames.has(info.name))
        .map(info => ({name: info.name, containerName: info.containerName, devMode: info.devMode}))
      if (running.length > 0) { setMultiTailServices(running); setMode('multi-tail-pick') }
      return
    }
    if (input === 'q') exit()
  })

  if (mode === 'operation') {
    return (
      <OperationOverlay
        title={operationTitle}
        logs={operationLogs}
        done={operationDone}
        error={operationError}
      />
    )
  }

  if (mode === 'logs') {
    return (
      <LogsOverlay
        service={logsService}
        devMode={logsDevMode}
        containerName={logsContainerName}
        errorFilter={logsErrorFilter}
        onClear={logsErrorFilter ? () => clearServiceErrors(logsService) : undefined}
        onExit={() => { setLogsErrorFilter(false); setMode('dashboard') }}
      />
    )
  }

  if (mode === 'all-errors') {
    return (
      <AllErrorsOverlay
        errorLines={visibleErrorLines}
        onClearService={clearServiceErrors}
        onExit={() => setMode('dashboard')}
      />
    )
  }

  if (mode === 'confirm-destroy') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <ConfirmDestroy
          onConfirm={() => runOperation('Destroying infrastructure (deleting volumes)...', scaffoldDestroy)}
          onCancel={() => setMode('dashboard')}
        />
      </Box>
    )
  }

  if (mode === 'confirm-full-reset') {
    const restore = pendingRestore ?? []
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <ConfirmFullReset
          services={restore}
          onConfirm={() => runOperation(
            `Full reset — will restore ${restore.length} service(s)...`,
            async (log) => {
              log('Running scaffold reset...')
              await scaffoldReset(log)
              log('Scaffold reset complete.')
              const total = restore.length
              for (let i = 0; i < total; i++) {
                const {name, dev} = restore[i]
                log(`[${i + 1}/${total}] starting ${name}${dev ? ' [dev]' : ''}...`)
                await startService(name, dev, log)
                log(`[${i + 1}/${total}] ✓ ${name}`)
              }
            }
          )}
          onCancel={() => setMode('dashboard')}
        />
      </Box>
    )
  }

  if (mode === 'debug-ports') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <DebugPortsOverlay statuses={statuses} onExit={() => setMode('dashboard')} />
      </Box>
    )
  }

  if (mode === 'multi-tail-pick') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <MultiTailPicker
          runningServices={multiTailServices}
          onConfirm={svcs => { setMultiTailServices(svcs); setMode('multi-tail') }}
          onCancel={() => setMode('dashboard')}
        />
      </Box>
    )
  }

  if (mode === 'multi-tail') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <MultiTailOverlay
          services={multiTailServices}
          onExit={() => setMode('dashboard')}
        />
      </Box>
    )
  }

  if (mode === 'select-services') {
    return (
      <Box height={process.stdout.rows || 24} flexDirection="column">
        <ServiceSelector
          onConfirm={async (services, dev) => {
            const total = services.length
            await runOperation(
              `Starting ${total} service(s)${dev ? ' [dev]' : ''}...`,
              async (log) => {
                for (let i = 0; i < services.length; i++) {
                  const svc = services[i]
                  log(`[${i + 1}/${total}] starting ${svc}...`)
                  await startService(svc, dev, log)
                  log(`[${i + 1}/${total}] ✓ ${svc}`)
                }
              }
            )
          }}
          onCancel={() => setMode('dashboard')}
        />
      </Box>
    )
  }

  return (
    <Box flexDirection="column" width={process.stdout.columns || 120} height={process.stdout.rows || 24}>
      <Box borderStyle="round" borderColor="cyan" paddingX={1}>
        <Box flexGrow={1}>
          <Text color="cyan" bold>crowd</Text>
          <Text color="gray">  Community Data Platform</Text>
        </Box>
        <Text color="gray">
          {time.toLocaleDateString('en-US', {weekday: 'short', month: 'short', day: 'numeric'})}
          {'  '}
          {time.toLocaleTimeString('en-US', {hour12: false, hour: '2-digit', minute: '2-digit'})}
        </Text>
      </Box>

      {errorServices.size > 0 && (
        <Box marginTop={1} paddingX={1} gap={2} borderStyle="single" borderColor="red" flexWrap="wrap">
          <Text color="red" bold inverse> !! ERRORS </Text>
          <Box flexWrap="wrap" gap={1}>
            {[...errorServices].map(svc => (
              <Text key={svc} color="red" bold>{svc}</Text>
            ))}
          </Box>
          <Text color="gray" dimColor>[e] service errors  [E] all errors</Text>
        </Box>
      )}

      <Box flexDirection="row" gap={2} marginTop={1}>
        <Box flexDirection="column">
          <ScaffoldPanel statuses={statuses} focused={focusPanel === 'scaffold'} cursor={scaffoldCursor} errorServices={errorServices} />
        </Box>
        <Box flexDirection="column" flexGrow={1}>
          <ServicesPanel statuses={statuses} focused={focusPanel === 'services'} cursor={servicesCursor} errorServices={errorServices} />
        </Box>
      </Box>

      <NetworkPanel name={NETWORK_NAME} info={networkInfo} />
      <ResourcesPanel statuses={statuses} imageSizes={imageSizes} volumeSizes={volumeSizes} dockerDisk={dockerDisk} />

      <Box flexGrow={1} />

      <Box>
        <ActionBar
          focusPanel={focusPanel}
          selectedRunning={
            focusPanel === 'scaffold'
              ? statuses[scaffoldServiceNames[scaffoldCursor]]?.status === 'running'
              : statuses[appServiceNames[servicesCursor]]?.status === 'running'
          }
          selectedHasErrors={
            focusPanel === 'scaffold'
              ? errorServices.has(scaffoldServiceNames[scaffoldCursor] ?? '')
              : errorServices.has(appServiceNames[servicesCursor] ?? '')
          }
          anyErrors={errorServices.size > 0}
          anyRunning={appServiceNames.some(n => statuses[n]?.status === 'running')}
          selectedCanPsql={
            focusPanel === 'scaffold' &&
            (scaffoldServiceNames[scaffoldCursor] === 'db' || scaffoldServiceNames[scaffoldCursor] === 'product') &&
            statuses[scaffoldServiceNames[scaffoldCursor]]?.status === 'running'
          }
        />
      </Box>
    </Box>
  )
}

function ConfirmButtons({
  confirmLabel,
  confirmColor,
  onConfirm,
  onCancel,
}: {
  confirmLabel: string
  confirmColor: 'yellow' | 'red'
  onConfirm: () => void
  onCancel: () => void
}) {
  const [focused, setFocused] = useState<'confirm' | 'cancel'>('cancel')

  useInput((input, key) => {
    if (key.leftArrow || key.rightArrow) {
      setFocused(f => f === 'confirm' ? 'cancel' : 'confirm')
      return
    }
    if (key.return || input === ' ') {
      focused === 'confirm' ? onConfirm() : onCancel()
      return
    }
    if (input === 'y' || input === 'Y') { onConfirm(); return }
    if (input === 'n' || input === 'N') { onCancel(); return }
  })

  return (
    <Box marginTop={1} gap={3}>
      <Text
        color={confirmColor}
        bold={focused === 'confirm'}
        inverse={focused === 'confirm'}
      >
        {' '}[Y] {confirmLabel}{' '}
      </Text>
      <Text
        color="gray"
        bold={focused === 'cancel'}
        inverse={focused === 'cancel'}
      >
        {' '}[N] Cancel{' '}
      </Text>
    </Box>
  )
}

function ConfirmFullReset({services, onConfirm, onCancel}: {services: Array<{name: string; dev: boolean}>; onConfirm: () => void; onCancel: () => void}) {
  return (
    <Box flexDirection="column" borderStyle="round" borderColor="yellow" paddingX={2} paddingY={1}>
      <Text color="yellow" bold>⚠  Full reset + restore?</Text>
      <Text color="gray">Scaffold will be reset (volumes wiped). Then these services will restart:</Text>
      <Box flexDirection="column" marginTop={1} marginLeft={2}>
        {services.length === 0
          ? <Text color="gray">(no running services — only scaffold reset will run)</Text>
          : services.map(s => (
              <Text key={s.name} color="gray">• {s.name}{s.dev ? <Text color="yellow"> [dev]</Text> : ''}</Text>
            ))
        }
      </Box>
      <ConfirmButtons confirmLabel="Yes, reset + restore" confirmColor="yellow" onConfirm={onConfirm} onCancel={onCancel} />
    </Box>
  )
}

function ConfirmDestroy({onConfirm, onCancel}: {onConfirm: () => void; onCancel: () => void}) {
  return (
    <Box flexDirection="column" borderStyle="round" borderColor="red" paddingX={2} paddingY={1}>
      <Text color="red" bold>⚠  Destroy infrastructure?</Text>
      <Text color="gray">This will permanently delete all Docker volumes.</Text>
      <ConfirmButtons confirmLabel="Yes, destroy" confirmColor="red" onConfirm={onConfirm} onCancel={onCancel} />
    </Box>
  )
}
