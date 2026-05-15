export const theme = {
  running: 'green',
  stopped: 'gray',
  exited: 'gray',
  restarting: 'yellow',
  unknown: 'gray',
  border: 'gray',
  title: 'cyan',
  accent: 'yellow',
  dim: 'gray',
} as const

export function statusLabel(status: string): string {
  if (status === 'unknown' || status === 'exited') return 'stopped'
  return status
}
