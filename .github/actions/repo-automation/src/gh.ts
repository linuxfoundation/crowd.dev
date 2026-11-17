import { execFileSync, spawnSync } from 'node:child_process'

/** Runs a command, returns trimmed stdout, throws on a non-zero exit. */
export function run(cmd: string, args: string[], cwd?: string): string {
  return execFileSync(cmd, args, {
    encoding: 'utf8',
    cwd,
    maxBuffer: 64 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'inherit'],
  }).trim()
}

/** Runs `gh` (authenticated through GH_TOKEN in the environment) and parses its JSON output. */
export function ghJson<T>(args: string[]): T {
  return JSON.parse(run('gh', args)) as T
}

/** Runs a command and returns trimmed stdout whatever the exit code (for `gh pr checks`, which exits non-zero while checks fail or pend). */
export function runCaptureStdout(cmd: string, args: string[]): string {
  const r = spawnSync(cmd, args, {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  if (r.error) throw r.error
  return (r.stdout ?? '').trim()
}
