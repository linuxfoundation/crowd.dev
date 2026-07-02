import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('installation-pool')

// Park an installation before GitHub starts rejecting — avoids a failed request + requeue
const PROACTIVE_PARK_REMAINING = 50

/** Round-robins over installations, skipping ones parked until their rate-limit reset. */
export class InstallationPool {
  private readonly parkedUntil = new Map<number, number>()
  private roundRobinIdx = 0

  constructor(private readonly ids: number[]) {}

  select(): { installationId: number; waitMs: number } {
    const now = Date.now()
    const n = this.ids.length

    for (let i = 0; i < n; i++) {
      const idx = (this.roundRobinIdx + i) % n
      const id = this.ids[idx]
      if ((this.parkedUntil.get(id) ?? 0) <= now) {
        this.roundRobinIdx = (idx + 1) % n
        return { installationId: id, waitMs: 0 }
      }
    }

    let soonestReset = Infinity
    let soonestId = this.ids[0]
    for (const id of this.ids) {
      const reset = this.parkedUntil.get(id) ?? 0
      if (reset < soonestReset) {
        soonestReset = reset
        soonestId = id
      }
    }
    return { installationId: soonestId, waitMs: Math.max(1_000, soonestReset - now) }
  }

  park(installationId: number, untilMs: number): void {
    this.parkedUntil.set(installationId, untilMs)
  }

  parkIfBudgetLow(
    installationId: number,
    remaining: number | null | undefined,
    resetAt: string | null | undefined,
  ): void {
    if (remaining == null || resetAt == null || remaining >= PROACTIVE_PARK_REMAINING) return
    this.park(installationId, new Date(resetAt).getTime() + 5_000)
    log.info(
      { installationId, remaining, resetAt },
      'Budget low — proactively parking installation',
    )
  }
}
