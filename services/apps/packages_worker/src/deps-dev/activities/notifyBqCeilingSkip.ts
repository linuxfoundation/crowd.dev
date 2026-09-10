import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

export interface NotifyBqCeilingSkipInput {
  jobKind: string
  message: string
}

// The advisories ceiling breach is caught one level up in bootstrapOsspckgs instead of inside a
// guard activity, so it needs its own alert call to keep repeated skips from going unnoticed
// (CM-1362 review). Uses CDP_AKRITES_ALERTS — the packages_worker team's own channel (matches
// service.ts / blast-radius-worker.ts), per reviewer request — rather than CDP_CRITICAL_ALERTS.
export async function notifyBqCeilingSkip(input: NotifyBqCeilingSkipInput): Promise<void> {
  // ingestAdvisories merges `advisories` to Postgres before exporting `advisory_packages`
  // (bootstrapOsspckgs.ts), so a breach on the latter still leaves this run's new advisory rows
  // committed with no package links — "existing data untouched" would misstate that (bugbot
  // review on CM-1362). A breach on `advisories` itself precedes any write this run, so that
  // claim stays accurate there.
  const impact =
    input.jobKind === 'advisory_packages'
      ? "This run's advisories were already merged without package links — they'll link up on the next successful run."
      : 'Ingest skipped for this run so scorecard/ranking still complete. Existing data untouched.'
  sendSlackNotification(
    SlackChannel.CDP_AKRITES_ALERTS,
    SlackPersona.CRITICAL_ALERTER,
    `:warning: ${input.jobKind} BQ byte ceiling exceeded — soft-failed`,
    [
      {
        title: 'Action',
        text: `${impact} ${input.message}`,
      },
    ],
  )
}
