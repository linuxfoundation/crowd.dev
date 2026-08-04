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
  sendSlackNotification(
    SlackChannel.CDP_AKRITES_ALERTS,
    SlackPersona.CRITICAL_ALERTER,
    `:warning: ${input.jobKind} BQ byte ceiling exceeded — soft-failed`,
    [
      {
        title: 'Action',
        text: `Ingest skipped for this run so scorecard/ranking still complete. Existing data untouched. ${input.message}`,
      },
    ],
  )
}
