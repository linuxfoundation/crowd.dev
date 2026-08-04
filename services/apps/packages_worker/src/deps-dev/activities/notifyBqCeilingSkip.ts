import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

export interface NotifyBqCeilingSkipInput {
  jobKind: string
  message: string
}

// Sibling guards (checkDependentCountsGuard, checkEdgeSnapshotQuality) alert on
// CDP_CRITICAL_ALERTS before their own soft-fail. The advisories ceiling breach is caught one
// level up in bootstrapOsspckgs instead of inside a guard activity, so it needs its own alert
// call to keep repeated skips from going unnoticed (CM-1362 review).
export async function notifyBqCeilingSkip(input: NotifyBqCeilingSkipInput): Promise<void> {
  sendSlackNotification(
    SlackChannel.CDP_CRITICAL_ALERTS,
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
