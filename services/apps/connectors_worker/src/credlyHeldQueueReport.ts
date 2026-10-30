import { groupBy } from '@crowd/common'
import type {
  ICredlyHeldTemplate,
  ICredlyStoredTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import {
  SlackChannel,
  SlackPersona,
  escapeSlackText,
  sendSlackNotificationAsync,
} from '@crowd/slack'
import type { SlackMessageSection } from '@crowd/slack'

import type { ResolvePlan } from './credlyTemplateResolve'

export const HELD_QUEUE_TOP = 10

export interface HeldQueueReport {
  changed: boolean
  title: string
  sections: SlackMessageSection[]
}

export function activeHeld(stored: ICredlyStoredTemplateMapping[]): ICredlyStoredTemplateMapping[] {
  return stored.filter((row) => row.status === 'held' && row.deactivatedAt === null)
}

function formatHeld(row: ICredlyHeldTemplate, urls: Map<string, string | null>): string {
  const name = escapeSlackText(row.badgeTemplateName)
  const url = urls.get(row.badgeTemplateId)
  const link = url ? `<${url}|${name}>` : name
  const slug = row.projectSlug ? `, ${escapeSlackText(row.projectSlug)}` : ''
  const badges = (row.badgeCount ?? 0).toLocaleString('en-US')
  return `• ${link} — ${badges} badges, \`${row.holdReason}\`${slug}, id \`${row.badgeTemplateId}\``
}

function capped(lines: string[], total: number): string {
  const rest = total - lines.length
  return rest > 0 ? `${lines.join('\n')}\n… and ${rest} more` : lines.join('\n')
}

function byBadges(rows: ICredlyHeldTemplate[]): ICredlyHeldTemplate[] {
  return [...rows].sort((a, b) => (b.badgeCount ?? 0) - (a.badgeCount ?? 0))
}

// Membership is compared between the active held rows before the apply and the stored queue
// after it: labels cannot tell a reactivated row from one that was held all along.
export function buildHeldQueueReport(
  heldBefore: ICredlyStoredTemplateMapping[],
  heldQueue: ICredlyHeldTemplate[],
  plan: ResolvePlan,
  templateUrls: Map<string, string | null>,
): HeldQueueReport {
  const beforeIds = new Set(heldBefore.map((row) => row.badgeTemplateId))
  const afterById = new Map(heldQueue.map((row) => [row.badgeTemplateId, row]))
  const afterLabel = new Map(plan.entries.map((entry) => [entry.badgeTemplateId, entry.after]))

  const newlyHeld = byBadges(heldQueue.filter((row) => !beforeIds.has(row.badgeTemplateId)))
  const released = heldBefore.filter((row) => !afterById.has(row.badgeTemplateId))
  const held = byBadges(heldQueue)
  const badges = held.reduce((sum, row) => sum + (row.badgeCount ?? 0), 0)
  const byReason = [...(groupBy(held, (row) => row.holdReason) as Map<string, unknown[]>)]
    .map(([reason, rows]) => [reason, rows.length] as const)
    .sort(([, a], [, b]) => b - a)

  const sections: SlackMessageSection[] = [
    {
      title: 'Held queue',
      text: [
        `${held.length.toLocaleString('en-US')} templates, ${badges.toLocaleString('en-US')} badges not ingested.`,
        byReason.map(([reason, count]) => `\`${reason}\` ${count}`).join(' · '),
      ]
        .filter(Boolean)
        .join('\n'),
    },
  ]
  if (newlyHeld.length > 0) {
    sections.push({
      title: `Newly held (${newlyHeld.length})`,
      text: capped(
        newlyHeld.slice(0, HELD_QUEUE_TOP).map((row) => formatHeld(row, templateUrls)),
        newlyHeld.length,
      ),
    })
  }
  if (released.length > 0) {
    sections.push({
      title: `Released (${released.length})`,
      text: capped(
        released.slice(0, HELD_QUEUE_TOP).map((row) => {
          const after = afterLabel.get(row.badgeTemplateId) ?? 'gone from Credly'
          return `• ${escapeSlackText(row.badgeTemplateName)} → ${escapeSlackText(after)}`
        }),
        released.length,
      ),
    })
  }
  if (held.length > 0) {
    sections.push({
      title: `Top ${Math.min(HELD_QUEUE_TOP, held.length)} by badges`,
      text: held
        .slice(0, HELD_QUEUE_TOP)
        .map((row) => formatHeld(row, templateUrls))
        .join('\n'),
    })
    sections.push({
      title: 'To resolve one',
      text: 'Reply in this thread with the template id and the project slug, or `excluded` for test badges. The CDP team records the decision as an override; nothing reads this thread automatically.',
    })
  }

  const changed = newlyHeld.length > 0 || released.length > 0
  return {
    changed,
    title: changed
      ? `Credly held templates: ${newlyHeld.length} new, ${released.length} released`
      : `Credly held templates: ${held.length} waiting for review`,
    sections,
  }
}

export function postHeldQueueReport(report: HeldQueueReport): Promise<boolean> {
  return sendSlackNotificationAsync(
    SlackChannel.CDP_INTEGRATIONS_ALERTS,
    SlackPersona.SUMMARY_REPORTER,
    report.title,
    report.sections,
  )
}
