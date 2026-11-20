import type { IStoredSessionizeEventMapping } from '@crowd/data-access-layer/src/sessionize'
import {
  SlackChannel,
  SlackPersona,
  escapeSlackText,
  sendSlackNotificationAsync,
} from '@crowd/slack'
import type { SlackMessageSection } from '@crowd/slack'

export const HELD_REPORT_TOP = 10

export interface SessionizeHeldReport {
  changed: boolean
  title: string
  sections: SlackMessageSection[]
}

function activeHeld(rows: IStoredSessionizeEventMapping[]): IStoredSessionizeEventMapping[] {
  return rows.filter((row) => row.status === 'held' && row.deactivatedAt === null)
}

function formatHeld(row: IStoredSessionizeEventMapping): string {
  return `• ${escapeSlackText(row.eventName)} — ${row.eventStart}, \`${row.holdReason}\`, key \`${row.eventKey}\``
}

function capped(rows: IStoredSessionizeEventMapping[]): string {
  const lines = rows.slice(0, HELD_REPORT_TOP).map(formatHeld)
  const rest = rows.length - lines.length
  return rest > 0 ? `${lines.join('\n')}\n… and ${rest} more` : lines.join('\n')
}

function upcomingFirst(
  rows: IStoredSessionizeEventMapping[],
  today: string,
): IStoredSessionizeEventMapping[] {
  const byStart = [...rows].sort((a, b) => a.eventStart.localeCompare(b.eventStart))
  return [
    ...byStart.filter((row) => row.eventStart >= today),
    ...byStart.filter((row) => row.eventStart < today),
  ]
}

export function buildSessionizeHeldReport(
  before: IStoredSessionizeEventMapping[],
  after: IStoredSessionizeEventMapping[],
  today: string = new Date().toISOString().slice(0, 10),
): SessionizeHeldReport {
  const heldBefore = activeHeld(before)
  const heldAfter = upcomingFirst(activeHeld(after), today)
  const beforeKeys = new Set(heldBefore.map((row) => row.eventKey))
  const afterKeys = new Set(heldAfter.map((row) => row.eventKey))

  const deactivatedKeys = new Set(
    after.filter((row) => row.deactivatedAt !== null).map((row) => row.eventKey),
  )

  const newlyHeld = heldAfter.filter((row) => !beforeKeys.has(row.eventKey))
  const leftHeld = upcomingFirst(
    heldBefore.filter((row) => !afterKeys.has(row.eventKey)),
    today,
  )
  const released = leftHeld.filter((row) => !deactivatedKeys.has(row.eventKey))
  const removed = leftHeld.filter((row) => deactivatedKeys.has(row.eventKey))

  const sections: SlackMessageSection[] = []
  if (newlyHeld.length > 0) {
    sections.push({ title: `Newly held (${newlyHeld.length})`, text: capped(newlyHeld) })
  }
  if (released.length > 0) {
    sections.push({ title: `Released (${released.length})`, text: capped(released) })
  }
  if (removed.length > 0) {
    sections.push({
      title: `Removed from Sessionize (${removed.length})`,
      text: capped(removed),
    })
  }
  if (heldAfter.length > 0) {
    sections.push({
      title: `Top ${Math.min(HELD_REPORT_TOP, heldAfter.length)} held`,
      text: heldAfter.slice(0, HELD_REPORT_TOP).map(formatHeld).join('\n'),
    })
  }

  const changed = newlyHeld.length > 0 || released.length > 0 || removed.length > 0
  return {
    changed,
    title: changed
      ? `Sessionize held events: ${newlyHeld.length} new, ${released.length} released, ${removed.length} removed`
      : `Sessionize held events: ${heldAfter.length} waiting for review`,
    sections,
  }
}

export function sendSessionizeHeldReport(report: SessionizeHeldReport): Promise<boolean> {
  return sendSlackNotificationAsync(
    SlackChannel.CDP_INTEGRATIONS_ALERTS,
    SlackPersona.SUMMARY_REPORTER,
    report.title,
    report.sections,
  )
}
