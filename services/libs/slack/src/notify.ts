import { SERVICE } from '@crowd/common'
import { getServiceLogger } from '@crowd/logging'

import { getWebhookClient } from './client'
import { getPersonaConfig } from './personas'
import { SlackChannel, SlackMessageSection, SlackPersona } from './types'

const log = getServiceLogger()

// Slack message size limit (keeping it conservative at 30KB)
const MAX_MESSAGE_SIZE = 30 * 1024 // 30KB in bytes
const MAX_BLOCKS = 50
const MAX_SECTION_TEXT = 2900 // Slack hard limit is 3000 chars per section block

interface SlackBlock {
  type: string
  text?: {
    type: string
    text: string
    emoji?: boolean
  }
  elements?: Array<{
    type: string
    text: string
  }>
}

/**
 * Split text into section blocks, respecting Slack's 3000 char per-section limit.
 * Splits at line boundaries where possible; hard-truncates individual lines that
 * are themselves longer than the limit.
 */
function splitIntoSectionBlocks(text: string): SlackBlock[] {
  const blocks: SlackBlock[] = []
  const lines = text.split('\n')
  let current = ''

  for (let line of lines) {
    // Hard-truncate a single line that exceeds the limit on its own
    if (line.length > MAX_SECTION_TEXT) {
      line = `${line.slice(0, MAX_SECTION_TEXT - 1)}…`
    }

    const candidate = current ? `${current}\n${line}` : line
    if (candidate.length > MAX_SECTION_TEXT && current) {
      blocks.push({ type: 'section', text: { type: 'mrkdwn', text: current } })
      current = line
    } else {
      current = candidate
    }
  }

  if (current) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: current } })
  }

  return blocks
}

/**
 * Build content blocks from either a simple string or an array of sections.
 * Splits text that exceeds Slack's 3000 char per-section limit at line boundaries.
 */
function buildContentBlocks(content: string | SlackMessageSection[]): SlackBlock[] {
  if (typeof content === 'string') {
    return splitIntoSectionBlocks(content)
  }

  const blocks: SlackBlock[] = []
  for (const section of content) {
    const fullText = `*${section.title}*\n${section.text}`
    for (const block of splitIntoSectionBlocks(fullText)) {
      blocks.push(block)
    }
  }
  return blocks
}

/**
 * Prune blocks if the message is too large for Slack
 */
function pruneBlocksIfNeeded(blocks: SlackBlock[]): SlackBlock[] {
  // Check current size
  const currentSize = JSON.stringify({ blocks }).length

  if (currentSize <= MAX_MESSAGE_SIZE && blocks.length <= MAX_BLOCKS) {
    return blocks // No pruning needed
  }

  log.warn(
    { currentSize, maxSize: MAX_MESSAGE_SIZE, blockCount: blocks.length },
    'Slack message too large, pruning sections',
  )

  // Keep header (first block) and context footer (last block)
  // Prune content blocks from the middle
  const header = blocks[0]
  const footer = blocks[blocks.length - 1]
  const contentBlocks = blocks.slice(1, -1)

  // Warning block to add
  const warningBlock: SlackBlock = {
    type: 'section',
    text: {
      type: 'mrkdwn',
      text: '⚠️ _Message was too large and has been pruned. Some sections were removed._',
    },
  }

  // Keep removing blocks from the end until it fits
  while (contentBlocks.length > 0) {
    const testBlocks = [header, ...contentBlocks, warningBlock, footer]
    const testSize = JSON.stringify({ blocks: testBlocks }).length

    if (testSize <= MAX_MESSAGE_SIZE && testBlocks.length <= MAX_BLOCKS) {
      log.info(
        {
          originalBlocks: blocks.length,
          prunedBlocks: testBlocks.length,
          removedBlocks: blocks.length - testBlocks.length,
        },
        'Successfully pruned Slack message to fit size limits',
      )
      return testBlocks
    }

    // Remove one block from the end of content
    contentBlocks.pop()
  }

  // If even with all content removed it's still too large,
  // just return header, warning, and footer
  log.warn('Had to remove all content blocks to fit message')
  return [header, warningBlock, footer]
}

/**
 * Send a Slack notification and return a Promise.
 * Use this version in jobs or when you need to await the notification.
 *
 * @param channel - The Slack channel to send the notification to
 * @param persona - The persona/type of the notification
 * @param title - The title of the notification
 * @param content - The markdown-formatted content (string) or an array of sections
 * @returns Promise that resolves when the message is sent
 */
export async function sendSlackNotificationAsync(
  channel: SlackChannel,
  persona: SlackPersona,
  title: string,
  content: string | SlackMessageSection[],
): Promise<void> {
  try {
    const client = getWebhookClient(channel)

    if (!client) {
      log.warn(
        { channel, persona, title },
        `Skipping Slack notification - webhook client not available for channel ${channel}`,
      )
      return
    }

    const personaConfig = getPersonaConfig(persona)

    // Format message using Slack Block Kit
    let blocks = [
      {
        type: 'header',
        text: {
          type: 'plain_text',
          text: `${personaConfig.icon} ${title}`,
          emoji: true,
        },
      },
      ...buildContentBlocks(content),
      {
        type: 'context',
        elements: [
          {
            type: 'mrkdwn',
            text: `*Service:* ${SERVICE} | *Persona:* ${personaConfig.name}`,
          },
        ],
      },
    ]

    // Prune blocks if message is too large
    blocks = pruneBlocksIfNeeded(blocks)

    await client.send({
      blocks,
    })

    log.debug(
      { channel, persona, title, service: SERVICE },
      `Successfully sent Slack notification to channel ${channel}`,
    )
  } catch (error) {
    log.error(
      { error, channel, persona, title, service: SERVICE },
      `Failed to send Slack notification to channel ${channel}`,
    )
  }
}

/**
 * Send a Slack notification to the specified channel with the given persona.
 * This is a fire-and-forget function that returns immediately and handles
 * all async operations internally. For jobs or long-running processes,
 * use sendSlackNotificationAsync instead.
 *
 * @param channel - The Slack channel to send the notification to
 * @param persona - The persona/type of the notification
 * @param title - The title of the notification
 * @param content - The markdown-formatted content (string) or an array of sections
 */
export function sendSlackNotification(
  channel: SlackChannel,
  persona: SlackPersona,
  title: string,
  content: string | SlackMessageSection[],
): void {
  // Fire and forget - handle async operations internally
  sendSlackNotificationAsync(channel, persona, title, content).catch((error) => {
    // Additional safety net for any errors that might escape the try-catch
    log.error(
      { error, channel, persona, title, service: SERVICE },
      `Unexpected error in Slack notification handler`,
    )
  })
}
