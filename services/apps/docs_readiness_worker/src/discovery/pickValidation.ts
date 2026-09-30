// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import type { IDocCandidate } from '@crowd/data-access-layer'

import type { IDocsValidatorPage } from './docsValidator'
import { primaryRepo } from './github'
import { fetchText, probe } from './http'
import type { IDiscoveryContext } from './strategies'

// Search and README links are where almost all wrong docs URLs come from; other sources are trusted.
const VALIDATED_METHODS = new Set<IDocCandidate['method']>(['serp', 'readme-scrape'])

export const MAX_VALIDATOR_CALLS_PER_PROJECT = 2

const MAX_HTML_CHARS = 300_000
const MAX_TEXT_CHARS = 1500

const ENTITIES: Record<string, string> = {
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&#39;': "'",
  '&nbsp;': ' ',
}

function toText(html: string): string {
  return html
    .replace(/<!--[\s\S]*?(?:-->|$)/g, ' ')
    .replace(/<(script|style|noscript|svg|template)\b[\s\S]*?(?:<\/\1>|$)/gi, ' ')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&(?:amp|lt|gt|quot|#39|nbsp);/g, (entity) => ENTITIES[entity])
    .replace(/\s+/g, ' ')
    .trim()
}

function firstElementText(html: string, tag: 'title' | 'h1'): string | null {
  const match = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)</${tag}>`, 'i').exec(html)
  return match ? toText(match[1]) : null
}

function metaDescription(html: string): string | null {
  for (const tag of html.match(/<meta\b[^>]*>/gi) ?? []) {
    if (/\b(?:name|property)\s*=\s*["'](?:og:)?description["']/i.test(tag)) {
      const content = /\bcontent\s*=\s*(?:"([^"]*)"|'([^']*)')/i.exec(tag)
      if (content) {
        return toText(content[1] ?? content[2] ?? '')
      }
    }
  }
  return null
}

export function extractPageEvidence(
  html: string,
  finalUrl: string,
  status: number,
): IDocsValidatorPage {
  const head = html.slice(0, MAX_HTML_CHARS)
  return {
    finalUrl,
    status,
    title: firstElementText(head, 'title'),
    h1: firstElementText(head, 'h1'),
    description: metaDescription(head),
    text: toText(head).slice(0, MAX_TEXT_CHARS),
  }
}

async function fetchPageEvidence(url: string): Promise<IDocsValidatorPage | null> {
  const [result, html] = await Promise.all([probe(url), fetchText(url)])
  return html === null ? null : extractPageEvidence(html, result.finalUrl || url, result.status)
}

// Fail open: a candidate the validator could not judge because of an unexpected error is kept.
export async function pickValidatedWinner(
  ctx: IDiscoveryContext,
  candidates: IDocCandidate[],
  rank: (pool: IDocCandidate[]) => IDocCandidate | null,
): Promise<IDocCandidate | null> {
  const validate = ctx.docsValidator
  if (!validate) {
    return rank(candidates)
  }

  let pool = candidates
  let calls = 0
  for (;;) {
    const winner = rank(pool)
    if (!winner || !VALIDATED_METHODS.has(winner.method)) {
      return winner
    }
    const others = pool.filter((c) => c !== winner)
    // A pick the cap leaves unchecked is not accepted: a wrong URL is worse than the next candidate.
    if (calls >= MAX_VALIDATOR_CALLS_PER_PROJECT) {
      pool = others
      continue
    }

    const fields = { slug: ctx.slug, method: winner.method, url: winner.url }
    try {
      const page = await fetchPageEvidence(winner.url)
      if (!page) {
        ctx.log?.info(
          { ...fields, verdict: 'no-page-evidence' },
          'docs pick kept without validation',
        )
        return winner
      }
      calls++
      const { verdict } = await validate(
        {
          name: ctx.name,
          website: ctx.website,
          repoUrl: primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name }),
        },
        page,
      )
      ctx.log?.info({ ...fields, verdict }, 'docs pick validated')
      if (verdict === 'documents_project') {
        return winner
      }
      pool = others
    } catch (err) {
      ctx.log?.warn(
        { ...fields, errorName: err instanceof Error ? err.name : 'unknown' },
        'docs pick validation failed, keeping the pick',
      )
      return winner
    }
  }
}
