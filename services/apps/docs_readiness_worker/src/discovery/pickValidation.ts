// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { registrableDomain } from '@crowd/common'
import type { IDocCandidate } from '@crowd/data-access-layer'

import type { IDocsValidatorPage } from './docsValidator'
import { primaryRepo } from './github'
import { fetchText, probe } from './http'
import { isRelevantSerpResult, nameTokens, projectTokens } from './relevance'
import type { IDiscoveryContext } from './strategies'

// Search and README links are where almost all wrong docs URLs come from; other sources are trusted.
const VALIDATED_METHODS = new Set<IDocCandidate['method']>(['serp', 'readme-scrape'])

export const MAX_VALIDATOR_CALLS_PER_PROJECT = 3

const MAX_HTML_CHARS = 200_000
const MAX_TEXT_CHARS = 1500
const MAX_ELEMENT_CHARS = 2000
const MAX_META_TAG_CHARS = 2000
const EVIDENCE_TIMEOUT_MS = 5_000

// Validation starts only with this much of the discovery budget left: evidence (5 s) plus the
// model call (20 s) plus the docs-root cut probe (10 s) that follows still fit under the budget.
export const MIN_VALIDATION_BUDGET_MS = 40_000

const ENTITIES: Record<string, string> = {
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&#39;': "'",
  '&nbsp;': ' ',
}

const RAW_TEXT_TAGS = ['script', 'style', 'noscript', 'svg', 'template']

const isNameChar = (code: number): boolean =>
  (code >= 48 && code <= 57) ||
  (code >= 65 && code <= 90) ||
  code === 95 ||
  (code >= 97 && code <= 122)

// Untrusted HTML: every scan below moves forward only, so the work stays linear in the input.
function matchesTagAt(html: string, at: number, name: string): boolean {
  return (
    html.slice(at, at + name.length).toLowerCase() === name &&
    !isNameChar(html.charCodeAt(at + name.length))
  )
}

// Index of the next `<name` (or `</name` when closing), or -1.
function findTag(html: string, name: string, from: number, closing = false): number {
  const skip = closing ? 2 : 1
  for (
    let i = html.indexOf(closing ? '</' : '<', from);
    i !== -1;
    i = html.indexOf(closing ? '</' : '<', i + 1)
  ) {
    if (matchesTagAt(html, i + skip, name)) {
      return i
    }
  }
  return -1
}

function decodeAndCollapse(text: string): string {
  return text
    .replace(/&(?:amp|lt|gt|quot|#39|nbsp);/g, (entity) => ENTITIES[entity])
    .replace(/\s+/g, ' ')
    .trim()
}

function toText(html: string): string {
  const parts: string[] = []
  let i = 0
  while (i < html.length) {
    const lt = html.indexOf('<', i)
    if (lt === -1) {
      parts.push(html.slice(i))
      break
    }
    parts.push(html.slice(i, lt), ' ')

    if (html.startsWith('<!--', lt)) {
      const end = html.indexOf('-->', lt + 4)
      if (end === -1) {
        break
      }
      i = end + 3
      continue
    }

    const rawTag = RAW_TEXT_TAGS.find((name) => matchesTagAt(html, lt + 1, name))
    if (rawTag) {
      const close = findTag(html, rawTag, lt + 1, true)
      const gt = close === -1 ? -1 : html.indexOf('>', close)
      if (gt === -1) {
        break
      }
      i = gt + 1
      continue
    }

    const gt = html.indexOf('>', lt + 1)
    if (gt === -1) {
      parts.push(html.slice(lt))
      break
    }
    i = gt + 1
  }
  return decodeAndCollapse(parts.join(''))
}

function firstElementText(html: string, tag: 'title' | 'h1'): string | null {
  const open = findTag(html, tag, 0)
  const contentStart = open === -1 ? -1 : html.indexOf('>', open)
  if (contentStart === -1) {
    return null
  }
  const close = findTag(html, tag, contentStart + 1, true)
  if (close === -1) {
    return null
  }
  return toText(html.slice(contentStart + 1, Math.min(close, contentStart + 1 + MAX_ELEMENT_CHARS)))
}

function metaDescription(html: string): string | null {
  for (let open = findTag(html, 'meta', 0); open !== -1;) {
    const gt = html.indexOf('>', open)
    if (gt === -1) {
      return null
    }
    const tag = html.slice(open, gt + 1)
    if (
      tag.length <= MAX_META_TAG_CHARS &&
      /\b(?:name|property)\s*=\s*["'](?:og:)?description["']/i.test(tag)
    ) {
      const content = /\bcontent\s*=\s*(?:"([^"]*)"|'([^']*)')/i.exec(tag)
      if (content) {
        return toText(content[1] ?? content[2] ?? '')
      }
    }
    open = findTag(html, 'meta', gt + 1)
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

// No http helper returns status, final URL and body together, so both are fetched, in parallel
// and with the same short timeout: evidence costs at most EVIDENCE_TIMEOUT_MS.
async function fetchPageEvidence(url: string): Promise<IDocsValidatorPage | null> {
  const [result, html] = await Promise.all([
    probe(url, EVIDENCE_TIMEOUT_MS),
    fetchText(url, EVIDENCE_TIMEOUT_MS),
  ])
  return html === null ? null : extractPageEvidence(html, result.finalUrl || url, result.status)
}

function isRelatedToProject(ctx: IDiscoveryContext, repoUrl: string | null, url: string): boolean {
  const tokens = projectTokens({ name: ctx.name, slug: ctx.slug, repoUrl })
  const hyphenTokens = [...nameTokens(ctx.name), ...nameTokens(ctx.slug)]
  if (isRelevantSerpResult(url, tokens, { hyphenTokens })) {
    return true
  }
  const siteRoot = ctx.website && !ctx.websiteShared ? registrableDomain(ctx.website) : null
  return !!siteRoot && siteRoot === registrableDomain(url)
}

// Fail open: a pick the validator could not judge (error, timeout, no credentials, no evidence,
// little time left) is kept; a verdict of other drops it, and unclear drops it unless the pick's
// domain is tied to the project.
export async function pickValidatedWinner(
  ctx: IDiscoveryContext,
  candidates: IDocCandidate[],
  rank: (pool: IDocCandidate[]) => IDocCandidate | null,
): Promise<IDocCandidate | null> {
  const validate = ctx.docsValidator
  if (!validate) {
    return rank(candidates)
  }

  const repoUrl = primaryRepo(ctx.repos, { slug: ctx.slug, name: ctx.name })
  let pool = candidates
  let calls = 0
  for (;;) {
    const winner = rank(pool)
    if (!winner || !VALIDATED_METHODS.has(winner.method)) {
      return winner
    }
    const others = pool.filter((c) => c !== winner)
    const fields = { slug: ctx.slug, method: winner.method, url: winner.url }
    // A pick the cap leaves unchecked is not accepted: a wrong URL is worse than the next candidate.
    if (calls >= MAX_VALIDATOR_CALLS_PER_PROJECT) {
      ctx.log?.info(
        { ...fields, outcome: 'call-cap' },
        'docs pick dropped, validator call cap reached',
      )
      pool = others
      continue
    }
    if (ctx.deadlineAt !== undefined && ctx.deadlineAt - Date.now() < MIN_VALIDATION_BUDGET_MS) {
      ctx.log?.info({ ...fields, outcome: 'budget' }, 'docs pick kept without validation')
      return winner
    }

    try {
      const page = await fetchPageEvidence(winner.url)
      if (!page) {
        ctx.log?.info(
          { ...fields, outcome: 'no-page-evidence' },
          'docs pick kept without validation',
        )
        return winner
      }
      calls++
      const { verdict } = await validate({ name: ctx.name, website: ctx.website, repoUrl }, page)
      ctx.log?.info({ ...fields, verdict }, 'docs pick validated')
      if (verdict === 'documents_project') {
        return winner
      }
      if (verdict === 'unclear' && isRelatedToProject(ctx, repoUrl, winner.url)) {
        ctx.log?.info({ ...fields, outcome: 'unclear-related' }, 'docs pick kept, domain matches')
        return winner
      }
      pool = others
    } catch (err) {
      ctx.log?.warn(
        {
          ...fields,
          outcome: 'validator-error',
          errorName: err instanceof Error ? err.name : 'unknown',
        },
        'docs pick validation failed, keeping the pick',
      )
      return winner
    }
  }
}
