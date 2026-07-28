import { createHash } from 'node:crypto'

import { parseGithubUrl } from '../../enricher/fetchLightRepo'
import type { githubApiGet } from '../githubToken'

const MAX_PAGE_BYTES = 500_000

export function sha256Hex(text: string): string {
  return createHash('sha256').update(text).digest('hex')
}

export async function fetchBlob(
  deps: { githubGet: typeof githubApiGet },
  repoUrl: string,
  blobOid: string,
  timeoutMs: number,
): Promise<string | null> {
  let owner: string
  let name: string
  try {
    ;({ owner, name } = parseGithubUrl(repoUrl))
  } catch {
    return null
  }
  const { text } = await deps.githubGet(`/repos/${owner}/${name}/git/blobs/${blobOid}`, timeoutMs, {
    raw: true,
  })
  return text
}

export function htmlToText(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#?\w+;/g, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
}

export async function fetchLinkedPage(
  url: string,
  timeoutMs: number,
): Promise<{ text: string; hash: string } | null> {
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { 'User-Agent': 'crowd.dev-reporting-protocol' },
      redirect: 'follow',
    })
    if (!res.ok) return null
    const raw = await res.text()
    const text = htmlToText(raw.slice(0, MAX_PAGE_BYTES))
    if (!text) return null
    return { text, hash: sha256Hex(text) }
  } catch {
    return null
  } finally {
    clearTimeout(timeoutHandle)
  }
}
