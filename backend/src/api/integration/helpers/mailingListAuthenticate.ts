import { z } from 'zod'

import Permissions from '../../../security/permissions'
import IntegrationService from '../../../services/integrationService'
import PermissionChecker from '../../../services/user/permissionChecker'
import { validateOrThrow } from '../../../utils/validation'

const MAX_LIST_NAME_LENGTH = 255

// `name` becomes a single filesystem path component for the mailing list
// mirror (see mirror_service.py's list_mirror_dir), so it must not contain a
// path separator or resolve to "." / ".." when used alone.
const isSafeListName = (name: string): boolean =>
  name.length > 0 &&
  name.length <= MAX_LIST_NAME_LENGTH &&
  !name.includes('/') &&
  !name.includes('\0') &&
  name !== '.' &&
  name !== '..'

// public-inbox-clone in the worker fetches this URL as-is; restrict to
// https so a caller can't point the worker at file://, javascript:, or a
// bare non-URL string. Requiring https already blocks the classic SSRF
// target (cloud-metadata IMDS is http-only, per securityTxt.ts precedent);
// also reject obvious loopback/localhost literals.
const isBlockedHost = (h: string): boolean =>
  h === 'localhost' || h === '::1' || h === '0.0.0.0' || h.startsWith('127.')

const isSafeSourceUrl = (sourceUrl: string): boolean => {
  try {
    const url = new URL(sourceUrl)
    return (
      url.protocol === 'https:' &&
      !isBlockedHost(url.hostname.toLowerCase()) &&
      // Credentials/query/fragment have no meaning for a public-inbox archive
      // URL; rejecting them keeps canonicalizeSourceUrl a lossless
      // normalization instead of one that silently drops caller-supplied data.
      url.username === '' &&
      url.password === '' &&
      url.search === '' &&
      url.hash === ''
    )
  } catch {
    return false
  }
}

// "https://host/list" and "https://HOST/list/" and "https://host:443/list"
// must all resolve to the same DB row — scheme case, host case, the default
// https port, and a trailing slash are not meaningful differences for the
// same archive, but a plain trailing-slash strip left them as distinct
// strings, bypassing the cross-project ownership check and causing
// duplicate ingestion. The worker's ensure_mirror() already normalizes to
// this same form before cloning (mirror_service.py), so storage must match.
// Kept as a plain function (not a zod .transform()/.preprocess()) since
// either makes the field optional in z.infer with the installed zod v4 —
// reproduced in isolation, unrelated to this schema's nesting.
export const canonicalizeSourceUrl = (sourceUrl: string): string => {
  const url = new URL(sourceUrl)
  const port = url.port && url.port !== '443' ? `:${url.port}` : ''
  const path = url.pathname.replace(/\/+$/, '') || '/'
  return `https://${url.hostname.toLowerCase()}${port}${path}`
}

export const bodySchema = z.object({
  lists: z
    .array(
      z.object({
        name: z.string().trim().min(1).refine(isSafeListName, {
          message: 'Invalid mailing list name',
        }),
        sourceUrl: z.string().trim().min(1).refine(isSafeSourceUrl, {
          message: 'sourceUrl must be a valid https:// URL',
        }),
      }),
    )
    .min(1, 'lists must contain at least one mailing list')
    .refine(
      (lists) =>
        new Set(lists.map((l) => canonicalizeSourceUrl(l.sourceUrl))).size === lists.length,
      { message: 'lists contains duplicate sourceUrl entries' },
    ),
})

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.tenantEdit)
  const integrationData = validateOrThrow(bodySchema, req.body)
  integrationData.lists = integrationData.lists.map((l) => ({
    ...l,
    sourceUrl: canonicalizeSourceUrl(l.sourceUrl),
  }))

  const payload = await new IntegrationService(req).mailingListConnectOrUpdate(integrationData)
  await req.responseHandler.success(req, res, payload)
}
