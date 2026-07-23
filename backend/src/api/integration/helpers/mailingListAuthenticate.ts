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
    return url.protocol === 'https:' && !isBlockedHost(url.hostname.toLowerCase())
  } catch {
    return false
  }
}

// "https://host/list" and "https://host/list/" must resolve to the same DB
// row — the worker's ensure_mirror() already normalizes to this form before
// cloning (mirror_service.py), so storage must match or the same archive
// ends up mirrored twice under two rows. Kept as a plain function (not a
// zod .transform()/.preprocess()) since either makes the field optional in
// z.infer with the installed zod v4 — reproduced in isolation, unrelated to
// this schema's nesting.
export const canonicalizeSourceUrl = (sourceUrl: string): string => sourceUrl.replace(/\/+$/, '')

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
