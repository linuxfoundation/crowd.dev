import crypto from 'crypto'

import { Response } from 'express'

import { generateUUIDv4 as uuid } from '@crowd/common'

import { GITLAB_CONFIG } from '../../../conf'
import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

/// credits to lucia-auth library for these functions

const createUrl = (url: string | URL, urlSearchParams: Record<string, string | undefined>): URL => {
  const newUrl = new URL(url)
  for (const [key, value] of Object.entries(urlSearchParams)) {
    // eslint-disable-next-line no-continue
    if (!value) continue
    newUrl.searchParams.set(key, value)
  }
  return newUrl
}

const getRandomValues = (bytes: number): Uint8Array => {
  const buffer = crypto.randomBytes(bytes)
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

const DEFAULT_ALPHABET = 'abcdefghijklmnopqrstuvwxyz1234567890'

export const generateRandomString = (size: number, alphabet = DEFAULT_ALPHABET): string => {
  // eslint-disable-next-line no-bitwise
  const mask = (2 << (Math.log(alphabet.length - 1) / Math.LN2)) - 1
  // eslint-disable-next-line no-bitwise
  const step = -~((1.6 * mask * size) / alphabet.length)

  let bytes = getRandomValues(step)
  let id = ''
  let index = 0

  while (id.length !== size) {
    // eslint-disable-next-line no-bitwise
    id += alphabet[bytes[index] & mask] ?? ''
    index += 1
    if (index > bytes.length) {
      bytes = getRandomValues(step)
      index = 0
    }
  }
  return id
}

const encodeBase64 = (data: string | ArrayLike<number> | ArrayBufferLike) => {
  if (typeof Buffer === 'function') {
    // node or bun
    const bufferData = typeof data === 'string' ? data : new Uint8Array(data)
    return Buffer.from(bufferData).toString('base64')
  }
  if (typeof data === 'string') return btoa(data)
  return btoa(String.fromCharCode(...new Uint8Array(data)))
}

const encodeBase64Url = (data: string | ArrayLike<number> | ArrayBufferLike) =>
  encodeBase64(data).replaceAll('=', '').replaceAll('+', '-').replaceAll('/', '_')

/// end credits

export default async (req, res: Response) => {
  // Checking we have permision to edit the project
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  const handle = uuid()

  const callbackUrl = GITLAB_CONFIG.callbackUrl

  const gitlabState = {
    crowdToken: req.query.crowdToken,
    tenantId: req.params.tenantId,
    handle,
  }

  const scopes = ['api', 'read_api', 'read_user', 'read_repository', 'profile', 'email']

  // Build the authorization URL
  const authUrl = createUrl('https://gitlab.com/oauth/authorize', {
    client_id: GITLAB_CONFIG.clientId,
    response_type: 'code',
    state: encodeBase64Url(JSON.stringify(gitlabState)),
    redirect_uri: callbackUrl,
    scope: scopes.join(' '),
  })

  // Redirect user to the authorization URL
  res.redirect(authUrl.toString())
}
