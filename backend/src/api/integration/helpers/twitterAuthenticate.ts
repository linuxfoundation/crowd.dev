import crypto from 'crypto'

import { Response } from 'express'

import { generateUUIDv4 as uuid } from '@crowd/common'
import { RedisCache } from '@crowd/redis'
import { PlatformType } from '@crowd/types'

import { TWITTER_CONFIG } from '../../../conf'
import SequelizeRepository from '../../../database/repositories/sequelizeRepository'
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

const generatePKCECodeChallenge = (method: 'S256', verifier: string) => {
  if (method === 'S256') {
    const hash = crypto.createHash('sha256')
    hash.update(verifier)
    const challengeBuffer = hash.digest()
    return encodeBase64Url(challengeBuffer)
  }
  throw new TypeError('Invalid PKCE code challenge method')
}

/// end credits

export default async (req, res: Response) => {
  // Checking we have permision to edit the project
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  const cache = new RedisCache('twitterPKCE', req.redis, req.log)

  // Generate code verifier and challenge for PKCE
  const codeVerifier = generateRandomString(
    96,
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_.~',
  )
  const codeChallenge = generatePKCECodeChallenge('S256', codeVerifier)

  const handle = uuid()

  const callbackUrl = TWITTER_CONFIG.callbackUrl

  const state = {
    handle,
    tenantId: req.params.tenantId,
    redirectUrl: req.query.redirectUrl,
    callbackUrl,
    hashtags: req.query.hashtags ? req.query.hashtags : '',
    crowdToken: req.query.crowdToken,
    platform: PlatformType.TWITTER,
    userId: req.currentUser.id,
    codeVerifier,
    segmentIds: SequelizeRepository.getSegmentIds(req),
  }

  const twitterState = {
    crowdToken: req.query.crowdToken,
    tenantId: req.params.tenantId,
    handle,
  }

  // Save state to redis
  await cache.set(req.currentUser.id, JSON.stringify(state), 300)

  const scopes = ['tweet.read', 'tweet.write', 'users.read', 'follows.read', 'offline.access']

  // Build the authorization URL
  const authUrl = createUrl('https://twitter.com/i/oauth2/authorize', {
    client_id: TWITTER_CONFIG.clientId,
    response_type: 'code',
    state: encodeBase64Url(JSON.stringify(twitterState)),
    redirect_uri: callbackUrl,
    code_challenge: codeChallenge,
    code_challenge_method: 'S256',
    scope: scopes.join(' '),
  })

  // Redirect user to the authorization URL
  res.redirect(authUrl.toString())
}
