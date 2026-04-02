import jwt from 'jsonwebtoken'
import jwksClient from 'jwks-rsa'

import { DEFAULT_TENANT_ID, Error401 } from '@crowd/common'

import { AUTH0_CONFIG } from '../../conf'
import AuthService from '../../services/auth/authService'

const jwks = jwksClient({
  jwksUri: AUTH0_CONFIG.jwks,
  cache: true,
  cacheMaxEntries: 5,
  cacheMaxAge: 86400000,
})

async function getKey(header, callback) {
  jwks.getSigningKey(header.kid, (err, key: any) => {
    const signingKey = key.publicKey || key.rsaPublicKey
    callback(null, signingKey)
  })
}

export default async (req, res) => {
  const { idToken, invitationToken } = req.body
  const tenantId = DEFAULT_TENANT_ID

  try {
    const verifyToken = new Promise((resolve, reject) => {
      jwt.verify(idToken, getKey, { algorithms: ['RS256'] }, (err, decoded) => {
        if (err) {
          req.log.error('Error verifying token', err)
          reject(new Error401())
        }

        const { aud } = decoded as any

        if (aud !== AUTH0_CONFIG.clientId) {
          req.log.error(`Invalid audience: ${aud}`)
          reject(new Error401())
        }

        resolve(decoded)
      })
    })
    const data: any = await verifyToken

    // Signin with data
    const token: string = await AuthService.signinFromSSO(
      'auth0',
      data.sub,
      data.email,
      data.email_verified,
      data.given_name,
      data.family_name,
      data.name,
      invitationToken,
      tenantId,
      data['http://lfx.dev/claims/roles'],
      req,
    )
    return res.send(token)
  } catch (err) {
    req.log.error('Error verifying token', err)
    return res.status(401).send({ error: err })
  }
}
