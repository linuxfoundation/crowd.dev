/* eslint-disable no-console */

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'
import crypto from 'crypto'

import { createApiKey } from '@crowd/data-access-layer/src/apiKeys'

import { databaseInit } from '@/database/databaseConnection'
import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'name',
    alias: 'n',
    type: String,
    description: 'Human-readable name for the key (e.g. "projects-evaluation-worker").',
  },
  {
    name: 'scopes',
    alias: 's',
    type: String,
    defaultValue: '',
    description: 'Comma-separated list of scopes (e.g. "project-evaluation:write").',
  },
  {
    name: 'expiresInDays',
    alias: 'e',
    type: Number,
    description: 'Optional expiry, in days from now. Omit for a key that never expires.',
  },
]

const sections = [
  {
    header: 'Create API key',
    content:
      'Generates a static API key for service-to-service auth (staticApiKeyMiddleware) ' +
      'and stores its hash in the apiKeys table. The raw key is printed once and never persisted.',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
const argv = process.argv.slice(2).filter((arg) => arg !== '--')
const parameters = commandLineArgs(options, { argv })

if (parameters.help || !parameters.name) {
  console.log(usage)
  process.exit(parameters.help ? 0 : 1)
} else {
  setImmediate(async () => {
    const prodDb = await databaseInit()
    const qx = SequelizeRepository.getQueryExecutor({ database: prodDb } as IRepositoryOptions)

    const rawKey = crypto.randomBytes(32).toString('hex')
    const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex')
    const keyPrefix = rawKey.slice(0, 8)

    const scopes = (parameters.scopes as string)
      .split(',')
      .map((scope) => scope.trim())
      .filter(Boolean)

    const expiresAt = parameters.expiresInDays
      ? new Date(Date.now() + parameters.expiresInDays * 24 * 60 * 60 * 1000)
      : null

    const id = await createApiKey(qx, {
      name: parameters.name,
      keyHash,
      keyPrefix,
      scopes,
      expiresAt,
      createdById: null,
    })

    console.log(`Created API key id=${id} name=${parameters.name} prefix=${keyPrefix}`)
    console.log(`Raw key (store it now, it won't be shown again): ${rawKey}`)

    process.exit(0)
  })
}
