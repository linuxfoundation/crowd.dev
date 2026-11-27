import { describe, expect, it, vi } from 'vitest'

import { DbConfiguration } from '@/conf/configTypes'

vi.mock('@/conf', () => ({ DB_CONFIG: {} }))

import { eventMappingWriterConfig } from './eventMappingQx'

const base: DbConfiguration = {
  readHost: 'read.example.test',
  writeHost: 'write.example.test',
  port: 5432,
  dialect: 'postgres',
  database: 'example',
  logging: false,
  transactions: true,
}

describe('eventMappingWriterConfig', () => {
  it.each([
    [
      'the shared credentials when they are set',
      {
        username: 'shared',
        password: 'shared-secret',
        apiUsername: 'api',
        apiPassword: 'api-secret',
      },
      { user: 'shared', password: 'shared-secret' },
    ],
    [
      'the api credentials when the shared ones are not set',
      { apiUsername: 'api', apiPassword: 'api-secret' },
      { user: 'api', password: 'api-secret' },
    ],
  ])('connects to the writer with %s', (_, credentials, expected) => {
    expect(eventMappingWriterConfig({ ...base, ...credentials })).toStrictEqual({
      host: 'write.example.test',
      port: 5432,
      database: 'example',
      ...expected,
    })
  })

  it('refuses to connect without credentials', () => {
    expect(() => eventMappingWriterConfig(base)).toThrow('No database credentials')
  })
})
