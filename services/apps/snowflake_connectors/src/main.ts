/**
 * Temporal worker setup.
 *
 * Uses the ServiceWorker archetype which handles Temporal connection,
 * workflow bundling, and activity registration automatically.
 */
import { Config } from '@crowd/archetype-standard'
import { Options, ServiceWorker } from '@crowd/archetype-worker'

const config: Config = {
  envvars: [
    // 'CROWD_SNOWFLAKE_ENABLED_PLATFORMS', // allow optional to enable/disable worker
    'CROWD_SNOWFLAKE_S3_BUCKET_PATH',
    'CROWD_SNOWFLAKE_STORAGE_INTEGRATION',
    'CROWD_SNOWFLAKE_S3_REGION',
    'CROWD_SNOWFLAKE_S3_ACCESS_KEY_ID',
    'CROWD_SNOWFLAKE_S3_SECRET_ACCESS_KEY',
  ],
  producer: {
    enabled: false,
  },
  temporal: {
    enabled: true,
  },
  redis: {
    enabled: true,
  },
}

const options: Options = {
  postgres: {
    enabled: true,
  },
  opensearch: {
    enabled: false,
  },
}

export const svc = new ServiceWorker(config, options)
