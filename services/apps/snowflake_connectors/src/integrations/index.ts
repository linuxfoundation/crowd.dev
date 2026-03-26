/**
 * Platform registry / discovery.
 *
 * All supported platforms are registered here, keyed by PlatformType.
 * Only those listed in CROWD_SNOWFLAKE_ENABLED_PLATFORMS (comma-separated) are active.
 */
import { PlatformType } from '@crowd/types'

import { buildSourceQuery as cventBuildSourceQuery } from './cvent/event-registrations/buildSourceQuery'
import { CventTransformer } from './cvent/event-registrations/transformer'
import { buildSourceQuery as tncCertificatesBuildQuery } from './tnc/certificates/buildSourceQuery'
import { TncCertificatesTransformer } from './tnc/certificates/transformer'
import { buildSourceQuery as tncCoursesBuildQuery } from './tnc/courses/buildSourceQuery'
import { TncCoursesTransformer } from './tnc/courses/transformer'
import { buildSourceQuery as tncEnrollmentsBuildQuery } from './tnc/enrollments/buildSourceQuery'
import { TncEnrollmentsTransformer } from './tnc/enrollments/transformer'
import { DataSource, DataSourceName, PlatformDefinition } from './types'

export type { BuildSourceQuery, DataSource, PlatformDefinition } from './types'
export { DataSourceName } from './types'

const supported: Partial<Record<PlatformType, PlatformDefinition>> = {
  [PlatformType.CVENT]: {
    sources: [
      {
        name: DataSourceName.CVENT_EVENT_REGISTRATIONS,
        buildSourceQuery: cventBuildSourceQuery,
        transformer: new CventTransformer(),
      },
    ],
  },
  [PlatformType.TNC]: {
    sources: [
      {
        name: DataSourceName.TNC_ENROLLMENTS,
        buildSourceQuery: tncEnrollmentsBuildQuery,
        transformer: new TncEnrollmentsTransformer(),
      },
      {
        name: DataSourceName.TNC_CERTIFICATES,
        buildSourceQuery: tncCertificatesBuildQuery,
        transformer: new TncCertificatesTransformer(),
      },
      {
        name: DataSourceName.TNC_COURSES,
        buildSourceQuery: tncCoursesBuildQuery,
        transformer: new TncCoursesTransformer(),
      },
    ],
  },
}

const enabled = (process.env.CROWD_SNOWFLAKE_ENABLED_PLATFORMS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean) as PlatformType[]

export function getPlatform(platform: PlatformType): PlatformDefinition {
  if (!supported[platform]) {
    throw new Error(`Unsupported platform: ${platform}`)
  }
  if (!enabled.includes(platform)) {
    throw new Error(`Platform not enabled: ${platform} (enabled: ${enabled.join(', ')})`)
  }
  return supported[platform]
}

export function getDataSourceNames(platform: PlatformType): string[] {
  return getPlatform(platform).sources.map((s) => s.name)
}

export function getDataSource(platform: PlatformType, sourceName: string): DataSource {
  const def = getPlatform(platform)
  const source = def.sources.find((s) => s.name === sourceName)
  if (!source) {
    throw new Error(`Unknown data source '${sourceName}' for platform '${platform}'`)
  }
  return source
}

export function getEnabledPlatforms(): PlatformType[] {
  return enabled
}
