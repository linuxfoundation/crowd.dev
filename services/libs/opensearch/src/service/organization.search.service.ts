import { DbStore } from '@crowd/database'
import { Logger, getChildLogger } from '@crowd/logging'
import { OpenSearchIndex } from '@crowd/types'

import { FieldTranslatorFactory } from '../fieldTranslatorFactory'
import { OpensearchQueryParser } from '../opensearchQueryParser'
import { OpenSearchService } from './opensearch.service'

export class OrganizationSearchService {
  private log: Logger

  constructor(
    store: DbStore,
    private readonly openSearchService: OpenSearchService,
    parentLog: Logger,
  ) {
    this.log = getChildLogger('organization-search-service', parentLog)
  }

  public async findAndCountAll(
    tenantId: string,
    {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      filter = {} as any,
      limit = 20,
      offset = 0,
      orderBy = 'joinedAt_DESC',
      countOnly = false,
      segments = [] as string[],
    },
  ) {
    const translator = FieldTranslatorFactory.getTranslator(OpenSearchIndex.ORGANIZATIONS)
    const parsed = OpensearchQueryParser.parse(
      { filter, limit, offset, orderBy },
      OpenSearchIndex.ORGANIZATIONS,
      translator,
    )

    // add tenant filter to parsed query
    parsed.query.bool.must.push({
      term: {
        uuid_tenantId: tenantId,
      },
    })

    // remove deleted members
    if (!parsed.query.bool.must_not) {
      parsed.query.bool.must_not = []
    }
    parsed.query.bool.must_not.push({
      term: {
        'obj_attributes.obj_isDeleted.bool_default': true,
      },
    })

    if (segments) {
      // add segment filter
      parsed.query.bool.must.push({
        terms: {
          uuid_segmentId: segments,
        },
      })
    }

    const countResponse = await this.openSearchService.client.count({
      index: OpenSearchIndex.ORGANIZATIONS,
      body: { query: parsed.query },
    })

    if (countOnly) {
      return {
        rows: [],
        count: countResponse.body.count,
        limit,
        offset,
      }
    }

    parsed.from = offset || 0

    const response = await this.openSearchService.client.search({
      index: OpenSearchIndex.ORGANIZATIONS,
      body: parsed,
    })

    const translatedRows = response.body.hits.hits.map((o) =>
      translator.translateObjectToCrowd(o._source),
    )

    return { rows: translatedRows, count: countResponse.body.count, limit, offset }
  }
}
