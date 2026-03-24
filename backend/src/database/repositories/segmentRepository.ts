import lodash from 'lodash'
import { QueryTypes } from 'sequelize'
import { v4 as uuid } from 'uuid'

import { Error404 } from '@crowd/common'
import {
  buildSegmentActivityTypes,
  findSegmentById,
  getMappedWithSegmentName,
  getSegmentActivityTypes,
  hasMappedRepos,
  isSegmentProject,
  isSegmentProjectGroup,
  populateSegmentRelations,
} from '@crowd/data-access-layer/src/segments'
import {
  ActivityTypeSettings,
  PageData,
  PlatformType,
  QueryData,
  SegmentCreateData,
  SegmentData,
  SegmentLevel,
  SegmentProjectGroupNestedData,
  SegmentProjectNestedData,
  SegmentStatus,
  SegmentUpdateChildrenPartialData,
  SegmentUpdateData,
} from '@crowd/types'

import removeFieldsFromObject from '../../utils/getObjectWithoutKey'

import { IRepositoryOptions } from './IRepositoryOptions'
import IntegrationRepository from './integrationRepository'
import { RepositoryBase } from './repositoryBase'
import SequelizeRepository from './sequelizeRepository'

class SegmentRepository extends RepositoryBase<
  SegmentData,
  string,
  SegmentCreateData,
  unknown,
  unknown
> {
  public constructor(options: IRepositoryOptions) {
    super(options, true)
  }

  /**
   * Insert a segment.
   * @param data segment data
   * @returns
   */
  async create(data: SegmentCreateData): Promise<SegmentData> {
    const transaction = this.transaction

    const id = uuid()

    await this.options.database.sequelize.query(
      `INSERT INTO "segments" ("id", "url", "name", "slug", "parentSlug", "grandparentSlug", "status", "parentName", "sourceId", "sourceParentId", "tenantId", "grandparentName", "parentId", "grandparentId", "isLF")
          VALUES
              (:id, :url, :name, :slug, :parentSlug, :grandparentSlug, :status, :parentName, :sourceId, :sourceParentId, :tenantId, :grandparentName, :parentId, :grandparentId, :isLF)
        `,
      {
        replacements: {
          id,
          url: data.url || null,
          name: data.name,
          parentName: data.parentName || null,
          grandparentName: data.grandparentName || null,
          slug: data.slug,
          parentSlug: data.parentSlug || null,
          grandparentSlug: data.grandparentSlug || null,
          status: data.status || SegmentStatus.ACTIVE,
          sourceId: data.sourceId || null,
          sourceParentId: data.sourceParentId || null,
          tenantId: this.options.currentTenant.id,
          parentId: data.parentId || null,
          grandparentId: data.grandparentId || null,
          isLF: data.isLF ?? true,
        },
        type: QueryTypes.INSERT,
        transaction,
      },
    )

    const segment = await this.findById(id)
    return segment
  }

  public async findById(id: string): Promise<SegmentData> {
    return findSegmentById(this.queryExecutor, id)
  }

  /**
   * Updates:
   * parent slugs of children => parentSlug, grandparentSlug
   * parent names of children => parentName, grandparentName
   * @param id
   * @param slug
   * @param name
   */
  async updateChildrenBulk(
    segment: SegmentData,
    data: SegmentUpdateChildrenPartialData,
  ): Promise<SegmentData> {
    if (isSegmentProjectGroup(segment)) {
      // update projects
      await this.updateBulk(
        (segment as SegmentProjectGroupNestedData).projects.map((p) => p.id),
        {
          parentName: data.name,
          parentSlug: data.slug,
        },
      )

      const subprojectIds = (segment as SegmentProjectGroupNestedData).projects.reduce((acc, p) => {
        acc.push(...(p as SegmentProjectNestedData).subprojects.map((sp) => sp.id))
        return acc
      }, [])

      await this.updateBulk(subprojectIds, {
        grandparentSlug: data.slug,
        grandparentName: data.name,
      })
    } else if (isSegmentProject(segment)) {
      // update subprojects
      await this.updateBulk(
        (segment as SegmentProjectNestedData).subprojects.map((sp) => sp.id),
        {
          parentName: data.name,
          parentSlug: data.slug,
          isLF: data.isLF,
        },
      )
    }

    return this.findById(segment.id)
  }

  async updateBulk(ids: string[], data: SegmentUpdateData): Promise<string[]> {
    const transaction = this.transaction

    // strip arbitrary fields
    const nullishValues = [undefined, null, '', NaN]
    const updateFields = Object.keys(data).filter(
      (key) =>
        !nullishValues.includes(data[key]) &&
        [
          'name',
          'slug',
          'parentSlug',
          'grandparentSlug',
          'parentId',
          'grandparentId',
          'status',
          'parentName',
          'sourceId',
          'sourceParentId',
          'grandparentName',
          'isLF',
        ].includes(key),
    )

    let segmentUpdateQuery = `UPDATE segments SET `
    const replacements = {} as any

    for (const field of updateFields) {
      segmentUpdateQuery += ` "${field}" = :${field} `
      replacements[field] = data[field]

      if (updateFields[updateFields.length - 1] !== field) {
        segmentUpdateQuery += ', '
      }
    }

    segmentUpdateQuery += ` WHERE id in (:ids) and "tenantId" = :tenantId returning id`
    replacements.tenantId = this.options.currentTenant.id
    replacements.ids = ids

    const idsUpdated = await this.options.database.sequelize.query(segmentUpdateQuery, {
      replacements,
      type: QueryTypes.UPDATE,
      transaction,
    })

    return idsUpdated
  }

  override async update(id: string, data: SegmentUpdateData): Promise<SegmentData> {
    const transaction = this.transaction

    const segment = await this.findById(id)

    if (!segment) {
      throw new Error404()
    }

    // strip arbitrary fields
    const updateFields = Object.keys(data).filter((key) =>
      [
        'name',
        'url',
        'slug',
        'parentSlug',
        'grandparentSlug',
        'status',
        'parentName',
        'sourceId',
        'sourceParentId',
        'customActivityTypes',
        'isLF',
      ].includes(key),
    )

    if (updateFields.length > 0) {
      let segmentUpdateQuery = `UPDATE segments SET `
      const replacements = {} as any

      for (const field of updateFields) {
        segmentUpdateQuery += ` "${field}" = :${field} `
        replacements[field] = data[field]

        if (updateFields[updateFields.length - 1] !== field) {
          segmentUpdateQuery += ', '
        }
      }

      segmentUpdateQuery += ` WHERE id = :id and "tenantId" = :tenantId `
      replacements.tenantId = this.options.currentTenant.id
      replacements.id = id

      if (replacements.customActivityTypes) {
        replacements.customActivityTypes = JSON.stringify(replacements.customActivityTypes)
      }

      await this.options.database.sequelize.query(segmentUpdateQuery, {
        replacements,
        type: QueryTypes.UPDATE,
        transaction,
      })
    }

    return this.findById(id)
  }

  async addActivityChannel(segmentId: string, platform: string, channel: string) {
    const transaction = this.transaction

    await this.options.database.sequelize.query(
      `
        INSERT INTO "segmentActivityChannels" ("tenantId", "segmentId", "platform", "channel")
        VALUES (:tenantId, :segmentId, :platform, :channel)
        ON CONFLICT DO NOTHING;
      `,
      {
        replacements: {
          tenantId: this.options.currentTenant.id,
          segmentId,
          platform,
          channel,
        },
        type: QueryTypes.INSERT,
        transaction,
      },
    )
  }

  async fetchActivityChannels(segmentId: string) {
    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `
        SELECT
          "platform",
          json_agg(DISTINCT "channel") AS "channels"
        FROM "segmentActivityChannels"
        WHERE "tenantId" = :tenantId
          AND "segmentId" = :segmentId
        GROUP BY "platform";
      `,
      {
        replacements: {
          tenantId: this.options.currentTenant.id,
          segmentId,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return records.reduce((acc, r) => {
      acc[r.platform] = r.channels
      return acc
    }, {})
  }

  async getSegmentSubprojects(segments: string[]) {
    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `
      with input_segment AS (
        select
          id,
          slug,
          "parentSlug",
          "grandparentSlug"
        from segments
        where id in (:segmentIds)
          and "tenantId" = :tenantId
      ),
      segment_level AS (
        select
          case
            when "parentSlug" is not null and "grandparentSlug" is not null
                then 'child'
            when "parentSlug" is not null and "grandparentSlug" is null
                then 'parent'
            when "parentSlug" is null and "grandparentSlug" is null
                then 'grandparent'
            end as level,
          id,
          slug,
          "parentSlug",
          "grandparentSlug"
        from input_segment
      )
        select s.*
        from segments s
        join segment_level sl on (sl.level = 'child' and s.id = sl.id)
            or (sl.level = 'parent' and s."parentSlug" = sl.slug and s."grandparentSlug" is not null)
            or (sl.level = 'grandparent' and s."grandparentSlug" = sl.slug)
        where status = 'active';
      `,
      {
        replacements: {
          tenantId: this.options.currentTenant.id,
          segmentIds: segments,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return records
  }

  async fetchTenantActivityChannels(segmentIds: string[]) {
    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `
        SELECT
          "platform",
          json_agg(DISTINCT "channel") AS "channels"
        FROM "segmentActivityChannels"
        WHERE "tenantId" = :tenantId
        and "segmentId" in (:segmentIds)
        GROUP BY "platform";
      `,
      {
        replacements: {
          tenantId: this.options.currentTenant.id,
          segmentIds,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return records.reduce((acc, r) => {
      acc[r.platform] = r.channels
      return acc
    }, {})
  }

  async findBySlug(slug: string, level: SegmentLevel) {
    const transaction = this.transaction

    let findBySlugQuery = `SELECT * FROM segments WHERE slug = :slug AND "tenantId" = :tenantId`

    if (level === SegmentLevel.SUB_PROJECT) {
      findBySlugQuery += ` and "parentSlug" is not null and "grandparentSlug" is not null`
    } else if (level === SegmentLevel.PROJECT) {
      findBySlugQuery += ` and "parentSlug" is not null and "grandparentSlug" is null`
    } else if (level === SegmentLevel.PROJECT_GROUP) {
      findBySlugQuery += ` and "parentSlug" is null and "grandparentSlug" is null`
    }

    const records = await this.options.database.sequelize.query(findBySlugQuery, {
      replacements: {
        slug,
        tenantId: this.options.currentTenant.id,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (records.length === 0) {
      return null
    }

    return this.findById(records[0].id)
  }

  async findByName(name: string, level: SegmentLevel) {
    const transaction = this.transaction

    let findByNameQuery = `SELECT * FROM segments WHERE name = :name AND "tenantId" = :tenantId`

    if (level === SegmentLevel.SUB_PROJECT) {
      findByNameQuery += ` and "parentSlug" is not null and "grandparentSlug" is not null`
    } else if (level === SegmentLevel.PROJECT) {
      findByNameQuery += ` and "parentSlug" is not null and "grandparentSlug" is null`
    } else if (level === SegmentLevel.PROJECT_GROUP) {
      findByNameQuery += ` and "parentSlug" is null and "grandparentSlug" is null`
    }

    const records = await this.options.database.sequelize.query(findByNameQuery, {
      replacements: {
        name,
        tenantId: this.options.currentTenant.id,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    if (records.length === 0) {
      return null
    }

    return this.findById(records[0].id)
  }

  async findInIds(ids: string[]): Promise<SegmentData[]> {
    if (ids.length === 0) {
      return []
    }

    const transaction = this.transaction

    const records = await this.options.database.sequelize.query(
      `
        SELECT
          s.*
        FROM segments s
        WHERE id in (:ids)
        AND s."tenantId" = :tenantId
        GROUP BY s.id;
      `,
      {
        replacements: {
          ids,
          tenantId: this.options.currentTenant.id,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return records.map((sr) => populateSegmentRelations(sr))
  }

  async findByIds(ids: string[]) {
    const records = await this.options.database.sequelize.query(
      `
        SELECT
            s.*
        FROM segments s
        WHERE s."id" IN (:ids);
      `,
      {
        replacements: {
          ids,
        },
        type: QueryTypes.SELECT,
        raw: true,
      },
    )

    return records
  }

  /**
   * Query project groups with their children
   * @returns
   */
  async queryProjectGroups(criteria: QueryData): Promise<PageData<SegmentData>> {
    let searchQuery = 'WHERE 1=1'
    let segmentsSearchQuery = ''

    const replacements = {
      tenantId: this.currentTenant.id,
      name: `%${criteria.filter?.name}%`,
      status: criteria.filter?.status,
      adminSegments: null,
    }

    if (criteria.filter?.status) {
      searchQuery += `AND s.status = :status`
    }

    if (criteria.filter?.name) {
      searchQuery += `AND s.name ilike :name`
    }

    if (criteria.filter?.adminOnly) {
      const adminSegments = this.options.currentUser.tenants.flatMap((t) => t.adminSegments)
      if (adminSegments.length === 0) {
        return { count: 0, rows: [], limit: criteria.limit, offset: criteria.offset }
      }
      segmentsSearchQuery += `AND sp.id IN (:adminSegments)`
      replacements.adminSegments = adminSegments
    }

    const projectGroups = await this.options.database.sequelize.query(
      `
          WITH
              foundations AS (
                  SELECT
                      f.id AS foundation_id,
                      f.name AS foundation_name,
                      f.status,
                      f."createdAt",
                      f."updatedAt",
                      f."sourceId",
                      f."sourceParentId",
                      f.slug,
                      p.name AS project_name,
                      p.id AS project_id,
                      p.status AS project_status,
                      p.slug AS project_slug,
                      COUNT(DISTINCT sp.id) AS subproject_count,
                      JSONB_AGG(JSONB_BUILD_OBJECT(
                          'id', sp.id,
                          'name', sp.name,
                          'status', sp.status,
                          'slug', sp.slug
                          )) AS subprojects
                  FROM segments f
                  JOIN segments p
                      ON p."parentSlug" = f."slug"
                             AND p."grandparentSlug" IS NULL
                             AND p."tenantId" = f."tenantId"
                  JOIN segments sp
                      ON sp."parentSlug" = p."slug"
                             AND sp."grandparentSlug" = f.slug
                             AND sp."tenantId" = f."tenantId"
                  WHERE f."parentSlug" IS NULL
                    AND f."tenantId" = :tenantId
                    ${segmentsSearchQuery}
                  GROUP BY f."id", p.id
              )
          SELECT
              s.*,
              COUNT(*) OVER () AS "totalCount",
              JSONB_AGG(JSONB_BUILD_OBJECT(
                      'id', f.project_id,
                      'name', f.project_name,
                      'status', f.project_status,
                      'slug', f.project_slug,
                      'subprojects', f.subprojects
                  )) AS projects
          FROM segments s
          JOIN foundations f ON s.id = f.foundation_id
          ${searchQuery}
          GROUP BY s.id, f.foundation_name
          ORDER BY f.foundation_name
          ${this.getPaginationString(criteria)};
      `,
      {
        replacements,
        type: QueryTypes.SELECT,
      },
    )

    const count = projectGroups.length > 0 ? Number.parseInt(projectGroups[0].totalCount, 10) : 0

    const rows = projectGroups.map((i) => removeFieldsFromObject(i, 'totalCount'))

    return { count, rows, limit: criteria.limit, offset: criteria.offset }
  }

  async queryProjects(criteria: QueryData): Promise<PageData<SegmentData>> {
    let searchQuery = ''

    if (criteria.filter?.status) {
      searchQuery += ` AND s.status = :status`
    }

    if (criteria.filter?.name) {
      searchQuery += ` AND s.name ilike :name`
    }

    if (criteria.filter?.parentSlug) {
      searchQuery += ` AND s."parentSlug" = :parent_slug `
    }

    const projects = await this.options.database.sequelize.query(
      `
            SELECT
                s.*,
                COUNT(DISTINCT sp.id)                                       AS subproject_count,
                jsonb_agg(jsonb_build_object(
                    'id', sp.id,
                    'name', sp.name,
                    'status', sp.status,
                    'insightsProjectName', ip.name,
                    'insightsProjectId', ip.id
                )) as subprojects,
                count(*) over () as "totalCount"
            FROM segments s
                JOIN segments sp ON sp."parentSlug" = s."slug" and sp."grandparentSlug" is not null
                AND sp."tenantId" = s."tenantId"
                LEFT JOIN "insightsProjects" ip ON ip."segmentId" = sp.id
            WHERE
                s."grandparentSlug" IS NULL
            and s."parentSlug" is not null
            and s."tenantId" = :tenantId
            ${searchQuery}
            GROUP BY s."id"
            ORDER BY s."updatedAt" DESC
            ${this.getPaginationString(criteria)};
            `,
      {
        replacements: {
          tenantId: this.currentTenant.id,
          name: `%${criteria.filter?.name}%`,
          status: criteria.filter?.status,
          parent_slug: `${criteria.filter?.parentSlug}`,
        },
        type: QueryTypes.SELECT,
      },
    )

    const subprojects = projects.map((p) => p.subprojects).flat()
    const integrationsBySegments = await this.queryIntegrationsForSubprojects(subprojects)
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    const githubPlatforms = [PlatformType.GITHUB, PlatformType.GITHUB_NANGO]
    const mappedGithubReposBySegments = (
      await Promise.all(
        subprojects.map(async (s) => ({
          segmentId: s.id,
          hasMappedRepo: await hasMappedRepos(qx, s.id, githubPlatforms),
        })),
      )
    ).reduce((acc, { segmentId, hasMappedRepo }) => {
      if (hasMappedRepo) {
        acc[segmentId] = true
      }
      return acc
    }, {})

    const count = projects.length > 0 ? Number.parseInt(projects[0].totalCount, 10) : 0

    const rows = projects.map((i) => removeFieldsFromObject(i, 'totalCount'))

    // assign integrations to subprojects
    await Promise.all(
      rows.map(async (row) => {
        await Promise.all(
          row.subprojects.map(async (subproject) => {
            const integrations = integrationsBySegments[subproject.id] || []
            const githubIntegration = integrations.find((i) => i.platform === 'github')

            if (githubIntegration) {
              githubIntegration.type = 'primary'
            } else if (mappedGithubReposBySegments[subproject.id]) {
              integrations.push({
                platform: 'github',
                segmentId: subproject.id,
                type: 'mapped',
                mappedWith: await getMappedWithSegmentName(qx, subproject.id, githubPlatforms),
              })
            }

            subproject.integrations = integrations
          }),
        )
      }),
    )

    return { count, rows, limit: criteria.limit, offset: criteria.offset }
  }

  async getDefaultSegment() {
    const segments = await this.querySubprojects({ limit: 1, offset: 0 })
    return segments.rows[0] || null
  }

  async querySubprojects(criteria: QueryData): Promise<PageData<SegmentData>> {
    let searchQuery = ''

    if (criteria.filter?.status) {
      searchQuery += ` AND s.status = :status`
    }

    if (criteria.filter?.name) {
      searchQuery += ` AND s.name ilike :name`
    }

    if (criteria.filter?.parentSlug) {
      searchQuery += ` AND s."parentSlug" = :parent_slug `
    }

    if (criteria.filter?.grandparentSlug) {
      searchQuery += ` AND s."grandparentSlug" = :grandparent_slug `
    }

    const subprojects = await this.options.database.sequelize.query(
      `
        SELECT
          s.*
        FROM segments s
        WHERE s."grandparentSlug" IS NOT NULL
          AND s."parentSlug" IS NOT NULL
          AND s."tenantId" = :tenantId
          ${searchQuery}
        ORDER BY s.name
        ${this.getPaginationString(criteria)};
      `,
      {
        replacements: {
          tenantId: this.currentTenant.id,
          name: `%${criteria.filter?.name}%`,
          status: criteria.filter?.status,
          parent_slug: `${criteria.filter?.parentSlug}`,
          grandparent_slug: `${criteria.filter?.grandparentSlug}`,
          ids: criteria.filter?.ids,
        },
        type: QueryTypes.SELECT,
      },
    )

    const rows = subprojects

    return {
      count: 1,
      rows: rows.map((sr) => populateSegmentRelations(sr)),
      limit: criteria.limit,
      offset: criteria.offset,
    }
  }

  async querySubprojectsLite(criteria: QueryData): Promise<PageData<SegmentData>> {
    let searchQuery = ''

    if (criteria.filter?.status) {
      searchQuery += ` AND s.status = :status`
    }

    if (criteria.filter?.name) {
      searchQuery += ` AND s.name ilike :name`
    }

    if (criteria.filter?.parentSlug) {
      searchQuery += ` AND s."parentSlug" = :parent_slug `
    }

    if (criteria.filter?.grandparentSlug) {
      searchQuery += ` AND s."grandparentSlug" = :grandparent_slug `
    }

    const subprojects = await this.options.database.sequelize.query(
      `
        SELECT
          s.id,
          s.name,
          s.url,
          s.slug,
          s.description,
          COUNT(*) OVER () AS "totalCount"
        FROM segments s
        WHERE s."grandparentSlug" IS NOT NULL
          AND s."parentSlug" IS NOT NULL
          AND s."tenantId" = :tenantId
          ${searchQuery}
        ORDER BY s.name
        ${this.getPaginationString(criteria)};
      `,
      {
        replacements: {
          tenantId: this.currentTenant.id,
          name: `%${criteria.filter?.name}%`,
          status: criteria.filter?.status,
          parent_slug: `${criteria.filter?.parentSlug}`,
          grandparent_slug: `${criteria.filter?.grandparentSlug}`,
          ids: criteria.filter?.ids,
        },
        type: QueryTypes.SELECT,
      },
    )

    const rows = subprojects.map((i) => removeFieldsFromObject(i, 'totalCount'))
    const count = subprojects.length > 0 ? +subprojects[0].totalCount : 0
    return {
      count,
      rows,
      limit: criteria.limit,
      offset: criteria.offset,
    }
  }

  private async queryIntegrationsForSubprojects(subprojects) {
    const segmentIds = subprojects.map((i) => i.id)
    let { rows: integrations } = await IntegrationRepository.findAndCountAll(
      {
        advancedFilter: {
          segmentId: segmentIds,
        },
      },
      {
        ...this.options,
        currentSegments: subprojects,
      },
    )

    integrations = integrations.map(({ platform, id, status, segmentId }) => ({
      platform,
      id,
      status,
      segmentId,
    }))

    return lodash.groupBy(integrations, 'segmentId')
  }

  static getActivityTypes(options: IRepositoryOptions): ActivityTypeSettings {
    return getSegmentActivityTypes(options.currentSegments)
  }

  static async fetchTenantActivityTypes(options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const [record] = await options.database.sequelize.query(
      `
        SELECT
            jsonb_merge_agg(s."customActivityTypes") AS "customActivityTypes"
        FROM segments s
        WHERE s."grandparentSlug" IS NOT NULL
          AND s."parentSlug" IS NOT NULL
          AND s."tenantId" = :tenantId
          AND s."customActivityTypes" != '{}'
      `,
      {
        replacements: {
          tenantId: options.currentTenant.id,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return buildSegmentActivityTypes(record)
  }

  static activityTypeExists(platform: string, key: string, options: IRepositoryOptions): boolean {
    const activityTypes = this.getActivityTypes(options)

    if (
      (activityTypes.default[platform] && activityTypes.default[platform][key]) ||
      (activityTypes.custom[platform] && activityTypes.custom[platform][key])
    ) {
      return true
    }

    return false
  }

  async findBySourceIds(sourceIds: string[]) {
    const transaction = SequelizeRepository.getTransaction(this.options)
    const seq = SequelizeRepository.getSequelize(this.options)

    if (!sourceIds || !sourceIds.length) {
      return []
    }

    const segments = await seq.query(
      `
        SELECT
            DISTINCT UNNEST(ARRAY[s.id, s1.id, s2.id]) AS id
        FROM segments s
        JOIN segments s1 ON s1."parentSlug" = s.slug
        JOIN segments s2 ON s2."parentSlug" = s1.slug
        WHERE s."tenantId" = :tenantId
          AND s."sourceId" IN (:sourceIds)
      `,
      {
        replacements: { sourceIds, tenantId: this.options.currentTenant.id },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return segments.map((i: any) => i.id)
  }
}

export default SegmentRepository
