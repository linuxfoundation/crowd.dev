"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = require("sequelize");
const uuid_1 = require("uuid");
const common_1 = require("@crowd/common");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const types_1 = require("@crowd/types");
const getObjectWithoutKey_1 = __importDefault(require("../../utils/getObjectWithoutKey"));
const integrationRepository_1 = __importDefault(require("./integrationRepository"));
const repositoryBase_1 = require("./repositoryBase");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class SegmentRepository extends repositoryBase_1.RepositoryBase {
    constructor(options) {
        super(options, true);
    }
    /**
     * Insert a segment.
     * @param data segment data
     * @returns
     */
    async create(data) {
        var _a;
        const transaction = this.transaction;
        const id = (0, uuid_1.v4)();
        await this.options.database.sequelize.query(`INSERT INTO "segments" ("id", "url", "name", "slug", "parentSlug", "grandparentSlug", "status", "parentName", "sourceId", "sourceParentId", "tenantId", "grandparentName", "parentId", "grandparentId", "isLF")
          VALUES
              (:id, :url, :name, :slug, :parentSlug, :grandparentSlug, :status, :parentName, :sourceId, :sourceParentId, :tenantId, :grandparentName, :parentId, :grandparentId, :isLF)
        `, {
            replacements: {
                id,
                url: data.url || null,
                name: data.name,
                parentName: data.parentName || null,
                grandparentName: data.grandparentName || null,
                slug: data.slug,
                parentSlug: data.parentSlug || null,
                grandparentSlug: data.grandparentSlug || null,
                status: data.status || types_1.SegmentStatus.ACTIVE,
                sourceId: data.sourceId || null,
                sourceParentId: data.sourceParentId || null,
                tenantId: this.options.currentTenant.id,
                parentId: data.parentId || null,
                grandparentId: data.grandparentId || null,
                isLF: (_a = data.isLF) !== null && _a !== void 0 ? _a : true,
            },
            type: sequelize_1.QueryTypes.INSERT,
            transaction,
        });
        const segment = await this.findById(id);
        return segment;
    }
    async findById(id) {
        return (0, segments_1.findSegmentById)(this.queryExecutor, id);
    }
    /**
     * Updates:
     * parent slugs of children => parentSlug, grandparentSlug
     * parent names of children => parentName, grandparentName
     * @param id
     * @param slug
     * @param name
     */
    async updateChildrenBulk(segment, data) {
        if ((0, segments_1.isSegmentProjectGroup)(segment)) {
            // update projects
            await this.updateBulk(segment.projects.map((p) => p.id), {
                parentName: data.name,
                parentSlug: data.slug,
            });
            const subprojectIds = segment.projects.reduce((acc, p) => {
                acc.push(...p.subprojects.map((sp) => sp.id));
                return acc;
            }, []);
            await this.updateBulk(subprojectIds, {
                grandparentSlug: data.slug,
                grandparentName: data.name,
            });
        }
        else if ((0, segments_1.isSegmentProject)(segment)) {
            // update subprojects
            await this.updateBulk(segment.subprojects.map((sp) => sp.id), {
                parentName: data.name,
                parentSlug: data.slug,
                isLF: data.isLF,
            });
        }
        return this.findById(segment.id);
    }
    async updateBulk(ids, data) {
        const transaction = this.transaction;
        // strip arbitrary fields
        const nullishValues = [undefined, null, '', NaN];
        const updateFields = Object.keys(data).filter((key) => !nullishValues.includes(data[key]) &&
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
            ].includes(key));
        let segmentUpdateQuery = `UPDATE segments SET `;
        const replacements = {};
        for (const field of updateFields) {
            segmentUpdateQuery += ` "${field}" = :${field} `;
            replacements[field] = data[field];
            if (updateFields[updateFields.length - 1] !== field) {
                segmentUpdateQuery += ', ';
            }
        }
        segmentUpdateQuery += ` WHERE id in (:ids) and "tenantId" = :tenantId returning id`;
        replacements.tenantId = this.options.currentTenant.id;
        replacements.ids = ids;
        const idsUpdated = await this.options.database.sequelize.query(segmentUpdateQuery, {
            replacements,
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        return idsUpdated;
    }
    async update(id, data) {
        const transaction = this.transaction;
        const segment = await this.findById(id);
        if (!segment) {
            throw new common_1.Error404();
        }
        // strip arbitrary fields
        const updateFields = Object.keys(data).filter((key) => [
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
        ].includes(key));
        if (updateFields.length > 0) {
            let segmentUpdateQuery = `UPDATE segments SET `;
            const replacements = {};
            for (const field of updateFields) {
                segmentUpdateQuery += ` "${field}" = :${field} `;
                replacements[field] = data[field];
                if (updateFields[updateFields.length - 1] !== field) {
                    segmentUpdateQuery += ', ';
                }
            }
            segmentUpdateQuery += ` WHERE id = :id and "tenantId" = :tenantId `;
            replacements.tenantId = this.options.currentTenant.id;
            replacements.id = id;
            if (replacements.customActivityTypes) {
                replacements.customActivityTypes = JSON.stringify(replacements.customActivityTypes);
            }
            await this.options.database.sequelize.query(segmentUpdateQuery, {
                replacements,
                type: sequelize_1.QueryTypes.UPDATE,
                transaction,
            });
        }
        return this.findById(id);
    }
    async addActivityChannel(segmentId, platform, channel) {
        const transaction = this.transaction;
        await this.options.database.sequelize.query(`
        INSERT INTO "segmentActivityChannels" ("tenantId", "segmentId", "platform", "channel")
        VALUES (:tenantId, :segmentId, :platform, :channel)
        ON CONFLICT DO NOTHING;
      `, {
            replacements: {
                tenantId: this.options.currentTenant.id,
                segmentId,
                platform,
                channel,
            },
            type: sequelize_1.QueryTypes.INSERT,
            transaction,
        });
    }
    async fetchActivityChannels(segmentId) {
        const transaction = this.transaction;
        const records = await this.options.database.sequelize.query(`
        SELECT
          "platform",
          json_agg(DISTINCT "channel") AS "channels"
        FROM "segmentActivityChannels"
        WHERE "tenantId" = :tenantId
          AND "segmentId" = :segmentId
        GROUP BY "platform";
      `, {
            replacements: {
                tenantId: this.options.currentTenant.id,
                segmentId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return records.reduce((acc, r) => {
            acc[r.platform] = r.channels;
            return acc;
        }, {});
    }
    async fetchTenantActivityChannels(segmentIds) {
        if (segmentIds.length === 0) {
            return {};
        }
        const transaction = this.transaction;
        const records = await this.options.database.sequelize.query(`
        SELECT
          "platform",
          json_agg(DISTINCT "channel") AS "channels"
        FROM "segmentActivityChannels"
        WHERE "tenantId" = :tenantId
        and "segmentId" in (:segmentIds)
        GROUP BY "platform";
      `, {
            replacements: {
                tenantId: this.options.currentTenant.id,
                segmentIds,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return records.reduce((acc, r) => {
            acc[r.platform] = r.channels;
            return acc;
        }, {});
    }
    async findBySlug(slug, level) {
        const transaction = this.transaction;
        let findBySlugQuery = `SELECT * FROM segments WHERE slug = :slug AND "tenantId" = :tenantId`;
        if (level === types_1.SegmentLevel.SUB_PROJECT) {
            findBySlugQuery += ` and "parentSlug" is not null and "grandparentSlug" is not null`;
        }
        else if (level === types_1.SegmentLevel.PROJECT) {
            findBySlugQuery += ` and "parentSlug" is not null and "grandparentSlug" is null`;
        }
        else if (level === types_1.SegmentLevel.PROJECT_GROUP) {
            findBySlugQuery += ` and "parentSlug" is null and "grandparentSlug" is null`;
        }
        const records = await this.options.database.sequelize.query(findBySlugQuery, {
            replacements: {
                slug,
                tenantId: this.options.currentTenant.id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (records.length === 0) {
            return null;
        }
        return this.findById(records[0].id);
    }
    async findByName(name, level) {
        const transaction = this.transaction;
        let findByNameQuery = `SELECT * FROM segments WHERE name = :name AND "tenantId" = :tenantId`;
        if (level === types_1.SegmentLevel.SUB_PROJECT) {
            findByNameQuery += ` and "parentSlug" is not null and "grandparentSlug" is not null`;
        }
        else if (level === types_1.SegmentLevel.PROJECT) {
            findByNameQuery += ` and "parentSlug" is not null and "grandparentSlug" is null`;
        }
        else if (level === types_1.SegmentLevel.PROJECT_GROUP) {
            findByNameQuery += ` and "parentSlug" is null and "grandparentSlug" is null`;
        }
        const records = await this.options.database.sequelize.query(findByNameQuery, {
            replacements: {
                name,
                tenantId: this.options.currentTenant.id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (records.length === 0) {
            return null;
        }
        return this.findById(records[0].id);
    }
    async findInIds(ids) {
        if (ids.length === 0) {
            return [];
        }
        const transaction = this.transaction;
        const records = await this.options.database.sequelize.query(`
        SELECT
          s.*
        FROM segments s
        WHERE id in (:ids)
        AND s."tenantId" = :tenantId
        GROUP BY s.id;
      `, {
            replacements: {
                ids,
                tenantId: this.options.currentTenant.id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return records.map((sr) => (0, segments_1.populateSegmentRelations)(sr));
    }
    async findByIds(ids) {
        const records = await this.options.database.sequelize.query(`
        SELECT
            s.*
        FROM segments s
        WHERE s."id" IN (:ids);
      `, {
            replacements: {
                ids,
            },
            type: sequelize_1.QueryTypes.SELECT,
            raw: true,
        });
        return records;
    }
    /**
     * Query project groups with their children
     * @returns
     */
    async queryProjectGroups(criteria) {
        var _a, _b, _c, _d, _e;
        let searchQuery = 'WHERE 1=1';
        let segmentsSearchQuery = '';
        const replacements = {
            tenantId: this.currentTenant.id,
            name: `%${(_a = criteria.filter) === null || _a === void 0 ? void 0 : _a.name}%`,
            status: (_b = criteria.filter) === null || _b === void 0 ? void 0 : _b.status,
            adminSegments: null,
        };
        if ((_c = criteria.filter) === null || _c === void 0 ? void 0 : _c.status) {
            searchQuery += `AND s.status = :status`;
        }
        if ((_d = criteria.filter) === null || _d === void 0 ? void 0 : _d.name) {
            searchQuery += `AND s.name ilike :name`;
        }
        if ((_e = criteria.filter) === null || _e === void 0 ? void 0 : _e.adminOnly) {
            const adminSegments = this.options.currentUser.tenants.flatMap((t) => t.adminSegments);
            if (adminSegments.length === 0) {
                return { count: 0, rows: [], limit: criteria.limit, offset: criteria.offset };
            }
            segmentsSearchQuery += `AND EXISTS (
        SELECT 1 FROM segments sp
        WHERE sp."grandparentSlug" = f.slug
          AND sp."tenantId" = f."tenantId"
          AND sp.id IN (:adminSegments)
      )`;
            replacements.adminSegments = adminSegments;
        }
        const projectGroups = await this.options.database.sequelize.query(`
          WITH
              foundations AS (
                  SELECT
                      f.id AS foundation_id,
                      f.name AS foundation_name,
                      COUNT(DISTINCT p.id)::int AS project_count
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
                  GROUP BY f.id
              )
          SELECT
              s.*,
              COUNT(*) OVER () AS "totalCount",
              f.project_count AS "projectCount"
          FROM segments s
          JOIN foundations f ON s.id = f.foundation_id
          ${searchQuery}
          ORDER BY f.foundation_name
          ${this.getPaginationString(criteria)};
      `, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
        });
        const count = projectGroups.length > 0 ? Number.parseInt(projectGroups[0].totalCount, 10) : 0;
        const rows = projectGroups.map((i) => (0, getObjectWithoutKey_1.default)(i, 'totalCount'));
        return { count, rows, limit: criteria.limit, offset: criteria.offset };
    }
    async queryProjects(criteria) {
        var _a, _b, _c, _d, _e, _f;
        let searchQuery = '';
        if ((_a = criteria.filter) === null || _a === void 0 ? void 0 : _a.status) {
            searchQuery += ` AND s.status = :status`;
        }
        if ((_b = criteria.filter) === null || _b === void 0 ? void 0 : _b.name) {
            searchQuery += ` AND s.name ilike :name`;
        }
        if ((_c = criteria.filter) === null || _c === void 0 ? void 0 : _c.parentSlug) {
            searchQuery += ` AND s."parentSlug" = :parent_slug `;
        }
        const projects = await this.options.database.sequelize.query(`
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
            `, {
            replacements: {
                tenantId: this.currentTenant.id,
                name: `%${(_d = criteria.filter) === null || _d === void 0 ? void 0 : _d.name}%`,
                status: (_e = criteria.filter) === null || _e === void 0 ? void 0 : _e.status,
                parent_slug: `${(_f = criteria.filter) === null || _f === void 0 ? void 0 : _f.parentSlug}`,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        const subprojects = projects.map((p) => p.subprojects).flat();
        const integrationsBySegments = await this.queryIntegrationsForSubprojects(subprojects);
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const githubPlatforms = [types_1.PlatformType.GITHUB, types_1.PlatformType.GITHUB_NANGO];
        const mappedGithubReposBySegments = (await Promise.all(subprojects.map(async (s) => ({
            segmentId: s.id,
            hasMappedRepo: await (0, segments_1.hasMappedRepos)(qx, s.id, githubPlatforms),
        })))).reduce((acc, { segmentId, hasMappedRepo }) => {
            if (hasMappedRepo) {
                acc[segmentId] = true;
            }
            return acc;
        }, {});
        const count = projects.length > 0 ? Number.parseInt(projects[0].totalCount, 10) : 0;
        const rows = projects.map((i) => (0, getObjectWithoutKey_1.default)(i, 'totalCount'));
        // assign integrations to subprojects
        await Promise.all(rows.map(async (row) => {
            await Promise.all(row.subprojects.map(async (subproject) => {
                const integrations = integrationsBySegments[subproject.id] || [];
                const githubIntegration = integrations.find((i) => i.platform === 'github');
                if (githubIntegration) {
                    githubIntegration.type = 'primary';
                }
                else if (mappedGithubReposBySegments[subproject.id]) {
                    integrations.push({
                        platform: 'github',
                        segmentId: subproject.id,
                        type: 'mapped',
                        mappedWith: await (0, segments_1.getMappedWithSegmentName)(qx, subproject.id, githubPlatforms),
                    });
                }
                subproject.integrations = integrations;
            }));
        }));
        return { count, rows, limit: criteria.limit, offset: criteria.offset };
    }
    async getDefaultSegment() {
        const segments = await this.querySubprojects({ limit: 1, offset: 0 });
        return segments.rows[0] || null;
    }
    async querySubprojects(criteria) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j;
        let searchQuery = '';
        if ((_a = criteria.filter) === null || _a === void 0 ? void 0 : _a.status) {
            searchQuery += ` AND s.status = :status`;
        }
        if ((_b = criteria.filter) === null || _b === void 0 ? void 0 : _b.name) {
            searchQuery += ` AND s.name ilike :name`;
        }
        if ((_c = criteria.filter) === null || _c === void 0 ? void 0 : _c.parentSlug) {
            searchQuery += ` AND s."parentSlug" = :parent_slug `;
        }
        if ((_d = criteria.filter) === null || _d === void 0 ? void 0 : _d.grandparentSlug) {
            searchQuery += ` AND s."grandparentSlug" = :grandparent_slug `;
        }
        const subprojects = await this.options.database.sequelize.query(`
        SELECT
          s.*
        FROM segments s
        WHERE s."grandparentSlug" IS NOT NULL
          AND s."parentSlug" IS NOT NULL
          AND s."tenantId" = :tenantId
          ${searchQuery}
        ORDER BY s.name
        ${this.getPaginationString(criteria)};
      `, {
            replacements: {
                tenantId: this.currentTenant.id,
                name: `%${(_e = criteria.filter) === null || _e === void 0 ? void 0 : _e.name}%`,
                status: (_f = criteria.filter) === null || _f === void 0 ? void 0 : _f.status,
                parent_slug: `${(_g = criteria.filter) === null || _g === void 0 ? void 0 : _g.parentSlug}`,
                grandparent_slug: `${(_h = criteria.filter) === null || _h === void 0 ? void 0 : _h.grandparentSlug}`,
                ids: (_j = criteria.filter) === null || _j === void 0 ? void 0 : _j.ids,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        const rows = subprojects;
        return {
            count: 1,
            rows: rows.map((sr) => (0, segments_1.populateSegmentRelations)(sr)),
            limit: criteria.limit,
            offset: criteria.offset,
        };
    }
    async querySubprojectsLite(criteria) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j;
        let searchQuery = '';
        if ((_a = criteria.filter) === null || _a === void 0 ? void 0 : _a.status) {
            searchQuery += ` AND s.status = :status`;
        }
        if ((_b = criteria.filter) === null || _b === void 0 ? void 0 : _b.name) {
            searchQuery += ` AND s.name ilike :name`;
        }
        if ((_c = criteria.filter) === null || _c === void 0 ? void 0 : _c.parentSlug) {
            searchQuery += ` AND s."parentSlug" = :parent_slug `;
        }
        if ((_d = criteria.filter) === null || _d === void 0 ? void 0 : _d.grandparentSlug) {
            searchQuery += ` AND s."grandparentSlug" = :grandparent_slug `;
        }
        const subprojects = await this.options.database.sequelize.query(`
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
      `, {
            replacements: {
                tenantId: this.currentTenant.id,
                name: `%${(_e = criteria.filter) === null || _e === void 0 ? void 0 : _e.name}%`,
                status: (_f = criteria.filter) === null || _f === void 0 ? void 0 : _f.status,
                parent_slug: `${(_g = criteria.filter) === null || _g === void 0 ? void 0 : _g.parentSlug}`,
                grandparent_slug: `${(_h = criteria.filter) === null || _h === void 0 ? void 0 : _h.grandparentSlug}`,
                ids: (_j = criteria.filter) === null || _j === void 0 ? void 0 : _j.ids,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        const rows = subprojects.map((i) => (0, getObjectWithoutKey_1.default)(i, 'totalCount'));
        const count = subprojects.length > 0 ? +subprojects[0].totalCount : 0;
        return {
            count,
            rows,
            limit: criteria.limit,
            offset: criteria.offset,
        };
    }
    async queryIntegrationsForSubprojects(subprojects) {
        const segmentIds = subprojects.map((i) => i.id);
        let { rows: integrations } = await integrationRepository_1.default.findAndCountAll({
            advancedFilter: {
                segmentId: segmentIds,
            },
        }, {
            ...this.options,
            currentSegments: subprojects,
        });
        integrations = integrations.map(({ platform, id, status, segmentId }) => ({
            platform,
            id,
            status,
            segmentId,
        }));
        return lodash_1.default.groupBy(integrations, 'segmentId');
    }
    static getActivityTypes(options) {
        return (0, segments_1.getSegmentActivityTypes)(options.currentSegments);
    }
    static async fetchTenantActivityTypes(options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const [record] = await options.database.sequelize.query(`
        SELECT
            jsonb_merge_agg(s."customActivityTypes") AS "customActivityTypes"
        FROM segments s
        WHERE s."grandparentSlug" IS NOT NULL
          AND s."parentSlug" IS NOT NULL
          AND s."tenantId" = :tenantId
          AND s."customActivityTypes" != '{}'
      `, {
            replacements: {
                tenantId: options.currentTenant.id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return (0, segments_1.buildSegmentActivityTypes)(record);
    }
    static activityTypeExists(platform, key, options) {
        const activityTypes = this.getActivityTypes(options);
        if ((activityTypes.default[platform] && activityTypes.default[platform][key]) ||
            (activityTypes.custom[platform] && activityTypes.custom[platform][key])) {
            return true;
        }
        return false;
    }
    async findBySourceIds(sourceIds) {
        const transaction = sequelizeRepository_1.default.getTransaction(this.options);
        const seq = sequelizeRepository_1.default.getSequelize(this.options);
        if (!sourceIds || !sourceIds.length) {
            return [];
        }
        const segments = await seq.query(`
        SELECT
            DISTINCT UNNEST(ARRAY[s.id, s1.id, s2.id]) AS id
        FROM segments s
        JOIN segments s1 ON s1."parentSlug" = s.slug
        JOIN segments s2 ON s2."parentSlug" = s1.slug
        WHERE s."tenantId" = :tenantId
          AND s."sourceId" IN (:sourceIds)
      `, {
            replacements: { sourceIds, tenantId: this.options.currentTenant.id },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return segments.map((i) => i.id);
    }
}
exports.default = SegmentRepository;
//# sourceMappingURL=segmentRepository.js.map