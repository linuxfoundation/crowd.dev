"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importStar(require("lodash"));
const sequelize_1 = require("sequelize");
const validator_1 = __importDefault(require("validator"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const lfx_memberships_1 = require("@crowd/data-access-layer/src/lfx_memberships");
const organizations_1 = require("@crowd/data-access-layer/src/organizations");
const attributesConfig_1 = require("@crowd/data-access-layer/src/organizations/attributesConfig");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const mergeSuggestionTypes_1 = require("@/types/mergeSuggestionTypes");
const organizationsQueryCache_1 = require("./organizationsQueryCache");
const segmentRepository_1 = __importDefault(require("./segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class OrganizationRepository {
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        if (!data.displayName) {
            data.displayName = data.identities[0].name;
        }
        const toInsert = {
            ...lodash_1.default.pick(data, [
                'displayName',
                'description',
                'headline',
                'logo',
                'importHash',
                'isTeamOrganization',
                'isAffiliationBlocked',
                'lastEnrichedAt',
                'manuallyCreated',
            ]),
            tenantId: tenant.id,
            createdById: currentUser.id,
            updatedById: currentUser.id,
        };
        const record = await options.database.organization.create(toInsert, {
            transaction,
        });
        // prepare attributes object
        const attributes = {};
        if (data.logo) {
            attributes.logo = {
                custom: [data.logo],
                default: data.logo,
            };
        }
        await this.updateOrgAttributes(record.id, { attributes }, options);
        await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.organizationCreateAction)(record.id, async (captureState) => {
            captureState(toInsert);
        }));
        await record.setMembers(data.members || [], {
            transaction,
        });
        if (data.identities && data.identities.length > 0) {
            await OrganizationRepository.setIdentities(record.id, data.identities, options);
        }
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        await (0, organizations_1.addOrgsToSegments)(qx, subprojectIds, [record.id]);
        return this.findById(record.id, options);
    }
    static async excludeOrganizationsFromSegments(organizationIds, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const bulkDeleteOrganizationSegments = `DELETE FROM "organizationSegments" WHERE "organizationId" in (:organizationIds) and "segmentId" in (:segmentIds);`;
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        if (subprojectIds.length === 0) {
            return;
        }
        await seq.query(bulkDeleteOrganizationSegments, {
            replacements: {
                organizationIds,
                segmentIds: subprojectIds,
            },
            type: sequelize_1.QueryTypes.DELETE,
            transaction,
        });
    }
    static async excludeOrganizationsFromAllSegments(organizationIds, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const bulkDeleteOrganizationSegments = `DELETE FROM "organizationSegments" WHERE "organizationId" in (:organizationIds);`;
        await seq.query(bulkDeleteOrganizationSegments, {
            replacements: {
                organizationIds,
            },
            type: sequelize_1.QueryTypes.DELETE,
            transaction,
        });
    }
    static convertOrgAttributesForInsert(data) {
        const orgAttributes = [];
        const defaultColumns = {};
        for (const [name, attribute] of Object.entries(data.attributes)) {
            const attributeDefinition = (0, attributesConfig_1.findAttribute)(name);
            if (!attribute.custom) {
                continue; // eslint-disable-line no-continue
            }
            for (const value of attribute.custom) {
                const isDefault = value === attribute.default;
                orgAttributes.push({
                    type: attributeDefinition.type,
                    name,
                    source: 'custom',
                    default: isDefault,
                    value,
                });
                if (isDefault && attributeDefinition.defaultColumn) {
                    defaultColumns[attributeDefinition.defaultColumn] = value;
                }
            }
        }
        return {
            orgAttributes,
            defaultColumns,
        };
    }
    static convertOrgAttributesForDisplay(attributes) {
        return attributes.reduce((acc, a) => {
            if (!acc[a.name]) {
                acc[a.name] = {};
            }
            if (!acc[a.name][a.source]) {
                acc[a.name][a.source] = [];
            }
            acc[a.name][a.source].push(a.value);
            if (a.default) {
                acc[a.name].default = a.value;
            }
            return acc;
        }, {});
    }
    static async updateOrgAttributes(organizationId, data, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const { orgAttributes, defaultColumns } = OrganizationRepository.convertOrgAttributesForInsert(data);
        await (0, organizations_1.upsertOrgAttributes)(qx, organizationId, orgAttributes);
        for (const attr of orgAttributes) {
            if (attr.default) {
                await (0, organizations_1.markOrgAttributeDefault)(qx, organizationId, attr);
            }
        }
        return defaultColumns;
    }
    static async update(id, data, options, overrideIdentities = false, manualChange = false) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const record = await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.organizationUpdateAction)(id, async (captureOldState, captureNewState) => {
            const record = await options.database.organization.findOne({
                where: {
                    id,
                    tenantId: currentTenant.id,
                },
                transaction,
            });
            if (!record) {
                throw new common_1.Error404();
            }
            captureOldState(record.get({ plain: true }));
            if (data.identities) {
                const primaryDomainIdentity = data.identities.find((i) => i.type === types_1.OrganizationIdentityType.PRIMARY_DOMAIN && i.verified);
                // check if domain already exists in another organization in the same tenant
                if (primaryDomainIdentity) {
                    const existingOrg = (await seq.query(`
          select "organizationId"
          from "organizationIdentities"
          where
            "tenantId" = :tenantId and
            "organizationId" <> :id and
            type = :type and
            value = :value and
            verified = true
          `, {
                        replacements: {
                            tenantId: currentTenant.id,
                            id: record.id,
                            type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                            value: primaryDomainIdentity.value,
                        },
                        type: sequelize_1.QueryTypes.SELECT,
                        transaction,
                    }));
                    // ensure that it's not the same organization
                    if (existingOrg && existingOrg.length > 0) {
                        throw new common_1.Error409(options.language, 'errors.alreadyExists', existingOrg[0].organizationId);
                    }
                }
            }
            if (data.attributes) {
                const defaultColumns = await OrganizationRepository.updateOrgAttributes(record.id, data, options);
                for (const col of Object.keys(defaultColumns)) {
                    data[col] = defaultColumns[col];
                }
            }
            const updatedData = {
                ...lodash_1.default.pick(data, this.ORGANIZATION_UPDATE_COLUMNS),
                updatedById: currentUser.id,
            };
            captureNewState(updatedData);
            await options.database.organization.update(updatedData, {
                where: {
                    id: record.id,
                },
                transaction,
            });
            return record;
        }), !manualChange);
        if (data.members) {
            await record.setMembers(data.members || [], {
                transaction,
            });
        }
        if (data.isTeamOrganization === true ||
            data.isTeamOrganization === 'true' ||
            data.isTeamOrganization === false ||
            data.isTeamOrganization === 'false') {
            await this.setOrganizationIsTeam(record.id, data.isTeamOrganization, options);
        }
        if (data.segments) {
            const qx = sequelizeRepository_1.default.getQueryExecutor(options);
            const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
            const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
            await (0, organizations_1.addOrgsToSegments)(qx, subprojectIds, [record.id]);
        }
        await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.organizationEditIdentitiesAction)(id, async (captureOldState, captureNewState) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor(options);
            const initialIdentities = await (0, organizations_1.fetchOrgIdentities)(qx, id);
            function convertIdentitiesForAudit(identities) {
                return identities.reduce((acc, r) => {
                    if (!acc[r.platform]) {
                        acc[r.platform] = [];
                    }
                    acc[r.platform].push({
                        value: r.value,
                        type: r.type,
                        verified: r.verified,
                    });
                    acc[r.platform] = acc[r.platform].sort((a, b) => `${a.value}:${a.type}:${a.verified}`.localeCompare(`${b.value}:${b.type}:${b.verified}`));
                    return acc;
                }, {});
            }
            captureOldState(convertIdentitiesForAudit(initialIdentities));
            if (data.identities && data.identities.length > 0) {
                if (overrideIdentities) {
                    captureNewState(convertIdentitiesForAudit(data.identities.map((i) => ({
                        platform: i.platform,
                        value: i.value,
                        type: i.type,
                        verified: i.verified,
                    }))));
                    await this.setIdentities(id, data.identities, options);
                }
                else {
                    captureNewState(convertIdentitiesForAudit([...initialIdentities, ...data.identities]));
                    await OrganizationRepository.addIdentities(id, data.identities, options);
                }
            }
        }));
        return this.findById(record.id, options);
    }
    /**
     * Marks/unmarks an organization's members as team members
     * @param organizationId
     * @param isTeam
     * @param options
     */
    static async setOrganizationIsTeam(organizationId, isTeam, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        await options.database.sequelize.query(`update members as m
      set attributes = jsonb_set("attributes", '{isTeamMember}', '{"default": ${isTeam}}'::jsonb)
      from "memberOrganizations" as mo
      where mo."memberId" = m.id
      and mo."organizationId" = :organizationId
      and mo."deletedAt" is null
      and m."tenantId" = :tenantId;
   `, {
            replacements: {
                isTeam,
                organizationId,
                tenantId: options.currentTenant.id,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
    }
    static async destroy(id, options, force = false) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.organization.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        await OrganizationRepository.excludeOrganizationsFromAllSegments([id], {
            ...options,
            transaction,
        });
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, organizations_1.cleanupForOganization)(qx, id);
        await (0, organizations_1.deleteOrganizationAttributes)(qx, [id]);
        await record.destroy({
            transaction,
            force,
        });
    }
    static async setIdentities(organizationId, identities, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, organizations_1.cleanUpOrgIdentities)(qx, organizationId);
        await OrganizationRepository.addIdentities(organizationId, identities, options);
    }
    static async addIdentities(organizationId, identities, options) {
        for (const identity of identities) {
            await OrganizationRepository.addIdentity(organizationId, identity, options);
        }
    }
    static async updateIdentity(organizationId, identity, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, organizations_1.updateOrgIdentityVerifiedFlag)(qx, {
            organizationId,
            platform: identity.platform,
            value: identity.value,
            type: identity.type,
            verified: identity.verified,
        });
    }
    static async addIdentity(organizationId, identity, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, organizations_1.insertOrganizationIdentities)(qx, [
            {
                organizationId,
                platform: identity.platform,
                source: identity.source,
                sourceId: identity.sourceId || null,
                value: identity.value,
                type: identity.type,
                verified: identity.verified,
                integrationId: identity.integrationId || null,
            },
        ], false);
    }
    static async getIdentities(organizationIds, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const sequelize = sequelizeRepository_1.default.getSequelize(options);
        const results = await sequelize.query(`
      select "sourceId", "source", platform, value, type, verified, "integrationId", "organizationId" from "organizationIdentities"
      where "organizationId" in (:organizationIds)
    `, {
            replacements: {
                organizationIds,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    static async moveIdentitiesBetweenOrganizations(fromOrganizationId, toOrganizationId, identitiesToMove, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const query = `
      update "organizationIdentities"
      set
        "organizationId" = :newOrganizationId
      where
        "organizationId" = :oldOrganizationId and
        platform = :platform and
        value = :value and
        type = :type and
        verified = :verified;
    `;
        for (const identity of identitiesToMove) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const [_, count] = await seq.query(query, {
                replacements: {
                    oldOrganizationId: fromOrganizationId,
                    newOrganizationId: toOrganizationId,
                    platform: identity.platform,
                    value: identity.value,
                    type: identity.type,
                    verified: identity.verified,
                },
                type: sequelize_1.QueryTypes.UPDATE,
                transaction,
            });
            if (count !== 1) {
                throw new Error('One row should be updated!');
            }
        }
    }
    static async addNoMerge(organizationId, noMergeId, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const query = `
    insert into "organizationNoMerge" ("organizationId", "noMergeId", "createdAt", "updatedAt")
    values
    (:organizationId, :noMergeId, now(), now()),
    (:noMergeId, :organizationId, now(), now())
    on conflict do nothing;
  `;
        try {
            await seq.query(query, {
                replacements: {
                    organizationId,
                    noMergeId,
                },
                type: sequelize_1.QueryTypes.INSERT,
                transaction,
            });
        }
        catch (error) {
            options.log.error('Error adding organizations no merge!', error);
            throw error;
        }
    }
    static async removeToMerge(organizationId, toMergeId, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const query = `
    delete from "organizationToMerge"
    where ("organizationId" = :organizationId and "toMergeId" = :toMergeId) or ("organizationId" = :toMergeId and "toMergeId" = :organizationId);
  `;
        try {
            await seq.query(query, {
                replacements: {
                    organizationId,
                    toMergeId,
                },
                type: sequelize_1.QueryTypes.DELETE,
                transaction,
            });
        }
        catch (error) {
            options.log.error('Error while removing organizations to merge!', error);
            throw error;
        }
    }
    static async findNonExistingIds(ids, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        let idValues = ``;
        for (let i = 0; i < ids.length; i++) {
            idValues += `('${ids[i]}'::uuid)`;
            if (i !== ids.length - 1) {
                idValues += ',';
            }
        }
        const query = `WITH id_list (id) AS (
      VALUES
          ${idValues}
        )
        SELECT id
        FROM id_list
        WHERE NOT EXISTS (
            SELECT 1
            FROM organizations o
            WHERE o.id = id_list.id
        );`;
        try {
            const results = await seq.query(query, {
                type: sequelize_1.QueryTypes.SELECT,
                transaction,
            });
            return results.map((r) => r.id);
        }
        catch (error) {
            options.log.error('error while getting non existing organizations from db', error);
            throw error;
        }
    }
    static async countOrganizationMergeSuggestions(organizationFilter, similarityFilter, displayNameFilter, replacements, options) {
        var _a;
        const organizationsJoin = displayNameFilter
            ? `JOIN organizations o1 ON o1.id = otm."organizationId"
         JOIN organizations o2 ON o2.id = otm."toMergeId"`
            : '';
        const result = await options.database.sequelize.query(`
      SELECT COUNT(*) AS total_count
      FROM "organizationToMerge" otm
      ${organizationsJoin}
      WHERE EXISTS (
          SELECT 1 FROM "organizationSegmentsAgg" os1
          WHERE os1."organizationId" = otm."organizationId" AND os1."segmentId" IN (:segmentIds)
      )
      AND EXISTS (
          SELECT 1 FROM "organizationSegmentsAgg" os2
          WHERE os2."organizationId" = otm."toMergeId" AND os2."segmentId" IN (:segmentIds)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM "mergeActions" ma
        WHERE ma.type = :mergeActionType
          AND ma.state <> :mergeActionState
          AND (
            (ma."primaryId" = otm."organizationId" AND ma."secondaryId" = otm."toMergeId")
            OR (ma."primaryId" = otm."toMergeId" AND ma."secondaryId" = otm."organizationId")
          )
      )
        ${organizationFilter}
        ${similarityFilter}
        ${displayNameFilter}
      `, {
            replacements: {
                ...replacements,
                mergeActionType: types_1.MergeActionType.ORG,
                mergeActionState: types_1.MergeActionState.ERROR,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        return ((_a = result[0]) === null || _a === void 0 ? void 0 : _a.total_count) || 0;
    }
    static async findOrganizationsWithMergeSuggestions(args, options) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t;
        const HIGH_CONFIDENCE_LOWER_BOUND = 0.9;
        const MEDIUM_CONFIDENCE_LOWER_BOUND = 0.7;
        // Organization segments are aggregated at each hierarchy level (group -> project -> subproject).
        const projectGroupSegment = sequelizeRepository_1.default.getStrictlySingleProjectGroupSegment(options);
        let segmentIds;
        if ((_b = (_a = args.filter) === null || _a === void 0 ? void 0 : _a.projectIds) === null || _b === void 0 ? void 0 : _b.length) {
            segmentIds = args.filter.projectIds;
        }
        else if ((_d = (_c = args.filter) === null || _c === void 0 ? void 0 : _c.subprojectIds) === null || _d === void 0 ? void 0 : _d.length) {
            segmentIds = args.filter.subprojectIds;
        }
        else {
            segmentIds = [projectGroupSegment.id];
        }
        let similarityFilter = '';
        const similarityConditions = [];
        for (const similarity of ((_e = args.filter) === null || _e === void 0 ? void 0 : _e.similarity) || []) {
            if (similarity === mergeSuggestionTypes_1.SimilarityScoreRange.HIGH) {
                similarityConditions.push(`(otm.similarity >= ${HIGH_CONFIDENCE_LOWER_BOUND})`);
            }
            else if (similarity === mergeSuggestionTypes_1.SimilarityScoreRange.MEDIUM) {
                similarityConditions.push(`(otm.similarity >= ${MEDIUM_CONFIDENCE_LOWER_BOUND} and otm.similarity < ${HIGH_CONFIDENCE_LOWER_BOUND})`);
            }
            else if (similarity === mergeSuggestionTypes_1.SimilarityScoreRange.LOW) {
                similarityConditions.push(`(otm.similarity < ${MEDIUM_CONFIDENCE_LOWER_BOUND})`);
            }
        }
        if (similarityConditions.length > 0) {
            similarityFilter = ` and (${similarityConditions.join(' or ')})`;
        }
        const organizationFilter = ((_f = args.filter) === null || _f === void 0 ? void 0 : _f.organizationId)
            ? ` AND ("otm"."organizationId" = :organizationId OR "otm"."toMergeId" = :organizationId)`
            : '';
        const displayNameFilter = ((_g = args.filter) === null || _g === void 0 ? void 0 : _g.displayName)
            ? ` and (o1."displayName" ilike :displayName OR o2."displayName" ilike :displayName)`
            : '';
        let order = 'otm.similarity desc, otm."organizationId", otm."toMergeId"';
        if (((_h = args.orderBy) === null || _h === void 0 ? void 0 : _h.length) > 0) {
            order = '';
            for (const orderBy of args.orderBy) {
                const [field, direction] = orderBy.split('_');
                if (['similarity'].includes(field) && ['asc', 'desc'].includes(direction.toLowerCase())) {
                    order += `otm.${field} ${direction}, `;
                }
            }
            order += 'otm."organizationId", otm."toMergeId"';
        }
        const hasProjectFilter = Boolean(((_k = (_j = args.filter) === null || _j === void 0 ? void 0 : _j.projectIds) === null || _k === void 0 ? void 0 : _k.length) || ((_m = (_l = args.filter) === null || _l === void 0 ? void 0 : _l.subprojectIds) === null || _m === void 0 ? void 0 : _m.length));
        const hasCountFilters = Boolean(((_o = args.filter) === null || _o === void 0 ? void 0 : _o.organizationId) || ((_p = args.filter) === null || _p === void 0 ? void 0 : _p.displayName) || ((_r = (_q = args.filter) === null || _q === void 0 ? void 0 : _q.similarity) === null || _r === void 0 ? void 0 : _r.length));
        const getTotalCount = async () => {
            var _a, _b, _c;
            if (!hasCountFilters && !hasProjectFilter) {
                const counts = await (0, segments_1.getSegmentMergeSuggestionCounts)(sequelizeRepository_1.default.getQueryExecutor(options), projectGroupSegment.id);
                return (_a = counts === null || counts === void 0 ? void 0 : counts.organizationMergeSuggestionsCount) !== null && _a !== void 0 ? _a : 0;
            }
            return this.countOrganizationMergeSuggestions(organizationFilter, similarityFilter, displayNameFilter, {
                segmentIds,
                displayName: ((_b = args === null || args === void 0 ? void 0 : args.filter) === null || _b === void 0 ? void 0 : _b.displayName) ? `${args.filter.displayName}%` : undefined,
                organizationId: (_c = args === null || args === void 0 ? void 0 : args.filter) === null || _c === void 0 ? void 0 : _c.organizationId,
            }, options);
        };
        if (args.countOnly) {
            return { count: await getTotalCount() };
        }
        const pageLimit = args.limit;
        const queryLimit = pageLimit + 1;
        const orgs = await options.database.sequelize.query(`
        SELECT
          otm."organizationId" AS id,
          otm."toMergeId",
          otm.similarity,
          o1."displayName" as "primaryDisplayName",
          o1.logo as "primaryLogo",
          o2."displayName" as "secondaryDisplayName",
          o2.logo as "secondaryLogo",
          (SELECT os1."segmentId" FROM "organizationSegmentsAgg" os1
           WHERE os1."organizationId" = otm."organizationId" AND os1."segmentId" IN (:segmentIds)
           LIMIT 1) as "primarySegmentId",
          (SELECT os2."segmentId" FROM "organizationSegmentsAgg" os2
           WHERE os2."organizationId" = otm."toMergeId" AND os2."segmentId" IN (:segmentIds)
           LIMIT 1) as "secondarySegmentId"
        FROM "organizationToMerge" otm
        JOIN organizations o1 ON o1.id = otm."organizationId"
        JOIN organizations o2 ON o2.id = otm."toMergeId"
        WHERE EXISTS (
            SELECT 1 FROM "organizationSegmentsAgg" os1
            WHERE os1."organizationId" = otm."organizationId" AND os1."segmentId" IN (:segmentIds)
        )
        AND EXISTS (
            SELECT 1 FROM "organizationSegmentsAgg" os2
            WHERE os2."organizationId" = otm."toMergeId" AND os2."segmentId" IN (:segmentIds)
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "mergeActions" ma
          WHERE ma.type = :mergeActionType
            AND ma.state <> :mergeActionState
            AND (
              (ma."primaryId" = otm."organizationId" AND ma."secondaryId" = otm."toMergeId")
              OR (ma."primaryId" = otm."toMergeId" AND ma."secondaryId" = otm."organizationId")
            )
        )
          ${organizationFilter}
          ${similarityFilter}
          ${displayNameFilter}
        ORDER BY ${order}
        LIMIT :limit OFFSET :offset
      `, {
            replacements: {
                segmentIds,
                limit: queryLimit,
                offset: args.offset,
                displayName: ((_s = args === null || args === void 0 ? void 0 : args.filter) === null || _s === void 0 ? void 0 : _s.displayName) ? `${args.filter.displayName}%` : undefined,
                mergeActionType: types_1.MergeActionType.ORG,
                mergeActionState: types_1.MergeActionState.ERROR,
                organizationId: (_t = args === null || args === void 0 ? void 0 : args.filter) === null || _t === void 0 ? void 0 : _t.organizationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        const hasMore = orgs.length > pageLimit;
        const pageRows = hasMore ? orgs.slice(0, pageLimit) : orgs;
        if (pageRows.length > 0) {
            let result;
            if (args.detail) {
                const organizationPromises = [];
                const toMergePromises = [];
                for (const org of pageRows) {
                    organizationPromises.push(OrganizationRepository.findById(org.id, options, org.primarySegmentId));
                    toMergePromises.push(OrganizationRepository.findById(org.toMergeId, options, org.secondarySegmentId));
                }
                const organizationResults = await Promise.all(organizationPromises);
                const organizationToMergeResults = await Promise.all(toMergePromises);
                result = organizationResults.map((i, idx) => ({
                    organizations: [i, organizationToMergeResults[idx]],
                    similarity: pageRows[idx].similarity,
                }));
            }
            else {
                result = pageRows.map((o) => ({
                    organizations: [
                        {
                            id: o.id,
                            displayName: o.primaryDisplayName,
                            logo: o.primaryLogo,
                        },
                        {
                            id: o.toMergeId,
                            displayName: o.secondaryDisplayName,
                            logo: o.secondaryLogo,
                        },
                    ],
                    similarity: o.similarity,
                }));
            }
            const qx = sequelizeRepository_1.default.getQueryExecutor(options);
            const organizationIds = (0, lodash_1.uniq)(result.map((r) => r.organizations[0].id));
            const lfxMemberships = await (0, lfx_memberships_1.findManyLfxMemberships)(qx, {
                organizationIds,
            });
            result.forEach((r) => {
                r.organizations.forEach((org) => {
                    org.lfxMembership = lfxMemberships.find((m) => m.organizationId === org.id);
                });
            });
            return {
                rows: result,
                hasMore,
                limit: args.limit,
                offset: args.offset,
            };
        }
        return {
            rows: [{ organizations: [], similarity: 0 }],
            hasMore: false,
            limit: args.limit,
            offset: args.offset,
        };
    }
    static async getOrganizationSegments(organizationId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const segmentRepository = new segmentRepository_1.default(options);
        const query = `
        SELECT "segmentId"
        FROM "organizationSegments"
        WHERE "organizationId" = :organizationId
        ORDER BY "createdAt";
    `;
        const data = await seq.query(query, {
            replacements: {
                organizationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        const segmentIds = data.map((item) => item.segmentId);
        const segments = await segmentRepository.findInIds(segmentIds);
        return segments;
    }
    static async findByVerifiedIdentities(identities, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const foundOrgs = await (0, organizations_1.queryOrgIdentities)(qx, {
            fields: [organizations_1.OrgIdentityField.ORGANIZATION_ID],
            filter: {
                or: identities.map((identity) => ({
                    and: [
                        { platform: { eq: identity.platform } },
                        { value: { eq: identity.value } },
                        { type: { eq: identity.type } },
                        { verified: { eq: true } },
                    ],
                })),
            },
        });
        if (foundOrgs.length === 0) {
            return null;
        }
        const foundOrgsIdentities = await (0, organizations_1.fetchManyOrgIdentities)(qx, foundOrgs.map((o) => o.organizationId));
        const orgIdWithMostIdentities = foundOrgsIdentities.sort((a, b) => b.identities.length - a.identities.length)[0].organizationId;
        const result = await (0, organizations_1.findOrgById)(qx, orgIdWithMostIdentities, [
            organizations_1.OrganizationField.ID,
            organizations_1.OrganizationField.DISPLAY_NAME,
            organizations_1.OrganizationField.DESCRIPTION,
            organizations_1.OrganizationField.LOGO,
            organizations_1.OrganizationField.TAGS,
            organizations_1.OrganizationField.EMPLOYEES,
            organizations_1.OrganizationField.REVENUE_RANGE,
            organizations_1.OrganizationField.IMPORT_HASH,
            organizations_1.OrganizationField.LOCATION,
            organizations_1.OrganizationField.TYPE,
            organizations_1.OrganizationField.SIZE,
            organizations_1.OrganizationField.HEADLINE,
            organizations_1.OrganizationField.INDUSTRY,
            organizations_1.OrganizationField.FOUNDED,
            organizations_1.OrganizationField.IS_TEAM_ORGANIZATION,
            organizations_1.OrganizationField.IS_AFFILIATION_BLOCKED,
            organizations_1.OrganizationField.MANUALLY_CREATED,
        ]);
        return result;
    }
    static async findById(id, options, segmentId) {
        let orgResponse = null;
        orgResponse = await OrganizationRepository.findAndCountAll({
            filter: { id: { eq: id } },
            limit: 1,
            offset: 0,
            segmentId,
            include: {
                aggregates: true,
                attributes: true,
                lfxMemberships: true,
                identities: true,
                segments: true,
            },
        }, options);
        if (orgResponse.count === 0) {
            // try it again without segment information (no aggregates)
            // for orgs without activities
            orgResponse = await OrganizationRepository.findAndCountAll({
                filter: { id: { eq: id } },
                limit: 1,
                offset: 0,
                include: {
                    aggregates: false,
                    attributes: true,
                    lfxMemberships: true,
                    identities: true,
                    segments: true,
                },
            }, options);
            if (orgResponse.count === 0) {
                throw new common_1.Error404();
            }
            orgResponse.rows[0].joinedAt = null;
            orgResponse.rows[0].lastActive = null;
            orgResponse.rows[0].activityCount = 0;
            orgResponse.rows[0].memberCount = 0;
            orgResponse.rows[0].avgContributorEngagement = null;
            orgResponse.rows[0].activeOn = null;
        }
        const organization = orgResponse.rows[0];
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const attributes = await (0, organizations_1.findOrgAttributes)(qx, id);
        organization.attributes = OrganizationRepository.convertOrgAttributesForDisplay(attributes);
        return organization;
    }
    static async destroyBulk(ids, options, force = false) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        await OrganizationRepository.excludeOrganizationsFromSegments(ids, {
            ...options,
            transaction,
        });
        await options.database.organization.destroy({
            where: {
                id: ids,
                tenantId: currentTenant.id,
            },
            force,
            transaction,
        });
    }
    static async count(filter, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        return options.database.organization.count({
            where: {
                ...filter,
                tenantId: tenant.id,
            },
            transaction,
        });
    }
    static removeLfxMembershipFromFilters(filtersArray, index, filterName) {
        var _a;
        const lfxFilterObj = (_a = Object.assign(filtersArray[filterName][index])) === null || _a === void 0 ? void 0 : _a.lfxMembership;
        filtersArray[filterName].splice(index, 1);
        if (filtersArray[filterName].length === 0)
            // edge case when "lfxMembership" is the only filter
            delete filtersArray[filterName];
        return lfxFilterObj;
    }
    static handleLfxMembershipFilter(filter) {
        if (!filter) {
            return { lfxMembershipFilter: null, updatedfilter: filter };
        }
        let lfxMembershipFilter = null;
        const updatedfilter = Object.assign(filter);
        // handle nested "and" filters \\ "or" inside "and"
        if (updatedfilter.and && Array.isArray(updatedfilter.and))
            for (let i = 0; i < updatedfilter.and.length; i++) {
                if (Object.hasOwn(updatedfilter.and[i], 'lfxMembership')) {
                    lfxMembershipFilter = this.removeLfxMembershipFromFilters(updatedfilter, i, 'and');
                    return { lfxMembershipFilter, updatedfilter };
                }
                if (Object.hasOwn(updatedfilter.and[i], 'and') ||
                    Object.hasOwn(updatedfilter.and[i], 'or')) {
                    const result = this.handleLfxMembershipFilter(updatedfilter.and[i]);
                    lfxMembershipFilter = result.lfxMembershipFilter;
                }
            }
        // "or" filters cannot be nested, we can only have "or" inside parent "and" filter
        if (updatedfilter.or && Array.isArray(updatedfilter.or))
            for (let i = 0; i < updatedfilter.or.length; i++)
                if (Object.hasOwn(updatedfilter.or[i], 'lfxMembership')) {
                    lfxMembershipFilter = this.removeLfxMembershipFromFilters(updatedfilter, i, 'or');
                    return { lfxMembershipFilter, updatedfilter };
                }
        return { lfxMembershipFilter, updatedfilter };
    }
    static async findAndCountAll({ countOnly = false, fields = [...OrganizationRepository.QUERY_FILTER_COLUMN_MAP.keys()], filter = {}, include = {
        identities: true,
        lfxMemberships: true,
        segments: false,
        attributes: false,
    }, limit = 20, offset = 0, orderBy = undefined, search = undefined, segmentId = undefined, }, options) {
        // Initialize cache
        const cache = new organizationsQueryCache_1.OrganizationQueryCache(options.redis);
        // Build cache key
        const cacheKey = organizationsQueryCache_1.OrganizationQueryCache.buildCacheKey({
            countOnly,
            fields,
            filter,
            include,
            limit,
            offset,
            orderBy,
            search,
            segmentId,
        });
        // Try to get from cache first
        const cachedResult = countOnly ? null : await cache.get(cacheKey);
        const cachedCount = countOnly ? await cache.getCount(cacheKey) : null;
        if (cachedResult) {
            this.refreshCacheInBackground(cache, cacheKey, {
                filter,
                search,
                limit,
                offset,
                orderBy,
                segmentId,
                countOnly: false,
                fields,
                include,
            }, options);
            options.log.info(`Organizations advanced query cache hit: ${cacheKey}`);
            return cachedResult;
        }
        if (countOnly && cachedCount !== null) {
            this.refreshCountCacheInBackground(cache, cacheKey, {
                filter,
                search,
                segmentId,
                include,
            }, options);
            options.log.info(`Organizations advanced count query cache hit: ${cacheKey}`);
            return {
                rows: [],
                count: cachedCount,
                limit,
                offset,
            };
        }
        return this.executeQuery(cache, cacheKey, {
            filter,
            search,
            limit,
            offset,
            orderBy,
            segmentId,
            countOnly,
            fields,
            include,
        }, options);
    }
    static async executeQuery(cache, cacheKey, { filter = {}, search = undefined, limit = 20, offset = 0, orderBy = undefined, segmentId = undefined, fields = [...OrganizationRepository.QUERY_FILTER_COLUMN_MAP.keys()], include = {
        identities: true,
        lfxMemberships: true,
        segments: false,
        attributes: false,
    }, countOnly = false, }, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const withAggregates = include.aggregates;
        const { lfxMembershipFilter, updatedfilter } = OrganizationRepository.handleLfxMembershipFilter(filter);
        filter = updatedfilter; // updated filter without lfxMembershipFilter
        let lfxMembershipFilterWhereClause = '';
        if (lfxMembershipFilter) {
            const filterKey = Object.keys(lfxMembershipFilter)[0];
            if (filterKey === 'ne') {
                lfxMembershipFilterWhereClause = `AND EXISTS (SELECT 1 FROM "lfxMemberships" lm WHERE lm."organizationId" = o.id AND lm."tenantId" = $(tenantId))`;
            }
            else if (filterKey === 'eq') {
                lfxMembershipFilterWhereClause = `AND NOT EXISTS (SELECT 1 FROM "lfxMemberships" lm WHERE lm."organizationId" = o.id AND lm."tenantId" = $(tenantId))`;
            }
        }
        if (segmentId) {
            const segment = (await (0, segments_1.findSegmentById)((0, sequelizeQueryExecutor_1.optionsQx)(options), segmentId));
            if (segment === null) {
                options.log.info('No segment found for organization');
                return {
                    rows: [],
                    count: 0,
                    limit,
                    offset,
                };
            }
            segmentId = segment.id;
        }
        const params = {
            limit,
            offset,
            segmentId,
            tenantId: options.currentTenant.id,
        };
        let searchWhereClause = '';
        if (search) {
            params.searchTerm = `%${search}%`;
            searchWhereClause = `AND o."displayName" ILIKE $(searchTerm)`;
        }
        const filterString = common_1.RawQueryParser.parseFilters(filter, OrganizationRepository.QUERY_FILTER_COLUMN_MAP, [], params, { pgPromiseFormat: true });
        const order = (function prepareOrderBy(orderBy = withAggregates ? 'lastActive_DESC' : 'id_DESC') {
            const orderSplit = orderBy.split('_');
            const orderField = OrganizationRepository.QUERY_FILTER_COLUMN_MAP.get(orderSplit[0]);
            if (!orderField) {
                return withAggregates ? 'osa."lastActive" DESC' : 'o.id DESC';
            }
            const orderDirection = ['DESC', 'ASC'].includes(orderSplit[1]) ? orderSplit[1] : 'DESC';
            return `${orderField} ${orderDirection}`;
        })(orderBy !== null && orderBy !== void 0 ? orderBy : 'id_DESC');
        const createQuery = (fields) => `
      SELECT
        ${fields}
      FROM organizations o
      LEFT JOIN "organizationSegmentsAgg" osa ON osa."organizationId" = o.id AND ${segmentId ? `osa."segmentId" = $(segmentId)` : `osa."segmentId" IS NULL`}
      LEFT JOIN "organizationEnrichments" oe ON oe."organizationId" = o.id
      WHERE 1=1
        AND o."tenantId" = $(tenantId)
        ${lfxMembershipFilterWhereClause}
        ${searchWhereClause}
        AND (${filterString})
    `;
        const countQuery = createQuery('COUNT(*)');
        if (countOnly) {
            const result = await qx.selectOne(countQuery, params);
            const count = parseInt(result.count, 10);
            // Cache the count
            await cache.setCount(cacheKey, count, 21600); // 6 hours TTL
            return {
                rows: [],
                count,
                limit,
                offset,
            };
        }
        const query = `
          ${createQuery((function prepareFields(fields) {
            return fields
                .map((f) => {
                const mappedField = OrganizationRepository.QUERY_FILTER_COLUMN_MAP.get(f);
                if (!mappedField) {
                    throw new common_1.Error400(options.language, `Invalid field: ${f}`);
                }
                return `${mappedField} as "${f}"`;
            })
                .filter((f) => {
                if (withAggregates) {
                    return true;
                }
                return !f.includes('osa.');
            })
                .join(',\n');
        })(fields))}
          ORDER BY ${order} NULLS LAST
          LIMIT $(limit)
          OFFSET $(offset)
        `;
        const results = await Promise.all([qx.select(query, params), qx.selectOne(countQuery, params)]);
        const rows = results[0];
        const count = parseInt(results[1].count, 10);
        const orgIds = rows.map((org) => org.id);
        if (orgIds.length === 0) {
            return { rows: [], count: 0, limit, offset };
        }
        if (include.lfxMemberships) {
            const lfxMemberships = await (0, lfx_memberships_1.findManyLfxMemberships)(qx, {
                organizationIds: orgIds,
            });
            rows.forEach((org) => {
                const membership = lfxMemberships.find((lm) => lm.organizationId === org.id);
                org.lfxMembership = !!membership;
            });
        }
        if (include.identities) {
            const identities = await (0, organizations_1.fetchManyOrgIdentities)(qx, orgIds);
            rows.forEach((org) => {
                var _a;
                const orgIdentities = ((_a = identities.find((i) => i.organizationId === org.id)) === null || _a === void 0 ? void 0 : _a.identities) || [];
                org.identities = orgIdentities.map((identity) => ({
                    type: identity.type,
                    value: identity.value,
                    platform: identity.platform,
                    verified: identity.verified,
                }));
            });
        }
        if (include.segments) {
            const orgSegments = await (0, organizations_1.fetchManyOrgSegments)(qx, orgIds);
            rows.forEach((org) => {
                var _a;
                org.segments =
                    ((_a = orgSegments
                        .find((i) => i.organizationId === org.id)) === null || _a === void 0 ? void 0 : _a.segments.filter((segment) => segment !== null)) || [];
            });
        }
        if (include.attributes) {
            const attributes = await (0, organizations_1.findManyOrgAttributes)(qx, orgIds);
            rows.forEach((org) => {
                var _a;
                org.attributes = ((_a = attributes.find((a) => a.organizationId === org.id)) === null || _a === void 0 ? void 0 : _a.attributes) || [];
            });
        }
        const result = { rows, count, limit, offset };
        await cache.set(cacheKey, result, 21600); // 6 hours TTL
        return result;
    }
    static async findAllAutocomplete(query, limit, options) {
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        const records = await options.database.sequelize.query(`
        SELECT
            DISTINCT
            o."id",
            o."displayName" AS label,
            o."logo",
            o."displayName" ILIKE :queryExact AS exact
        FROM "organizations" AS o
        JOIN "organizationSegments" os ON os."organizationId" = o.id
        WHERE o."deletedAt" IS NULL
          AND o."tenantId" = :tenantId
          AND (o."displayName" ILIKE :queryLike OR o.id = :uuid)
          AND os."segmentId" IN (:segmentIds)
          AND os."tenantId" = :tenantId
        ORDER BY o."displayName" ILIKE :queryExact DESC, o."displayName"
        LIMIT :limit;
      `, {
            replacements: {
                limit: limit ? Number(limit) : 20,
                tenantId: tenant.id,
                segmentIds: subprojectIds,
                queryLike: `%${query}%`,
                queryExact: query,
                uuid: validator_1.default.isUUID(query) ? query : null,
            },
            type: sequelize_1.QueryTypes.SELECT,
            raw: true,
        });
        return records;
    }
    static async refreshCacheInBackground(cache, cacheKey, params, options) {
        try {
            await this.executeQuery(cache, cacheKey, params, options);
        }
        catch (error) {
            options.log.warn('Background cache refresh failed:', error);
        }
    }
    static async refreshCountCacheInBackground(cache, cacheKey, params, options) {
        try {
            options.log.info(`Refreshing organizations advanced count cache in background: ${cacheKey}`);
            await this.executeQuery(cache, cacheKey, {
                ...params,
                countOnly: true,
                fields: [...OrganizationRepository.QUERY_FILTER_COLUMN_MAP.keys()],
                limit: 20,
                offset: 0,
            }, options);
        }
        catch (error) {
            options.log.warn('Background count cache refresh failed:', error);
        }
    }
    static async findByIds(ids, options) {
        const records = await options.database.sequelize.query(`
        SELECT
            o."id",
            o."displayName",
            o."logo"
        FROM "organizations" AS o
        WHERE o."id" IN (:ids);
      `, {
            replacements: {
                ids,
            },
            type: sequelize_1.QueryTypes.SELECT,
            raw: true,
        });
        return records;
    }
    static calculateRenderFriendlyOrganizations(memberOrganizations) {
        const organizations = [];
        for (const role of memberOrganizations) {
            organizations.push({
                id: role.organizationId,
                displayName: role.organizationName,
                logo: role.organizationLogo,
                memberOrganizations: role,
            });
        }
        return organizations;
    }
    static async getActivityCountInPlatform(organizationId, platform, options) {
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const activityTypes = segmentRepository_1.default.getActivityTypes(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        const result = await (0, data_access_layer_1.queryActivities)({
            segmentIds: subprojectIds,
            countOnly: true,
            filter: {
                and: [
                    {
                        organizationId: {
                            eq: organizationId,
                        },
                        platform: {
                            eq: platform,
                        },
                    },
                ],
            },
        }, qx, activityTypes);
        return result.count;
    }
    static async getMemberCountInPlatform(organizationId, platform, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const rows = await (0, data_access_layer_1.queryActivityRelations)(qx, {
            filter: {
                and: [
                    {
                        organizationId: {
                            eq: organizationId,
                        },
                        platform: {
                            eq: platform,
                        },
                    },
                ],
            },
            countOnly: true,
        });
        return rows.count;
    }
    static async removeIdentitiesFromOrganization(organizationId, identities, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const query = `
      delete from "organizationIdentities" where "organizationId" = :organizationId and platform = :platform and value = :value and type = :type;
    `;
        for (const identity of identities) {
            await seq.query(query, {
                replacements: {
                    organizationId,
                    value: identity.value,
                    type: identity.type,
                    platform: identity.platform,
                },
                type: sequelize_1.QueryTypes.DELETE,
                transaction,
            });
        }
    }
}
OrganizationRepository.QUERY_FILTER_COLUMN_MAP = new Map([
    // id fields
    ['id', 'o.id'],
    ['segmentId', 'osa."segmentId"'],
    // basic fields for filtering
    ['size', 'o.size'],
    ['industry', 'o.industry'],
    ['employees', 'o."employees"'],
    ['founded', 'o."founded"'],
    ['headline', 'o."headline"'],
    ['location', 'o."location"'],
    ['country', 'o."country"'],
    ['tags', 'o."tags"'],
    ['type', 'o."type"'],
    ['isTeamOrganization', 'o."isTeamOrganization"'],
    ['isAffiliationBlocked', 'o."isAffiliationBlocked"'],
    // basic fields for querying
    ['displayName', 'o."displayName"'],
    ['revenueRange', 'o."revenueRange"'],
    ['employeeGrowthRate', 'o."employeeGrowthRate"'],
    // derived fields
    ['employeeChurnRate12Month', `(o."employeeChurnRate"->>'12_month')::decimal`],
    ['employeeGrowthRate12Month', `(o."employeeGrowthRate"->>'12_month')::decimal`],
    ['revenueRangeMin', `(o."revenueRange"->>'min')::integer`],
    ['revenueRangeMax', `(o."revenueRange"->>'max')::integer`],
    // aggregated fields
    ['activityCount', 'coalesce(osa."activityCount", 0)::integer'],
    ['memberCount', 'coalesce(osa."memberCount", 0)::integer'],
    ['activeOn', 'coalesce(osa."activeOn", \'{}\'::text[])'],
    ['joinedAt', 'osa."joinedAt"'],
    ['lastActive', 'osa."lastActive"'],
    ['avgContributorEngagement', 'coalesce(osa."avgContributorEngagement", 0)::integer'],
    // org fields for display
    ['logo', 'o."logo"'],
    ['description', 'o."description"'],
    // enrichment
    ['lastEnrichedAt', 'oe."lastUpdatedAt"'],
]);
OrganizationRepository.ORGANIZATION_UPDATE_COLUMNS = [
    'importHash',
    'isTeamOrganization',
    'isAffiliationBlocked',
    'headline',
    'lastEnrichedAt',
    // default attributes
    'type',
    'industry',
    'founded',
    'size',
    'employees',
    'displayName',
    'description',
    'logo',
    'tags',
    'location',
    'country',
    'employees',
    'revenueRange',
    'employeeChurnRate',
    'employeeGrowthRate',
];
OrganizationRepository.isEqual = {
    displayName: (a, b) => a === b,
    description: (a, b) => a === b,
    emails: (a, b) => lodash_1.default.isEqual((a || []).sort(), (b || []).sort()),
    phoneNumbers: (a, b) => lodash_1.default.isEqual((a || []).sort(), (b || []).sort()),
    logo: (a, b) => a === b,
    location: (a, b) => a === b,
    country: (a, b) => a === b,
    isTeamOrganization: (a, b) => a === b,
    isAffiliationBlocked: (a, b) => a === b,
    attributes: (a, b) => lodash_1.default.isEqual(a, b),
};
exports.default = OrganizationRepository;
//# sourceMappingURL=organizationRepository.js.map