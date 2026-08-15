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
const sequelize_1 = __importStar(require("sequelize"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const lfx_memberships_1 = require("@crowd/data-access-layer/src/lfx_memberships");
const maintainers_1 = require("@crowd/data-access-layer/src/maintainers");
const member_merge_1 = require("@crowd/data-access-layer/src/member_merge");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const members_1 = require("@crowd/data-access-layer/src/members");
const segments_1 = require("@crowd/data-access-layer/src/members/segments");
const segments_2 = require("@crowd/data-access-layer/src/segments");
const integrations_1 = require("@crowd/integrations");
const types_1 = require("@crowd/types");
const conf_1 = require("@/conf");
const configTypes_1 = require("@/conf/configTypes");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const mergeSuggestionTypes_1 = require("@/types/mergeSuggestionTypes");
const memberAttributeSettingsRepository_1 = __importDefault(require("./memberAttributeSettingsRepository"));
const segmentRepository_1 = __importDefault(require("./segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const tenantRepository_1 = __importDefault(require("./tenantRepository"));
const memberTypes_1 = require("./types/memberTypes");
const { Op } = sequelize_1.default;
class MemberRepository {
    static async create(data, options) {
        var _a;
        if (!data.username && !data.identities) {
            throw new Error('Username not set when creating member!');
        }
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const botDetectionService = new common_services_1.BotDetectionService(options.log);
        const botDetection = botDetectionService.isMemberBot(data.identities, data.attributes || {}, data.displayName);
        if (botDetection === types_1.MemberBotDetection.CONFIRMED_BOT) {
            options.log.debug({ memberIdentities: data.identities }, 'Member confirmed as bot!');
            const existingIsBot = ((_a = data.attributes) === null || _a === void 0 ? void 0 : _a.isBot) || {};
            // add default and system flags only if no active flag exists
            if (!Object.values(existingIsBot).some(Boolean)) {
                if (!data.attributes) {
                    data.attributes = {};
                }
                // When bot detection confirms a bot, set system flag and don't preserve custom flag
                // Custom flag should only be set when user manually marks as bot, not when system detects it
                data.attributes.isBot = { default: true, system: true };
            }
        }
        const toInsert = {
            ...lodash_1.default.pick(data, [
                'id',
                'displayName',
                'attributes',
                'emails',
                'enrichedBy',
                'contributions',
                'score',
                'reach',
                'joinedAt',
                'manuallyCreated',
                'importHash',
            ]),
            tenantId: common_1.DEFAULT_TENANT_ID,
            createdById: currentUser.id,
            updatedById: currentUser.id,
        };
        const record = await options.database.member.create(toInsert, {
            transaction,
        });
        await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.memberCreateAction)(record.id, async (captureNewState) => {
            captureNewState(toInsert);
        }));
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_2.getSegmentSubprojectIds)(qx, currentSegments);
        if (data.identities) {
            await (0, data_access_layer_1.insertMemberIdentities)(qx, data.identities.map((i) => ({
                memberId: record.id,
                platform: i.platform,
                type: i.type,
                value: i.value,
                sourceId: i.sourceId || null,
                integrationId: i.integrationId || null,
                verified: i.verified,
                source: i.source,
            })));
        }
        else if (data.username) {
            const username = (0, memberTypes_1.mapUsernameToIdentities)(data.username);
            const identitiesToInsert = [];
            for (const platform of Object.keys(username)) {
                const identities = username[platform];
                for (const identity of identities) {
                    identitiesToInsert.push({
                        memberId: record.id,
                        platform,
                        value: identity.value ? identity.value : identity.username,
                        type: identity.type ? identity.type : types_1.MemberIdentityType.USERNAME,
                        verified: true,
                        sourceId: identity.sourceId || null,
                        integrationId: identity.integrationId || null,
                        source: identity.source || 'ui',
                    });
                }
            }
            if (identitiesToInsert.length > 0) {
                await (0, data_access_layer_1.insertMemberIdentities)(qx, identitiesToInsert);
            }
        }
        await (0, segments_1.includeMemberToSegments)(qx, record.id, subprojectIds);
        const memberService = new common_services_1.CommonMemberService((0, sequelizeQueryExecutor_1.optionsQx)(options), options.temporal, options.log);
        await memberService.updateMemberOrganizations(record.id, data.organizations, true, subprojectIds, options);
        await record.setNoMerge(data.noMerge || [], {
            transaction,
        });
        await record.setToMerge(data.toMerge || [], {
            transaction,
        });
        if (data.affiliations) {
            await this.setAffiliations(record.id, data.affiliations, options);
        }
        if (botDetection === types_1.MemberBotDetection.SUSPECTED_BOT) {
            options.log.debug({ memberId: record.id }, 'Member suspected as bot, running LLM check!');
            await options.temporal.workflow.start('processMemberBotAnalysisWithLLM', {
                taskQueue: 'profiles',
                workflowId: `${types_1.TemporalWorkflowId.MEMBER_BOT_ANALYSIS_WITH_LLM}/${record.id}`,
                retry: {
                    maximumAttempts: 10,
                },
                args: [{ memberId: record.id }],
                searchAttributes: {
                    TenantId: [common_1.DEFAULT_TENANT_ID],
                },
            });
        }
        return this.findById(record.id, options);
    }
    static async excludeMembersFromSegments(memberIds, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const bulkDeleteMemberSegments = `DELETE FROM "memberSegments" WHERE "memberId" in (:memberIds) and "segmentId" in (:segmentIds);`;
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_2.getSegmentSubprojectIds)(qx, currentSegments);
        if (subprojectIds.length === 0) {
            return;
        }
        await seq.query(bulkDeleteMemberSegments, {
            replacements: {
                memberIds,
                segmentIds: subprojectIds,
            },
            type: sequelize_1.QueryTypes.DELETE,
            transaction,
        });
    }
    static async countMemberMergeSuggestions(memberFilter, similarityFilter, displayNameFilter, replacements, options) {
        var _a;
        const membersJoin = displayNameFilter
            ? `JOIN members m ON m.id = mtm."memberId"
         JOIN members m2 ON m2.id = mtm."toMergeId"`
            : '';
        const totalCount = await options.database.sequelize.query(`
        SELECT
            COUNT(*) AS count
        FROM "memberToMerge" mtm
        ${membersJoin}
        WHERE EXISTS (
            SELECT 1 FROM "memberSegmentsAgg" ms
            WHERE ms."memberId" = mtm."memberId" AND ms."segmentId" IN (:segmentIds)
        )
        AND EXISTS (
            SELECT 1 FROM "memberSegmentsAgg" ms2
            WHERE ms2."memberId" = mtm."toMergeId" AND ms2."segmentId" IN (:segmentIds)
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "mergeActions" ma
          WHERE ma.type = :mergeActionType
            AND ma.state <> :mergeActionState
            AND (
              (ma."primaryId" = mtm."memberId" AND ma."secondaryId" = mtm."toMergeId")
              OR (ma."primaryId" = mtm."toMergeId" AND ma."secondaryId" = mtm."memberId")
            )
        )
          ${memberFilter}
          ${similarityFilter}
          ${displayNameFilter}
      `, {
            replacements: {
                ...replacements,
                mergeActionType: types_1.MergeActionType.MEMBER,
                mergeActionState: types_1.MergeActionState.ERROR,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        return ((_a = totalCount[0]) === null || _a === void 0 ? void 0 : _a.count) || 0;
    }
    static async findMembersWithMergeSuggestions(args, options) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t;
        const HIGH_CONFIDENCE_LOWER_BOUND = 0.9;
        const MEDIUM_CONFIDENCE_LOWER_BOUND = 0.7;
        // Member segments are aggregated at each hierarchy level (group -> project -> subproject).
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
                similarityConditions.push(`(mtm.similarity >= ${HIGH_CONFIDENCE_LOWER_BOUND})`);
            }
            else if (similarity === mergeSuggestionTypes_1.SimilarityScoreRange.MEDIUM) {
                similarityConditions.push(`(mtm.similarity >= ${MEDIUM_CONFIDENCE_LOWER_BOUND} and mtm.similarity < ${HIGH_CONFIDENCE_LOWER_BOUND})`);
            }
            else if (similarity === mergeSuggestionTypes_1.SimilarityScoreRange.LOW) {
                similarityConditions.push(`(mtm.similarity < ${MEDIUM_CONFIDENCE_LOWER_BOUND})`);
            }
        }
        if (similarityConditions.length > 0) {
            similarityFilter = ` and (${similarityConditions.join(' or ')})`;
        }
        const memberFilter = ((_f = args.filter) === null || _f === void 0 ? void 0 : _f.memberId)
            ? ` and (mtm."memberId" = :memberId OR mtm."toMergeId" = :memberId)`
            : '';
        const displayNameFilter = ((_g = args.filter) === null || _g === void 0 ? void 0 : _g.displayName)
            ? ` and (m."displayName" ilike :displayName OR m2."displayName" ilike :displayName)`
            : '';
        let order = 'mtm."activityEstimate" desc, mtm.similarity desc, mtm."memberId", mtm."toMergeId"';
        if (((_h = args.orderBy) === null || _h === void 0 ? void 0 : _h.length) > 0) {
            order = '';
            for (const orderBy of args.orderBy) {
                const [field, direction] = orderBy.split('_');
                if (['similarity', 'activityEstimate'].includes(field) &&
                    ['asc', 'desc'].includes(direction.toLowerCase())) {
                    order += `mtm.${field} ${direction}, `;
                }
            }
            order += 'mtm."memberId", mtm."toMergeId"';
        }
        const hasProjectFilter = Boolean(((_k = (_j = args.filter) === null || _j === void 0 ? void 0 : _j.projectIds) === null || _k === void 0 ? void 0 : _k.length) || ((_m = (_l = args.filter) === null || _l === void 0 ? void 0 : _l.subprojectIds) === null || _m === void 0 ? void 0 : _m.length));
        const hasCountFilters = Boolean(((_o = args.filter) === null || _o === void 0 ? void 0 : _o.memberId) || ((_p = args.filter) === null || _p === void 0 ? void 0 : _p.displayName) || ((_r = (_q = args.filter) === null || _q === void 0 ? void 0 : _q.similarity) === null || _r === void 0 ? void 0 : _r.length));
        const getTotalCount = async () => {
            var _a, _b, _c;
            if (!hasCountFilters && !hasProjectFilter) {
                const counts = await (0, segments_2.getSegmentMergeSuggestionCounts)(sequelizeRepository_1.default.getQueryExecutor(options), projectGroupSegment.id);
                return (_a = counts === null || counts === void 0 ? void 0 : counts.memberMergeSuggestionsCount) !== null && _a !== void 0 ? _a : 0;
            }
            return this.countMemberMergeSuggestions(memberFilter, similarityFilter, displayNameFilter, {
                segmentIds,
                displayName: ((_b = args === null || args === void 0 ? void 0 : args.filter) === null || _b === void 0 ? void 0 : _b.displayName) ? `${args.filter.displayName}%` : undefined,
                memberId: (_c = args === null || args === void 0 ? void 0 : args.filter) === null || _c === void 0 ? void 0 : _c.memberId,
            }, options);
        };
        if (args.countOnly) {
            return { count: await getTotalCount() };
        }
        const pageLimit = args.limit;
        const queryLimit = pageLimit + 1;
        const mems = await options.database.sequelize.query(`
        SELECT
            mtm."memberId" AS id,
            mtm."toMergeId",
            mtm.similarity,
            mtm."activityEstimate",
            m."displayName" as "primaryDisplayName",
            m.attributes->'avatarUrl'->>'default' as "primaryAvatarUrl",
            m2."displayName" as "toMergeDisplayName",
            m2.attributes->'avatarUrl'->>'default' as "toMergeAvatarUrl"
        FROM "memberToMerge" mtm
        JOIN members m ON m.id = mtm."memberId"
        JOIN members m2 ON m2.id = mtm."toMergeId"
        WHERE EXISTS (
            SELECT 1 FROM "memberSegmentsAgg" ms
            WHERE ms."memberId" = mtm."memberId" AND ms."segmentId" IN (:segmentIds)
        )
        AND EXISTS (
            SELECT 1 FROM "memberSegmentsAgg" ms2
            WHERE ms2."memberId" = mtm."toMergeId" AND ms2."segmentId" IN (:segmentIds)
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "mergeActions" ma
          WHERE ma.type = :mergeActionType
            AND ma.state <> :mergeActionState
            AND (
              (ma."primaryId" = mtm."memberId" AND ma."secondaryId" = mtm."toMergeId")
              OR (ma."primaryId" = mtm."toMergeId" AND ma."secondaryId" = mtm."memberId")
            )
        )
          ${memberFilter}
          ${similarityFilter}
          ${displayNameFilter}
        ORDER BY ${order}
        LIMIT :limit
        OFFSET :offset
      `, {
            replacements: {
                segmentIds,
                limit: queryLimit,
                offset: args.offset,
                displayName: ((_s = args === null || args === void 0 ? void 0 : args.filter) === null || _s === void 0 ? void 0 : _s.displayName) ? `${args.filter.displayName}%` : undefined,
                memberId: (_t = args === null || args === void 0 ? void 0 : args.filter) === null || _t === void 0 ? void 0 : _t.memberId,
                mergeActionType: types_1.MergeActionType.MEMBER,
                mergeActionState: types_1.MergeActionState.ERROR,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        const hasMore = mems.length > pageLimit;
        const pageRows = hasMore ? mems.slice(0, pageLimit) : mems;
        if (pageRows.length > 0) {
            let result;
            if (args.detail) {
                const memberPromises = [];
                const toMergePromises = [];
                const findMemberInfo = async (memberId) => {
                    const qx = sequelizeRepository_1.default.getQueryExecutor(options);
                    const [member, identities, aggregates, memberOrgs] = await Promise.all([
                        (0, members_1.findMemberById)(qx, memberId, [
                            members_1.MemberField.ID,
                            members_1.MemberField.DISPLAY_NAME,
                            members_1.MemberField.ATTRIBUTES,
                            members_1.MemberField.JOINED_AT,
                        ]),
                        (0, members_1.fetchMemberIdentities)(qx, memberId),
                        (0, segments_1.fetchAbsoluteMemberAggregates)(qx, memberId),
                        (0, members_1.fetchMemberOrganizations)(qx, memberId),
                    ]);
                    const orgIds = memberOrgs.map((o) => o.organizationId);
                    let orgExtraInfo = [];
                    let lfxMemberships = [];
                    if (orgIds.length > 0) {
                        orgExtraInfo = await (0, data_access_layer_1.queryOrgs)(qx, {
                            filter: {
                                [data_access_layer_1.OrganizationField.ID]: { in: orgIds },
                            },
                            fields: [
                                data_access_layer_1.OrganizationField.ID,
                                data_access_layer_1.OrganizationField.DISPLAY_NAME,
                                data_access_layer_1.OrganizationField.LOGO,
                            ],
                        });
                        lfxMemberships = await (0, lfx_memberships_1.findManyLfxMemberships)(qx, {
                            organizationIds: orgIds,
                        });
                    }
                    return {
                        ...member,
                        identities,
                        ...{
                            activityCount: aggregates === null || aggregates === void 0 ? void 0 : aggregates.activityCount,
                            lastActive: aggregates === null || aggregates === void 0 ? void 0 : aggregates.lastActive,
                        },
                        organizations: memberOrgs.map((o) => ({
                            ...orgExtraInfo.find((oei) => oei.id === o.organizationId),
                            lfxMembership: lfxMemberships.find((lm) => lm.organizationId === o.organizationId),
                            memberOrganizations: o,
                        })),
                    };
                };
                for (const mem of pageRows) {
                    memberPromises.push(findMemberInfo(mem.id));
                    toMergePromises.push(findMemberInfo(mem.toMergeId));
                }
                const memberResults = await Promise.all(memberPromises);
                const memberToMergeResults = await Promise.all(toMergePromises);
                result = memberResults.map((i, idx) => ({
                    members: [i, memberToMergeResults[idx]],
                    similarity: pageRows[idx].similarity,
                }));
            }
            else {
                result = pageRows.map((i) => ({
                    members: [
                        {
                            id: i.id,
                            displayName: i.primaryDisplayName,
                            activityCount: i.primaryActivityCount,
                            avatarUrl: i.primaryAvatarUrl,
                        },
                        {
                            id: i.toMergeId,
                            displayName: i.toMergeDisplayName,
                            activityCount: i.toActivityCount,
                            avatarUrl: i.toMergeAvatarUrl,
                        },
                    ],
                    similarity: i.similarity,
                }));
            }
            return { rows: result, hasMore, limit: args.limit, offset: args.offset };
        }
        return {
            rows: [{ members: [], similarity: 0 }],
            hasMore: false,
            limit: args.limit,
            offset: args.offset,
        };
    }
    static async removeToMerge(id, toMergeId, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, member_merge_1.removeMemberToMerge)(qx, id, toMergeId);
    }
    static async addNoMerge(id, toMergeId, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, member_merge_1.insertMemberNoMerge)(qx, id, toMergeId);
    }
    static async memberExists(username, platform, options, doPopulateRelations = true) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const usernames = [];
        if (typeof username === 'string') {
            usernames.push(username);
        }
        else if (Array.isArray(username)) {
            usernames.push(...username);
        }
        else {
            throw new Error('Unknown username format! Allowed formats are string or string[]. For example: "username" or ["username1", "username2"]');
        }
        // first find the id - we don't need the other bloat
        const results = await seq.query(`
    select mi."memberId"
    from "memberIdentities" mi
    where mi.platform = :platform and
          mi.type = :type and
          mi.value in (:usernames) and
          mi."deletedAt" is null and
          exists (select 1 from "memberSegments" ms where ms."memberId" = mi."memberId")
  `, {
            type: sequelize_1.default.QueryTypes.SELECT,
            replacements: {
                platform,
                usernames,
                type: types_1.MemberIdentityType.USERNAME,
            },
            transaction,
        });
        const ids = results.map((r) => r.memberId);
        if (ids.length === 0) {
            return null;
        }
        if (doPopulateRelations) {
            return this.findById(ids[0], options);
        }
        // the if needed actualy query the db for the rest by primary/foreign key which is much faster
        const records = await seq.query(`
      with segment_ids as (
        select "memberId", array_agg("segmentId") as "segmentIds" from
        "memberSegments"
        where "memberId" = :memberId
        group by "memberId"
      ),
      identities as (select mi."memberId",
                            array_agg(distinct mi.platform)             as identities,
                            jsonb_object_agg(mi.platform, mi.usernames) as username
                      from (select "memberId",
                                  platform,
                                  array_agg(username) as usernames
                            from (select "memberId",
                                        platform,
                                        value as username,
                                        "createdAt",
                                        row_number() over (partition by "memberId", platform order by "createdAt" desc) =
                                        1 as is_latest
                                  from "memberIdentities" where "memberId" = :memberId and type = '${types_1.MemberIdentityType.USERNAME}' and "deletedAt" is null) sub
                            group by "memberId", platform) mi
                      group by mi."memberId"),
        member_organizations as (
          select
            "memberId",
            JSONB_AGG(
                DISTINCT JSONB_BUILD_OBJECT(
                  'id', "organizationId",
                  'memberOrganizations',
                  JSONB_BUILD_OBJECT(
                    'memberId', "memberId",
                    'organizationId', "organizationId",
                    'dateStart', "dateStart",
                    'dateEnd', "dateEnd",
                    'createdAt', "createdAt",
                    'updatedAt', "updatedAt",
                    'title', title,
                    'source', source
                  )
                )
            ) AS orgs
          from "memberOrganizations"
          where "memberId" = :memberId
            and "deletedAt" is null
          group by "memberId"
        )
        select m."id",
              m."displayName",
              m."attributes",
              m."emails",
              m."score",
              m."enrichedBy",
              m."contributions",
              m."reach",
              m."joinedAt",
              m."importHash",
              m."createdAt",
              m."updatedAt",
              m."deletedAt",
              m."createdById",
              m."updatedById",
              i.username,
              si."segmentIds" as segments,
              coalesce(mo.orgs, '[]'::JSONB) as "organizations"
        from members m
                inner join identities i on i."memberId" = m.id
                inner join segment_ids si on si."memberId" = m.id
                left join member_organizations mo on mo."memberId" = m.id
        where m.id = :memberId;`, {
            type: sequelize_1.default.QueryTypes.SELECT,
            replacements: {
                memberId: ids[0],
            },
            transaction,
        });
        if (records.length !== 1) {
            throw new Error('Invalid number of records found!');
        }
        return records[0];
    }
    static async update(id, data, options, { manualChange = false, } = {}) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const record = await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.memberEditProfileAction)(id, async (captureOldState, captureNewState) => {
            var _a;
            const record = await options.database.member.findOne({
                where: {
                    id,
                },
                transaction,
            });
            if (!record) {
                throw new common_1.Error404();
            }
            captureOldState(record.get({ plain: true }));
            // exclude syncRemote attributes, since these are populated from memberSyncRemote table
            if ((_a = data.attributes) === null || _a === void 0 ? void 0 : _a.syncRemote) {
                delete data.attributes.syncRemote;
            }
            if (manualChange) {
                const manuallyChangedFields = record.manuallyChangedFields || [];
                for (const column of this.MEMBER_UPDATE_COLUMNS) {
                    let changed = false;
                    // only check fields that are in the data object that will be updated
                    if (column in data) {
                        if (record[column] !== null &&
                            column in data &&
                            (data[column] === null || data[column] === undefined)) {
                            // column was removed in the update -> will be set to null by sequelize
                            changed = true;
                        }
                        else if (record[column] === null &&
                            data[column] !== null &&
                            data[column] !== undefined &&
                            // also ignore empty arrays
                            (!Array.isArray(data[column]) || data[column].length > 0)) {
                            // column was null before now it's not anymore
                            changed = true;
                        }
                        else if (this.isEqual[column] &&
                            this.isEqual[column](record[column], data[column]) === false) {
                            // column value has changed
                            changed = true;
                        }
                    }
                    if (changed && !manuallyChangedFields.includes(column)) {
                        // handle attributes, keep each changed attribute separately
                        if (column === 'attributes') {
                            for (const key of Object.keys(data.attributes)) {
                                if (!record.attributes[key]) {
                                    manuallyChangedFields.push(`attributes.${key}`);
                                }
                                else if (!lodash_1.default.isEqual(record.attributes[key].default, data.attributes[key].default)) {
                                    manuallyChangedFields.push(`attributes.${key}`);
                                }
                            }
                        }
                        else {
                            manuallyChangedFields.push(column);
                        }
                    }
                }
                data.manuallyChangedFields = manuallyChangedFields;
            }
            else {
                // ignore columns that were manually changed
                // by rewriting them with db data
                const manuallyChangedFields = record.manuallyChangedFields || [];
                for (const manuallyChangedColumn of manuallyChangedFields) {
                    if (data.attributes && manuallyChangedColumn.startsWith('attributes')) {
                        const attributeKey = manuallyChangedColumn.split('.')[1];
                        data.attributes[attributeKey] = record.attributes[attributeKey];
                    }
                    else {
                        data[manuallyChangedColumn] = record[manuallyChangedColumn];
                    }
                }
                data.manuallyChangedFields = manuallyChangedFields;
            }
            const updatedMember = {
                ...lodash_1.default.pick(data, this.MEMBER_UPDATE_COLUMNS),
                updatedById: currentUser.id,
                manuallyChangedFields: data.manuallyChangedFields,
            };
            await options.database.member.update(captureNewState(updatedMember), {
                where: {
                    id: record.id,
                },
                transaction,
            });
            return record;
        }), !manualChange);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const subprojectIds = await (0, segments_2.getSegmentSubprojectIds)(qx, sequelizeRepository_1.default.getSegmentIds(options));
        const memberService = new common_services_1.CommonMemberService((0, sequelizeQueryExecutor_1.optionsQx)(options), options.temporal, options.log);
        await memberService.updateMemberOrganizations(record.id, data.organizations, data.organizationsReplace, subprojectIds, options);
        if (data.noMerge) {
            await record.setNoMerge(data.noMerge || [], {
                transaction,
            });
        }
        if (data.toMerge) {
            await record.setToMerge(data.toMerge || [], {
                transaction,
            });
        }
        if (data.affiliations) {
            await MemberRepository.setAffiliations(id, data.affiliations, options);
        }
        if (options.currentSegments && options.currentSegments.length > 0) {
            await (0, segments_1.includeMemberToSegments)(qx, record.id, subprojectIds);
        }
        // Before upserting identities, check if they already exist
        const checkIdentities = [...(data.identitiesToCreate || []), ...(data.identitiesToUpdate || [])];
        if (checkIdentities.length > 0) {
            for (const i of checkIdentities) {
                const query = `
          select "memberId"
          from "memberIdentities"
          where "platform" = :platform and
                "value" = :value and
                "type" = :type and
                "deletedAt" is null
        `;
                const data = await seq.query(query, {
                    replacements: {
                        platform: i.platform,
                        value: i.value,
                        type: i.type || types_1.MemberIdentityType.USERNAME,
                    },
                    type: sequelize_1.QueryTypes.SELECT,
                    transaction,
                });
                if (data.length > 0 && data[0].memberId !== record.id) {
                    const memberSegment = (await seq.query(`
            select distinct ms."segmentId", ms."memberId"
            from "memberSegments" ms where ms."memberId" = :memberId
            limit 1
            `, {
                        replacements: {
                            memberId: data[0].memberId,
                        },
                        type: sequelize_1.QueryTypes.SELECT,
                        transaction,
                    }));
                    if (memberSegment.length === 0) {
                        throw new Error('Member with same identity already exists!');
                    }
                    const segmentInfo = (await seq.query(`
          select s.id, pd.id as "parentId", gpd.id as "grandParentId"
          from segments s
                  inner join segments pd
                              on pd.slug = s."parentSlug" and pd."grandparentSlug" is null and
                                pd."parentSlug" is not null
                  inner join segments gpd on gpd.slug = s."grandparentSlug" and
                                              gpd."grandparentSlug" is null and gpd."parentSlug" is null
          where s.id = :segmentId;
          `, {
                        replacements: {
                            segmentId: memberSegment[0].segmentId,
                        },
                        type: sequelize_1.QueryTypes.SELECT,
                        transaction,
                    }));
                    throw new common_1.Error409(options.language, 'errors.alreadyExists', 
                    // @ts-ignore
                    JSON.stringify({
                        memberId: data[0].memberId,
                        grandParentId: segmentInfo[0].grandParentId,
                    }));
                }
            }
        }
        if (data.identitiesToCreate && data.identitiesToCreate.length > 0) {
            await (0, data_access_layer_1.insertMemberIdentities)(qx, data.identitiesToCreate.map((i) => ({
                memberId: record.id,
                platform: i.platform,
                value: i.value,
                type: i.type ? i.type : types_1.MemberIdentityType.USERNAME,
                sourceId: i.sourceId || null,
                integrationId: i.integrationId || null,
                verified: i.verified !== undefined ? i.verified : !!manualChange,
                source: i.source,
            })));
        }
        if (data.identitiesToUpdate && data.identitiesToUpdate.length > 0) {
            for (const i of data.identitiesToUpdate) {
                await (0, data_access_layer_1.updateVerifiedFlag)(qx, {
                    memberId: record.id,
                    platform: i.platform,
                    value: i.value,
                    type: i.type ? i.type : types_1.MemberIdentityType.USERNAME,
                    verified: i.verified !== undefined ? i.verified : !!manualChange,
                });
            }
        }
        if (data.identitiesToDelete && data.identitiesToDelete.length > 0) {
            for (const i of data.identitiesToDelete) {
                await (0, data_access_layer_1.deleteMemberIdentities)(qx, {
                    memberId: record.id,
                    platform: i.platform,
                    value: i.value,
                    type: i.type ? i.type : types_1.MemberIdentityType.USERNAME,
                });
            }
        }
        if (data.username) {
            data.username = (0, memberTypes_1.mapUsernameToIdentities)(data.username);
            const platforms = Object.keys(data.username);
            if (platforms.length > 0) {
                const platformsToDelete = [];
                const valuesToDelete = [];
                const typesToDelete = [];
                const identitiesToInsert = [];
                for (const platform of platforms) {
                    const identities = data.username[platform];
                    for (const identity of identities) {
                        if (identity.delete) {
                            platformsToDelete.push(identity.platform);
                            if (identity.value) {
                                valuesToDelete.push(identity.value);
                                typesToDelete.push(identity.type);
                            }
                            else {
                                valuesToDelete.push(identity.username);
                                typesToDelete.push(types_1.MemberIdentityType.USERNAME);
                            }
                        }
                        else if ((identity.username && identity.username !== '') ||
                            (identity.value && identity.value !== '')) {
                            identitiesToInsert.push({
                                memberId: record.id,
                                platform,
                                value: identity.value ? identity.value : identity.username,
                                type: identity.type ? identity.type : types_1.MemberIdentityType.USERNAME,
                                sourceId: identity.sourceId || null,
                                integrationId: identity.integrationId || null,
                                verified: identity.verified !== undefined ? identity.verified : !!manualChange,
                                source: identity.source || 'ui',
                            });
                        }
                    }
                }
                if (identitiesToInsert.length > 0) {
                    await (0, data_access_layer_1.insertMemberIdentities)(qx, identitiesToInsert);
                }
                if (platformsToDelete.length > 0) {
                    await (0, data_access_layer_1.deleteMemberIdentitiesByCombinations)(qx, {
                        memberId: record.id,
                        platforms: platformsToDelete,
                        values: valuesToDelete,
                        types: typesToDelete,
                    });
                }
            }
        }
        return this.findById(record.id, options);
    }
    static async destroy(id, options, force = false) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        await MemberRepository.excludeMembersFromSegments([id], { ...options, transaction });
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const memberSegments = await (0, segments_1.fetchAbsoluteMemberAggregates)(qx, id);
        // if member doesn't belong to any other segment anymore, remove it
        if (!memberSegments) {
            const record = await options.database.member.findOne({
                where: {
                    id,
                },
                transaction,
            });
            if (!record) {
                throw new common_1.Error404();
            }
            await record.destroy({
                force,
                transaction,
            });
        }
    }
    static async destroyBulk(ids, options, force = false) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        await MemberRepository.excludeMembersFromSegments(ids, { ...options, transaction });
        await options.database.member.destroy({
            where: {
                id: ids,
            },
            force,
            transaction,
        });
    }
    static async setAffiliations(memberId, data, options) {
        const qx = (0, sequelizeQueryExecutor_1.optionsQx)(options);
        await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.memberEditAffiliationsAction)(memberId, async (captureOldState, captureNewState) => {
            const oldOnes = await (0, member_segment_affiliations_1.findMemberAffiliations)(qx, memberId);
            captureOldState(oldOnes.map((item) => ({
                segmentId: item.segmentId,
                organizationId: item.organizationId,
                dateStart: item.dateStart,
                dateEnd: item.dateEnd,
            })));
            captureNewState(data);
            await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, { memberId });
            if (data.length === 0) {
                return;
            }
            await (0, member_segment_affiliations_1.insertMemberSegmentAffiliations)(qx, data.map((item) => ({
                memberId,
                segmentId: item.segmentId,
                organizationId: item.organizationId,
                dateStart: item.dateStart || null,
                dateEnd: item.dateEnd || null,
            })), true);
        }));
    }
    static async getAffiliations(memberId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const query = `
      select
        msa.id,
        s.id as "segmentId",
        s.slug as "segmentSlug",
        s.name as "segmentName",
        s."parentName" as "segmentParentName",
        o.id as "organizationId",
        o."displayName" as "organizationName",
        o.logo as "organizationLogo",
        msa."dateStart" as "dateStart",
        msa."dateEnd" as "dateEnd"
      from "memberSegmentAffiliations" msa
      left join organizations o on o.id = msa."organizationId"
      join segments s on s.id = msa."segmentId"
      where msa."memberId" = :memberId
        and msa."deletedAt" is null
    `;
        const data = await seq.query(query, {
            replacements: {
                memberId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return data;
    }
    static async findById(id, options, { segmentId, } = {}, include = {}, includeAllAttributes = false) {
        let memberResponse = null;
        const qx = (0, sequelizeQueryExecutor_1.optionsQx)(options);
        const bgQx = (0, sequelizeQueryExecutor_1.optionsBgQx)(options);
        memberResponse = await (0, members_1.queryMembersAdvanced)(qx, bgQx, options.redis, {
            filter: { id: { eq: id } },
            limit: 1,
            offset: 0,
            segmentId,
            includeAllAttributes,
            include: {
                memberOrganizations: false,
                lfxMemberships: true,
                identities: false,
                segments: true,
                onlySubProjects: true,
                maintainers: true,
                ...include,
            },
        });
        if (memberResponse.count === 0) {
            // try it again without segment information (no aggregates)
            // for members without activities
            memberResponse = await (0, members_1.queryMembersAdvanced)(qx, bgQx, options.redis, {
                filter: { id: { eq: id } },
                limit: 1,
                offset: 0,
                includeAllAttributes,
                include: {
                    lfxMemberships: true,
                    segments: true,
                    maintainers: true,
                    ...include,
                },
            });
            if (memberResponse.count === 0) {
                throw new common_1.Error404();
            }
            memberResponse.rows[0].activityCount = 0;
            memberResponse.rows[0].lastActive = null;
            memberResponse.rows[0].activityTypes = [];
            memberResponse.rows[0].activeOn = [];
            memberResponse.rows[0].averageSentiment = null;
        }
        const [data] = memberResponse.rows;
        return data;
    }
    static getUsernameFromIdentities(identities) {
        const username = {};
        for (const identity of identities.filter((i) => i.type === types_1.MemberIdentityType.USERNAME)) {
            if (username[identity.platform]) {
                username[identity.platform].push(identity.value);
            }
            else {
                username[identity.platform] = [identity.value];
            }
        }
        return username;
    }
    static async count(filter, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        return options.database.member.count({
            where: {
                ...filter,
            },
            transaction,
        });
    }
    static async countMembersPerSegment(options, segmentIds) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const result = await (0, data_access_layer_1.queryActivityRelations)(qx, {
            filter: {
                and: [
                    {
                        segmentId: {
                            in: segmentIds,
                        },
                    },
                ],
            },
            countOnly: true,
        });
        return result.count;
    }
    static async countMembers(options, segmentIds) {
        const countQuery = `
        SELECT
            COUNT(DISTINCT msa."memberId") AS "totalCount",
            msa."segmentId"
        FROM "memberSegmentsAgg" msa
        WHERE msa."segmentId" IN (:segmentIds)
        GROUP BY msa."segmentId";
    `;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        return seq.query(countQuery, {
            replacements: {
                segmentIds,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
    }
    static async findAndCountAll({ filter = {}, search = null, limit = 20, offset = 0, orderBy = 'joinedAt_DESC', segmentId = undefined, countOnly = false, fields = [...MemberRepository.QUERY_FILTER_COLUMN_MAP.keys()], include = {
        identities: true,
        segments: false,
        onlySubProjects: false,
        lfxMemberships: false,
        memberOrganizations: false,
        attributes: true,
        maintainers: true,
    }, attributesSettings = [], }, options) {
        if (!attributesSettings) {
            attributesSettings = (await memberAttributeSettingsRepository_1.default.findAndCountAll({}, options))
                .rows;
        }
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const withAggregates = !!segmentId;
        let segment;
        if (withAggregates) {
            segment = await new segmentRepository_1.default(options).findById(segmentId);
            if (segment === null) {
                options.log.info('No segment found for member query. Returning empty result.');
                return {
                    rows: [],
                    count: 0,
                    limit,
                    offset,
                };
            }
        }
        const params = {
            limit,
            offset,
            segmentId: segment === null || segment === void 0 ? void 0 : segment.id,
        };
        const filterString = common_1.RawQueryParser.parseFilters(filter, new Map([...MemberRepository.QUERY_FILTER_COLUMN_MAP.entries()].map(([key, { name }]) => [
            key,
            name,
        ])), [
            {
                property: 'attributes',
                column: 'm.attributes',
                attributeInfos: [
                    ...attributesSettings,
                    {
                        name: 'jobTitle',
                        type: types_1.MemberAttributeType.STRING,
                    },
                ],
            },
            {
                property: 'username',
                column: 'aggs.username',
                attributeInfos: types_1.ALL_PLATFORM_TYPES.map((p) => ({
                    name: p,
                    type: types_1.MemberAttributeType.STRING,
                })),
            },
        ], params, { pgPromiseFormat: true });
        const order = (function prepareOrderBy(orderBy = withAggregates ? 'activityCount_DESC' : 'id_DESC') {
            var _a;
            const orderSplit = orderBy.split('_');
            const orderField = (_a = MemberRepository.QUERY_FILTER_COLUMN_MAP.get(orderSplit[0])) === null || _a === void 0 ? void 0 : _a.name;
            if (!orderField) {
                return withAggregates ? 'msa."activityCount" DESC' : 'm.id DESC';
            }
            const orderDirection = ['DESC', 'ASC'].includes(orderSplit[1]) ? orderSplit[1] : 'DESC';
            return `${orderField} ${orderDirection}`;
        })(orderBy);
        const withSearch = !!search;
        let searchCTE = '';
        let searchJoin = '';
        let searchFilter = '1=1';
        if (withSearch) {
            search = search.toLowerCase();
            searchCTE = `
      ,
      member_search AS (
          SELECT
            DISTINCT "memberId"
          FROM "memberIdentities" mi
          where (verified and lower("value") like '%${search}%') and "deletedAt" is null
        )
      `;
            searchJoin = ` LEFT JOIN member_search ms ON ms."memberId" = m.id `;
            searchFilter = `
        (ms."memberId" IS NOT NULL OR lower(m."displayName") like '%${search}%')
       `;
        }
        const createQuery = (fields) => `
      WITH member_orgs AS (
        SELECT
          "memberId",
          ARRAY_AGG("organizationId")::TEXT[] AS "organizationId"
        FROM "memberOrganizations"
        WHERE "deletedAt" IS NULL
        GROUP BY 1
      )
      ${searchCTE}
      SELECT
        ${fields}
      FROM members m
      ${withAggregates
            ? ` JOIN "memberSegmentsAgg" msa ON msa."memberId" = m.id AND msa."segmentId" = $(segmentId)`
            : ''}
      LEFT JOIN member_orgs mo ON mo."memberId" = m.id
      ${searchJoin}
      WHERE (${filterString})
        AND (${searchFilter})
    `;
        if (countOnly) {
            return {
                rows: [],
                count: parseInt((await qx.selectOne(createQuery('COUNT(*)'), params)).count, 10),
                limit,
                offset,
            };
        }
        const results = await Promise.all([
            qx.select(`
          ${createQuery((function prepareFields(fields) {
                return `${fields
                    .map((f) => {
                    const mappedField = MemberRepository.QUERY_FILTER_COLUMN_MAP.get(f);
                    if (!mappedField) {
                        throw new common_1.Error400(options.language, `Invalid field: ${f}`);
                    }
                    return {
                        alias: f,
                        ...mappedField,
                    };
                })
                    .filter((mappedField) => mappedField.queryable !== false)
                    .filter((mappedField) => {
                    if (!withAggregates && mappedField.name.includes('msa.')) {
                        return false;
                    }
                    if (!include.memberOrganizations && mappedField.name.includes('mo.')) {
                        return false;
                    }
                    if (!include.attributes && mappedField.name === 'm.attributes') {
                        return false;
                    }
                    return true;
                })
                    .map((mappedField) => `${mappedField.name} AS "${mappedField.alias}"`)
                    .join(',\n')}`;
            })(fields))}
          ORDER BY ${order} NULLS LAST
          LIMIT $(limit)
          OFFSET $(offset)
        `, params),
            qx.selectOne(createQuery('COUNT(*)'), params),
        ]);
        const rows = results[0];
        const count = parseInt(results[1].count, 10);
        const memberIds = rows.map((org) => org.id);
        if (memberIds.length === 0) {
            return { rows: [], count, limit, offset };
        }
        if (include.memberOrganizations) {
            const memberOrganizations = await (0, members_1.fetchManyMemberOrgs)(qx, memberIds);
            const orgIds = (0, lodash_1.uniq)(memberOrganizations.reduce((acc, mo) => {
                acc.push(...mo.organizations.map((o) => o.organizationId));
                return acc;
            }, []));
            const orgExtra = orgIds.length
                ? await (0, data_access_layer_1.queryOrgs)(qx, {
                    filter: {
                        [data_access_layer_1.OrganizationField.ID]: {
                            in: orgIds,
                        },
                    },
                    fields: [data_access_layer_1.OrganizationField.ID, data_access_layer_1.OrganizationField.DISPLAY_NAME, data_access_layer_1.OrganizationField.LOGO],
                })
                : [];
            rows.forEach((member) => {
                var _a;
                member.organizations = (((_a = memberOrganizations.find((o) => o.memberId === member.id)) === null || _a === void 0 ? void 0 : _a.organizations) || []).map((o) => ({
                    id: o.organizationId,
                    ...orgExtra.find((odn) => odn.id === o.organizationId),
                    memberOrganizations: o,
                }));
                // sort organizations
                MemberRepository.sortOrganizations(member.organizations);
            });
        }
        if (include.lfxMemberships) {
            const lfxMemberships = await (0, lfx_memberships_1.findManyLfxMemberships)(qx, {
                organizationIds: (0, lodash_1.uniq)(rows.reduce((acc, r) => {
                    if (r.organizations) {
                        acc.push(...r.organizations.map((o) => o.id));
                    }
                    return acc;
                }, [])),
            });
            rows.forEach((member) => {
                if (member.organizations) {
                    member.organizations.forEach((o) => {
                        o.lfxMembership = lfxMemberships.find((m) => m.organizationId === o.id);
                    });
                }
            });
        }
        if (include.identities) {
            const identities = await (0, members_1.fetchManyMemberIdentities)(qx, memberIds);
            rows.forEach((member) => {
                var _a;
                member.identities = ((_a = identities.find((i) => i.memberId === member.id)) === null || _a === void 0 ? void 0 : _a.identities) || [];
            });
        }
        if (include.segments) {
            const memberSegments = await (0, members_1.fetchManyMemberSegments)(qx, memberIds);
            const segmentIds = (0, lodash_1.uniq)(memberSegments.reduce((acc, ms) => {
                acc.push(...ms.segments.map((s) => s.segmentId));
                return acc;
            }, []));
            const segmentsInfo = await (0, segments_2.fetchManySegments)(qx, segmentIds);
            rows.forEach((member) => {
                var _a;
                member.segments = (((_a = memberSegments.find((i) => i.memberId === member.id)) === null || _a === void 0 ? void 0 : _a.segments) || [])
                    .map((segment) => {
                    const segmentInfo = segmentsInfo.find((s) => s.id === segment.segmentId);
                    // include only subprojects if flag is set
                    if (include.onlySubProjects && (segmentInfo === null || segmentInfo === void 0 ? void 0 : segmentInfo.type) !== types_1.SegmentType.SUB_PROJECT) {
                        return null;
                    }
                    return {
                        id: segment.segmentId,
                        name: segmentInfo === null || segmentInfo === void 0 ? void 0 : segmentInfo.name,
                        activityCount: segment.activityCount,
                    };
                })
                    .filter(Boolean);
            });
        }
        if (include.maintainers) {
            const maintainerRoles = await (0, maintainers_1.findMaintainerRoles)(qx, memberIds);
            const segmentIds = (0, lodash_1.uniq)(maintainerRoles.map((m) => m.segmentId));
            const segmentsInfo = await (0, segments_2.fetchManySegments)(qx, segmentIds);
            const groupedMaintainers = (0, common_1.groupBy)(maintainerRoles, (m) => m.memberId);
            rows.forEach((member) => {
                member.maintainerRoles = (groupedMaintainers.get(member.id) || []).map((role) => {
                    const segmentInfo = segmentsInfo.find((s) => s.id === role.segmentId);
                    return {
                        ...role,
                        segmentName: segmentInfo === null || segmentInfo === void 0 ? void 0 : segmentInfo.name,
                    };
                });
            });
        }
        if (memberIds.length > 0) {
            const activityTypes = segmentRepository_1.default.getActivityTypes(options);
            const lastActivities = await (0, data_access_layer_1.getLastActivitiesForMembers)(qx, memberIds, activityTypes, [
                segmentId,
            ]);
            rows.forEach((r) => {
                r.lastActivity = lastActivities.find((a) => a.memberId === r.id);
                if (r.lastActivity) {
                    r.lastActivity.display = integrations_1.ActivityDisplayService.getDisplayOptions(r.lastActivity, segmentRepository_1.default.getActivityTypes(options), [types_1.ActivityDisplayVariant.SHORT, types_1.ActivityDisplayVariant.CHANNEL]);
                }
            });
        }
        return { rows, count, limit, offset };
    }
    /**
     * Returns sequelize literals for dynamic member attributes.
     * @param memberAttributeSettings
     * @param options
     * @returns
     */
    static async getDynamicAttributesLiterals(memberAttributeSettings, options) {
        // get possible platforms for a tenant
        const availableDynamicAttributePlatformKeys = [
            'default',
            'custom',
            ...(await tenantRepository_1.default.getAvailablePlatforms(options)).map((p) => p.platform),
        ];
        const dynamicAttributesDefaultNestedFields = memberAttributeSettings.reduce((acc, attribute) => {
            acc[attribute.name] = `attributes.${attribute.name}.default`;
            return acc;
        }, {});
        const dynamicAttributesPlatformNestedFields = memberAttributeSettings.reduce((acc, attribute) => {
            for (const key of availableDynamicAttributePlatformKeys) {
                if (attribute.type === types_1.MemberAttributeType.NUMBER) {
                    acc[`attributes.${attribute.name}.${key}`] = sequelize_1.default.literal(`("member"."attributes"#>>'{${attribute.name},${key}}')::integer`);
                }
                else if (attribute.type === types_1.MemberAttributeType.BOOLEAN) {
                    acc[`attributes.${attribute.name}.${key}`] = sequelize_1.default.literal(`("member"."attributes"#>>'{${attribute.name},${key}}')::boolean`);
                }
                else if (attribute.type === types_1.MemberAttributeType.MULTI_SELECT) {
                    acc[`attributes.${attribute.name}.${key}`] = sequelize_1.default.literal(`ARRAY( SELECT jsonb_array_elements_text("member"."attributes"#>'{${attribute.name},${key}}'))`);
                }
                else {
                    acc[`attributes.${attribute.name}.${key}`] = sequelize_1.default.literal(`"member"."attributes"#>>'{${attribute.name},${key}}'`);
                }
            }
            return acc;
        }, {});
        const dynamicAttributesProjection = memberAttributeSettings.reduce((acc, attribute) => {
            for (const key of availableDynamicAttributePlatformKeys) {
                if (key === 'default') {
                    acc.push([
                        sequelize_1.default.literal(`"member"."attributes"#>>'{${attribute.name},default}'`),
                        attribute.name,
                    ]);
                }
                else {
                    acc.push([
                        sequelize_1.default.literal(`"member"."attributes"#>>'{${attribute.name},${key}}'`),
                        `${attribute.name}.${key}`,
                    ]);
                }
            }
            return acc;
        }, []);
        return {
            dynamicAttributesDefaultNestedFields,
            dynamicAttributesPlatformNestedFields,
            availableDynamicAttributePlatformKeys,
            dynamicAttributesProjection,
        };
    }
    static async findAllAutocomplete(query, limit, options) {
        const whereAnd = [{}];
        if (query) {
            whereAnd.push({
                [Op.or]: [
                    {
                        displayName: {
                            [Op.iLike]: `${query}%`,
                        },
                    },
                ],
            });
        }
        const where = { [Op.and]: whereAnd };
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_2.getSegmentSubprojectIds)(qx, currentSegments);
        const records = await options.database.member.findAll({
            attributes: ['id', 'displayName', 'attributes'],
            where,
            limit: limit ? Number(limit) : undefined,
            order: [['displayName', 'ASC']],
            include: [
                {
                    model: options.database.organization,
                    attributes: ['id', 'displayName'],
                    as: 'organizations',
                },
                {
                    model: options.database.segment,
                    as: 'segments',
                    where: {
                        id: subprojectIds,
                    },
                },
            ],
        });
        return records.map((record) => {
            var _a, _b;
            return ({
                id: record.id,
                label: record.displayName,
                avatar: ((_b = (_a = record.attributes) === null || _a === void 0 ? void 0 : _a.avatarUrl) === null || _b === void 0 ? void 0 : _b.default) || null,
                organizations: record.organizations.map((org) => ({
                    id: org.id,
                    name: org.name,
                })),
            });
        });
    }
    static async addAsUnverifiedIdentity(memberIds, value, type, platform, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const normalizedValue = (0, common_1.normalizeMemberIdentityValue)(value);
        const query = `
      insert into "memberIdentities"("memberId", platform, type, value, "tenantId", verified)
      values(:memberId, :platform, :type, :value, :tenantId, false)
      on conflict do nothing;
    `;
        for (const memberId of memberIds) {
            await seq.query(query, {
                replacements: {
                    memberId,
                    value: normalizedValue,
                    type,
                    platform,
                    tenantId: common_1.DEFAULT_TENANT_ID,
                },
                type: sequelize_1.QueryTypes.INSERT,
                transaction,
            });
        }
    }
    static async _populateRelationsForRows(rows, attributesSettings, exportMode = false) {
        if (!rows) {
            return rows;
        }
        if ((conf_1.KUBE_MODE && conf_1.SERVICE === configTypes_1.ServiceType.JOB_GENERATOR && !exportMode) ||
            process.env.SERVICE === 'integrations') {
            return rows.map((record) => {
                const plainRecord = record.get({ plain: true });
                plainRecord.noMerge = plainRecord.noMergeIds ? plainRecord.noMergeIds.split(',') : [];
                plainRecord.toMerge = plainRecord.toMergeIds ? plainRecord.toMergeIds.split(',') : [];
                delete plainRecord.toMergeIds;
                delete plainRecord.noMergeIds;
                return plainRecord;
            });
        }
        return Promise.all(rows.map(async (record) => {
            var _a;
            const plainRecord = record.get({ plain: true });
            plainRecord.noMerge = plainRecord.noMergeIds ? plainRecord.noMergeIds.split(',') : [];
            plainRecord.toMerge = plainRecord.toMergeIds ? plainRecord.toMergeIds.split(',') : [];
            plainRecord.lastActivity = plainRecord.lastActive
                ? (await record.getActivities({
                    order: [['timestamp', 'DESC']],
                    limit: 1,
                }))[0].get({ plain: true })
                : null;
            delete plainRecord.toMergeIds;
            delete plainRecord.noMergeIds;
            plainRecord.activeOn = (_a = plainRecord.activeOn) !== null && _a !== void 0 ? _a : [];
            for (const attribute of attributesSettings) {
                if (Object.prototype.hasOwnProperty.call(plainRecord, attribute.name)) {
                    delete plainRecord[attribute.name];
                }
            }
            for (const attributeName in plainRecord.attributes) {
                if (!lodash_1.default.find(attributesSettings, { name: attributeName })) {
                    delete plainRecord.attributes[attributeName];
                }
            }
            delete plainRecord.contributions;
            delete plainRecord.company;
            plainRecord.organizations = await record.getOrganizations({
                joinTableAttributes: [],
            });
            return plainRecord;
        }));
    }
    static async findWorkExperience(memberId, timestamp, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const query = `
      SELECT * FROM "memberOrganizations"
      WHERE "memberId" = :memberId
        AND (
          ("dateStart" <= :timestamp AND "dateEnd" >= :timestamp)
          OR ("dateStart" <= :timestamp AND "dateEnd" IS NULL)
        )
        AND "deletedAt" IS NULL
      ORDER BY "dateStart" DESC, id
      LIMIT 1
    `;
        const records = await seq.query(query, {
            replacements: {
                memberId,
                timestamp,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (records.length === 0) {
            return null;
        }
        return records[0];
    }
    static async findMostRecentOrganization(memberId, timestamp, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const query = `
      SELECT * FROM "memberOrganizations"
      WHERE "memberId" = :memberId
        AND "dateStart" IS NULL
        AND "dateEnd" IS NULL
        AND "createdAt" <= :timestamp
        AND "deletedAt" IS NULL
      ORDER BY "createdAt" DESC, id
      LIMIT 1
    `;
        const records = await seq.query(query, {
            replacements: {
                memberId,
                timestamp,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (records.length === 0) {
            return null;
        }
        return records[0];
    }
    static async findMostRecentOrganizationEver(memberId, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const query = `
      SELECT * FROM "memberOrganizations"
      WHERE "memberId" = :memberId
        AND "dateStart" IS NULL
        AND "dateEnd" IS NULL
        AND "deletedAt" IS NULL
      ORDER BY "createdAt", id
      LIMIT 1
    `;
        const records = await seq.query(query, {
            replacements: {
                memberId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (records.length === 0) {
            return null;
        }
        return records[0];
    }
    static sortOrganizations(organizations) {
        organizations.sort((a, b) => {
            var _a, _b, _c, _d;
            a = a.dataValues ? a.get({ plain: true }) : a;
            b = b.dataValues ? b.get({ plain: true }) : b;
            const aStart = (_a = a.memberOrganizations) === null || _a === void 0 ? void 0 : _a.dateStart;
            const bStart = (_b = b.memberOrganizations) === null || _b === void 0 ? void 0 : _b.dateStart;
            const aEnd = (_c = a.memberOrganizations) === null || _c === void 0 ? void 0 : _c.dateEnd;
            const bEnd = (_d = b.memberOrganizations) === null || _d === void 0 ? void 0 : _d.dateEnd;
            // Sorting:
            // 1. Those without dateEnd, but with dateStart should be at the top, orderd by dateStart
            // 2. Those with dateEnd and dateStart should be in the middle, ordered by dateEnd
            // 3. Those without dateEnd and dateStart should be at the bottom, ordered by name
            if (!aEnd && aStart) {
                if (!bEnd && bStart) {
                    return aStart > bStart ? -1 : 1;
                }
                if (bEnd && bStart) {
                    return -1;
                }
                return -1;
            }
            if (aEnd && aStart) {
                if (!bEnd && bStart) {
                    return 1;
                }
                if (bEnd && bStart) {
                    return aEnd > bEnd ? -1 : 1;
                }
                return -1;
            }
            if (!bEnd && bStart) {
                return 1;
            }
            if (bEnd && bStart) {
                return 1;
            }
            return a.name > b.name ? 1 : -1;
        });
    }
    static async moveSelectedAffiliationsBetweenMembers(fromMemberId, toMemberId, memberSegmentAffiliationIds, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const params = {
            fromMemberId,
            toMemberId,
            memberSegmentAffiliationIds,
        };
        const updateQuery = `
      update "memberSegmentAffiliations" set "memberId" = :toMemberId where "memberId" = :fromMemberId
      and "id" in (:memberSegmentAffiliationIds);
    `;
        await seq.query(updateQuery, {
            replacements: params,
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
    }
    static async removeIdentitiesFromMember(memberId, identities, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        for (const identity of identities) {
            await (0, data_access_layer_1.deleteMemberIdentities)(qx, {
                memberId,
                value: identity.value,
                type: identity.type,
                platform: identity.platform,
            });
        }
    }
    static async findAlreadyExistingIdentities(identities, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const existingIdentities = await (0, data_access_layer_1.findAlreadyExistingVerifiedIdentities)(qx, { identities });
        return existingIdentities;
    }
}
MemberRepository.MEMBER_UPDATE_COLUMNS = [
    'displayName',
    'attributes',
    'emails',
    'contributions',
    'score',
    'reach',
    'importHash',
];
MemberRepository.isEqual = {
    displayName: (a, b) => a === b,
    attributes: (a, b) => lodash_1.default.isEqual(a, b),
    emails: (a, b) => lodash_1.default.isEqual(a, b),
    contributions: (a, b) => lodash_1.default.isEqual(a, b),
    score: (a, b) => a === b,
    reach: (a, b) => lodash_1.default.isEqual(a, b),
    importHash: (a, b) => a === b,
};
MemberRepository.QUERY_FILTER_COLUMN_MAP = new Map([
    // id fields
    ['id', { name: 'm.id' }],
    ['segmentId', { name: 'msa."segmentId"' }],
    // member fields
    ['displayName', { name: 'm."displayName"' }],
    ['reach', { name: 'm.reach' }],
    ['joinedAt', { name: 'm."joinedAt"' }],
    ['jobTitle', { name: `m.attributes -> 'jobTitle' ->> 'default'` }],
    [
        'numberOfOpenSourceContributions',
        {
            name: "CASE WHEN jsonb_typeof(m.contributions) = 'array' THEN jsonb_array_length(m.contributions) ELSE 0 END",
        },
    ],
    ['isBot', { name: `COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE)` }],
    [
        'isTeamMember',
        { name: `COALESCE((m.attributes -> 'isTeamMember' ->> 'default')::BOOLEAN, FALSE)` },
    ],
    [
        'isOrganization',
        { name: `COALESCE((m.attributes -> 'isOrganization' ->> 'default')::BOOLEAN, FALSE)` },
    ],
    // member agg fields
    ['lastActive', { name: 'msa."lastActive"' }],
    ['identityPlatforms', { name: 'msa."activeOn"' }],
    ['score', { name: 'm.score' }],
    ['averageSentiment', { name: 'msa."averageSentiment"' }],
    ['activityTypes', { name: 'msa."activityTypes"' }],
    ['activeOn', { name: 'msa."activeOn"' }],
    ['activityCount', { name: 'msa."activityCount"' }],
    // others
    ['organizations', { name: 'mo."organizationId"', queryable: false }],
    // fields for querying
    ['attributes', { name: 'm.attributes' }],
]);
exports.default = MemberRepository;
//# sourceMappingURL=memberRepository.js.map