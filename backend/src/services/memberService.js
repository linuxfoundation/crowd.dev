"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-continue */
const lodash_1 = __importDefault(require("lodash"));
const moment_timezone_1 = __importDefault(require("moment-timezone"));
const validator_1 = __importDefault(require("validator"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const members_1 = require("@crowd/data-access-layer/src/members");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const memberAttributeSettingsRepository_1 = __importDefault(require("../database/repositories/memberAttributeSettingsRepository"));
const memberRepository_1 = __importDefault(require("../database/repositories/memberRepository"));
const mergeActionsRepository_1 = require("../database/repositories/mergeActionsRepository");
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const memberTypes_1 = require("../database/repositories/types/memberTypes");
const telemetryTrack_1 = __importDefault(require("../segment/telemetryTrack"));
const memberAttributeSettingsService_1 = __importDefault(require("./memberAttributeSettingsService"));
const organizationService_1 = __importDefault(require("./organizationService"));
const searchSyncService_1 = __importDefault(require("./searchSyncService"));
const settingsService_1 = __importDefault(require("./settingsService"));
class MemberService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    static normalizeIds(ids) {
        if (typeof ids === 'string') {
            return ids.length > 0 ? [ids] : [];
        }
        if (Array.isArray(ids)) {
            return ids.filter((id) => typeof id === 'string' && id.length > 0);
        }
        return [];
    }
    /**
     * Validates the attributes against its saved settings.
     *
     * Throws 400 Errors if the attribute does not exist in settings,
     * or if the sent attribute type does not match the type in the settings.
     * Also restructures custom attributes that come only as a value, without platforms.
     *
     * Example custom attributes restructuring
     * {
     *   attributes: {
     *      someAttributeName: 'someValue'
     *   }
     * }
     *
     * This object is transformed into:
     * {
     *   attributes: {
     *     someAttributeName: {
     *        custom: 'someValue'
     *     },
     *   }
     * }
     *
     * @param attributes
     * @returns restructured object
     */
    async validateAttributes(attributes, transaction = null) {
        // check attribute exists in memberAttributeSettings
        const memberAttributeSettings = (await memberAttributeSettingsRepository_1.default.findAndCountAll({}, { ...this.options, ...(transaction && { transaction }) })).rows.reduce((acc, attribute) => {
            acc[attribute.name] = attribute;
            return acc;
        }, {});
        for (const attributeName of Object.keys(attributes)) {
            if (!memberAttributeSettings[attributeName]) {
                this.log.error('Attribute does not exist', {
                    attributeName,
                    attributes,
                });
                delete attributes[attributeName];
                continue;
            }
            if (typeof attributes[attributeName] !== 'object') {
                attributes[attributeName] = {
                    custom: attributes[attributeName],
                };
            }
            for (const platform of Object.keys(attributes[attributeName])) {
                if (attributes[attributeName][platform] !== undefined &&
                    attributes[attributeName][platform] !== null) {
                    if (!memberAttributeSettingsService_1.default.isCorrectType(attributes[attributeName][platform], memberAttributeSettings[attributeName].type, { options: memberAttributeSettings[attributeName].options })) {
                        this.log.error('Failed to validate attributee', {
                            attributeName,
                            platform,
                            attributeValue: attributes[attributeName][platform],
                            attributeType: memberAttributeSettings[attributeName].type,
                            options: memberAttributeSettings[attributeName].options,
                        });
                        throw new common_1.Error400(this.options.language, 'settings.memberAttributes.wrongType', attributeName, memberAttributeSettings[attributeName].type);
                    }
                }
            }
        }
        return attributes;
    }
    /**
     * Sets the attribute.default key as default values of attribute
     * object using the priority array stored in the settings.
     * Throws a 400 Error if priority array does not exist.
     * @param attributes
     * @returns attribute object with default values
     */
    async setAttributesDefaultValues(attributes) {
        if (!(await settingsService_1.default.platformPriorityArrayExists(this.options))) {
            throw new common_1.Error400(this.options.language, 'settings.memberAttributes.priorityArrayNotFound');
        }
        const priorityArray = this.options.currentTenant.settings[0].get({ plain: true })
            .attributeSettings.priorities;
        for (const attributeName of Object.keys(attributes)) {
            const highestPriorityPlatform = MemberService.getHighestPriorityPlatformForAttributes(Object.keys(attributes[attributeName]), priorityArray);
            if (highestPriorityPlatform !== undefined) {
                attributes[attributeName].default = attributes[attributeName][highestPriorityPlatform];
            }
            else {
                delete attributes[attributeName];
            }
        }
        return attributes;
    }
    /**
     * Returns the highest priority platform from an array of platforms
     * If any of the platforms does not exist in the priority array, returns
     * the first platform sent as the highest priority platform.
     * @param platforms Array of platforms to select the highest priority platform
     * @param priorityArray zero indexed priority array. Lower index means higher priority
     * @returns the highest priority platform or undefined if values are incorrect
     */
    static getHighestPriorityPlatformForAttributes(platforms, priorityArray) {
        if (platforms.length <= 0) {
            return undefined;
        }
        const filteredPlatforms = priorityArray.filter((i) => platforms.includes(i));
        return filteredPlatforms.length > 0 ? filteredPlatforms[0] : platforms[0];
    }
    /**
     * Upsert a member. If the member exists, it updates it. If it does not exist, it creates it.
     * The update is done with a deep merge of the original and the new member.
     * The member is returned without relations
     * Only the fields that have changed are updated.
     * @param data Data for the member
     * @param existing If the member already exists. If it does not, false. Othwerwise, the member.
     * @returns The created member
     */
    async upsert(data, existing = false, fireCrowdWebhooks = true, syncToOpensearch = true) {
        const logger = this.options.log;
        const searchSyncService = new searchSyncService_1.default(this.options);
        const errorDetails = {};
        if (data.identities && data.identities.length > 0) {
            // map identities to username
            const username = {};
            for (const i of data.identities) {
                if (!username[i.platform]) {
                    username[i.platform] = [];
                }
                if (!data.platform && i.type === types_1.MemberIdentityType.USERNAME) {
                    data.platform = i.platform;
                }
                username[i.platform].push({
                    value: i.value,
                    type: i.type,
                });
            }
            data.username = username;
        }
        if (!('platform' in data)) {
            throw new common_1.Error400(this.options.language, 'activity.platformRequiredWhileUpsert');
        }
        data.username = (0, memberTypes_1.mapUsernameToIdentities)(data.username, data.platform);
        if (!(data.platform in data.username)) {
            throw new common_1.Error400(this.options.language, 'activity.platformAndUsernameNotMatching');
        }
        if (!data.displayName) {
            data.displayName = (0, common_1.getProperDisplayName)(data.username[data.platform][0].username);
        }
        if (!(data.platform in data.username)) {
            throw new common_1.Error400(this.options.language, 'activity.platformAndUsernameNotMatching');
        }
        if (!data.displayName) {
            data.displayName = data.username[data.platform].username;
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const { platform } = data;
            if (data.attributes) {
                data.attributes = await this.validateAttributes(data.attributes, transaction);
            }
            if (data.reach) {
                data.reach = typeof data.reach === 'object' ? data.reach : { [platform]: data.reach };
                data.reach = (0, common_1.calculateReach)(data.reach, {});
            }
            else {
                data.reach = { total: -1 };
            }
            delete data.platform;
            if (!('joinedAt' in data)) {
                data.joinedAt = moment_timezone_1.default.tz('Europe/London').toDate();
            }
            if (!existing) {
                existing = await this.memberExists(data.username, platform);
            }
            else {
                // let's look just in case for an existing member and if they are different we should log them because they will probably fail to insert
                const tempExisting = await this.memberExists(data.username, platform);
                if (!tempExisting) {
                    logger.warn({ existingMemberId: existing.id }, 'We have received an existing member but actually we could not find him by username and platform!');
                    errorDetails.reason = 'member_service_upsert_existing_member_not_found';
                    errorDetails.details = {
                        existingMemberId: existing.id,
                        username: data.username,
                        platform,
                    };
                }
                else if (existing.id !== tempExisting.id) {
                    logger.warn({ existingMemberId: existing.id, actualExistingMemberId: tempExisting.id }, 'We found a member with the same username and platform but different id!');
                    errorDetails.reason = 'member_service_upsert_existing_member_mismatch';
                    errorDetails.details = {
                        existingMemberId: existing.id,
                        actualExistingMemberId: tempExisting.id,
                        username: data.username,
                        platform,
                    };
                }
            }
            // Collect IDs for relation
            const organizations = [];
            // If organizations are sent
            if (data.organizations) {
                for (const organization of data.organizations) {
                    if (typeof organization === 'string' && validator_1.default.isUUID(organization)) {
                        // If an ID was already sent, we simply push it to the list
                        organizations.push(organization);
                    }
                    else if (typeof organization === 'object' && organization.id) {
                        organizations.push(organization);
                    }
                    else {
                        // Otherwise, either another string or an object was sent
                        const organizationService = new organizationService_1.default(this.options);
                        let data = {};
                        if (typeof organization === 'string') {
                            // If a string was sent, we assume it is the name of the organization
                            data = {
                                identities: [
                                    {
                                        value: organization,
                                        type: types_1.OrganizationIdentityType.USERNAME,
                                        platform,
                                        verified: true,
                                        source: 'ui',
                                        sourceId: null,
                                        integrationId: null,
                                    },
                                ],
                            };
                        }
                        else {
                            // Otherwise, we assume it is an object with the data of the organization
                            data = organization;
                        }
                        // We createOrUpdate the organization and add it to the list of IDs
                        const organizationRecord = await organizationService.createOrUpdate(data, {
                            doSync: syncToOpensearch,
                            mode: types_1.SyncMode.ASYNCHRONOUS,
                        });
                        organizations.push({ id: organizationRecord.id });
                    }
                }
            }
            // Auto assign member to organization if email domain matches
            if (data.emails) {
                const emailDomains = new Set();
                // Collect unique domains
                for (const email of data.emails) {
                    if (email) {
                        const domain = email.split('@')[1];
                        if (!(0, common_1.isDomainExcluded)(domain)) {
                            emailDomains.add(domain);
                        }
                    }
                }
                // Fetch organization ids for these domains
                const organizationService = new organizationService_1.default(this.options);
                for (const domain of emailDomains) {
                    if (domain) {
                        const org = await organizationService.createOrUpdate({
                            displayName: domain,
                            attributes: {
                                name: {
                                    default: domain,
                                    custom: [domain],
                                },
                            },
                            identities: [
                                {
                                    value: domain,
                                    type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                                    platform: 'email',
                                    verified: true,
                                    source: 'ui',
                                    sourceId: null,
                                    integrationId: null,
                                },
                            ],
                        }, {
                            doSync: syncToOpensearch,
                            mode: types_1.SyncMode.ASYNCHRONOUS,
                        });
                        if (org) {
                            organizations.push({ id: org.id });
                        }
                    }
                }
            }
            // Remove dups
            if (organizations.length > 0) {
                data.organizations = lodash_1.default.uniqBy(organizations, 'id');
            }
            let record;
            if (existing) {
                const { id } = existing;
                delete existing.id;
                const toUpdate = common_services_1.CommonMemberService.membersMerge(existing, data);
                if (toUpdate.attributes) {
                    if (!(0, common_1.hasAttributeValue)(toUpdate.attributes.country)) {
                        const location = (0, common_1.getAttributeValue)(toUpdate.attributes.location);
                        const country = (0, common_1.getCountry)(location);
                        if (country) {
                            toUpdate.attributes.country = {
                                ...toUpdate.attributes.country,
                                system: country,
                            };
                        }
                    }
                    toUpdate.attributes = await this.setAttributesDefaultValues(toUpdate.attributes);
                }
                // It is important to call it with doPopulateRelations=false
                // because otherwise the performance is greatly decreased in integrations
                record = await memberRepository_1.default.update(id, toUpdate, {
                    ...this.options,
                    transaction,
                });
            }
            else {
                // It is important to call it with doPopulateRelations=false
                // because otherwise the performance is greatly decreased in integrations
                if (data.attributes) {
                    if (!(0, common_1.hasAttributeValue)(data.attributes.country)) {
                        const location = (0, common_1.getAttributeValue)(data.attributes.location);
                        const country = (0, common_1.getCountry)(location);
                        if (country) {
                            data.attributes.country = {
                                ...data.attributes.country,
                                system: country,
                            };
                        }
                    }
                    data.attributes = await this.setAttributesDefaultValues(data.attributes);
                }
                record = await memberRepository_1.default.create(data, {
                    ...this.options,
                    transaction,
                });
                (0, telemetryTrack_1.default)('Member created', {
                    id: record.id,
                    createdAt: record.createdAt,
                    identities: record.identities,
                }, this.options);
            }
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction });
            await (async function includeMemberInSegments(qx, memberId, segmentIds) {
                const segments = await (0, segments_1.fetchManySegments)(qx, segmentIds);
                const data = segments.reduce((acc, s) => {
                    for (const segmentId of [s.id, s.parentId, s.grandparentId]) {
                        acc.push({
                            memberId,
                            segmentId,
                            activityCount: 0,
                            lastActive: '1970-01-01',
                            activityTypes: [],
                            activeOn: [],
                            averageSentiment: null,
                        });
                    }
                    return acc;
                }, []);
                await (0, members_1.insertMemberSegmentAggregates)(qx, data);
            })(qx, record.id, this.options.currentSegments.map((s) => s.id));
            await sequelizeRepository_1.default.commitTransaction(transaction);
            if (syncToOpensearch) {
                await searchSyncService.triggerMemberSync(record.id);
            }
            if (!fireCrowdWebhooks) {
                this.log.info('Ignoring outgoing webhooks because of fireCrowdWebhooks!');
            }
            return record;
        }
        catch (error) {
            const reason = errorDetails.reason || undefined;
            const details = errorDetails.details || undefined;
            if (error.name && error.name.includes('Sequelize')) {
                logger.error(error, {
                    query: error.sql,
                    errorMessage: error.original.message,
                    reason,
                    details,
                }, 'Error during member upsert!');
            }
            else {
                logger.error(error, { reason, details }, 'Error during member upsert!');
            }
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'member');
            throw { ...error, reason, details };
        }
    }
    /**
     * Checks if given user already exists by username and platform.
     * Username can be given as a plain string or as dictionary with
     * related platforms.
     * Ie:
     * username = 'anil' || username = { github: 'anil' } || username = { github: 'anil', twitter: 'some-other-username' } || username = { github: { username: 'anil' } } || username = { github: [{ username: 'anil' }] }
     * @param username username of the member
     * @param platform platform of the member
     * @returns null | found member
     */
    async memberExists(username, platform) {
        const fillRelations = false;
        const usernames = [];
        if (typeof username === 'string') {
            usernames.push(username);
        }
        else if (typeof username === 'object') {
            if ('username' in username) {
                usernames.push(username.username);
            }
            else if (platform in username) {
                if (typeof username[platform] === 'string') {
                    usernames.push(username[platform]);
                }
                else if (Array.isArray(username[platform])) {
                    if (username[platform].length === 0) {
                        throw new common_1.Error400(this.options.language, 'activity.platformAndUsernameNotMatching');
                    }
                    else if (typeof username[platform] === 'string') {
                        usernames.push(username[platform]);
                    }
                    else if (typeof username[platform][0] === 'object') {
                        usernames.push(...username[platform].map((u) => u.username));
                    }
                }
                else if (typeof username[platform] === 'object') {
                    usernames.push(username[platform].username);
                }
                else {
                    throw new common_1.Error400(this.options.language, 'activity.platformAndUsernameNotMatching');
                }
            }
            else {
                throw new common_1.Error400(this.options.language, 'activity.platformAndUsernameNotMatching');
            }
        }
        // It is important to call it with doPopulateRelations=false
        // because otherwise the performance is greatly decreased in integrations
        const existing = await memberRepository_1.default.memberExists(usernames, platform, {
            ...this.options,
        }, fillRelations);
        return existing;
    }
    async unmergePreview(memberId, identityId, revertPreviousMerge = false) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, common_services_1.prepareMemberUnmerge)(qx, memberId, identityId, revertPreviousMerge);
    }
    async unmerge(memberId, payload) {
        var _a;
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const { primary, secondary, movedIdentities } = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.memberUnmergeAction)(memberId, async (captureOldState, captureNewState) => {
            captureOldState({ primary: payload.primary });
            const result = await qx.tx(async (tx) => { var _a; return (0, common_services_1.unmergeMember)(tx, memberId, payload, (_a = this.options.currentUser) === null || _a === void 0 ? void 0 : _a.id); });
            captureNewState({
                primary: result.primary,
                secondary: result.secondary,
            });
            return result;
        }));
        await (0, common_services_1.invalidateMemberQueryCache)(this.options.redis, [primary.id, secondary.id], true);
        await (0, common_services_1.startMemberUnmergeWorkflow)(this.options.temporal, {
            primaryId: primary.id,
            secondaryId: secondary.id,
            movedIdentities,
            primaryDisplayName: primary.displayName,
            secondaryDisplayName: secondary.displayName,
            actorId: (_a = this.options.currentUser) === null || _a === void 0 ? void 0 : _a.id,
        });
    }
    async canRevertMerge(memberId, identityId) {
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
            const identity = await (0, members_1.findMemberIdentityById)(qx, memberId, identityId);
            if (!identity) {
                throw new Error(`Member doesn't have an identity with id ${identityId}!`);
            }
            const mergeAction = await mergeActionsRepository_1.MergeActionsRepository.findMergeBackup(memberId, types_1.MergeActionType.MEMBER, identity, this.options);
            if (!mergeAction) {
                return false;
            }
            const secondaryBackup = mergeAction.unmergeBackup.secondary;
            const memberIdentities = await (0, members_1.fetchMemberIdentities)(qx, memberId);
            const remainingIdentitiesInCurrentMember = memberIdentities.filter((i) => !secondaryBackup.identities.some((s) => s.platform === i.platform && s.value === i.value && s.type === i.type));
            return remainingIdentitiesInCurrentMember.length > 0;
        }
        catch (err) {
            this.options.log.error(err, 'Error while checking if member merge can be reverted!');
            throw err;
        }
    }
    async findGithub(memberId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const memberIdentities = memberRepository_1.default.getUsernameFromIdentities(await (0, members_1.fetchMemberIdentities)(qx, memberId));
        const token = await (0, common_services_1.getGithubInstallationToken)();
        const axios = require('axios');
        // GitHub allows a maximum of 5 parameters
        const identities = Object.values(memberIdentities).flat().slice(0, 5);
        // Join the usernames for search
        const identitiesQuery = identities.join('+OR+');
        const url = `https://api.github.com/search/users?q=${identitiesQuery}`;
        const headers = {
            Accept: 'application/vnd.github+json',
            Authorization: `Bearer ${token}`,
            'X-GitHub-Api-Version': '2022-11-28',
        };
        const response = await axios.get(url, { headers });
        const data = response.data.items.map((item) => ({
            username: item.login,
            avatarUrl: item.avatar_url,
            score: item.score,
            url: item.html_url,
        }));
        return data;
    }
    /**
     * Given two members, add them to the noMerge fields of each other.
     * @param memberOneId ID of the first member
     * @param memberTwoId ID of the second member
     * @returns Success/Error message
     */
    async addToNoMerge(memberOneId, memberTwoId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const txOptions = { ...this.options, transaction };
        try {
            await memberRepository_1.default.addNoMerge(memberOneId, memberTwoId, txOptions);
            await memberRepository_1.default.addNoMerge(memberTwoId, memberOneId, txOptions);
            // Removes from either order of the pair
            await memberRepository_1.default.removeToMerge(memberOneId, memberTwoId, txOptions);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const projectGroupSegmentIds = await (0, segments_1.getMembersCommonProjectGroupSegmentIds)(qx, [
            memberOneId,
            memberTwoId,
        ]);
        // Precomputed per-project-group counts are only refreshed by cron every few hours.
        // Decrement here so no-merge from the UI is reflected immediately.
        await (0, segments_1.decrementMemberMergeSuggestionCounts)(qx, projectGroupSegmentIds);
        return { status: 200 };
    }
    async update(id, data, { syncToOpensearch = true, manualChange = false, invalidateCache = false, } = {}) {
        var _a, _b;
        let transaction;
        try {
            const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
            transaction = repoOptions.transaction;
            if (data.displayName) {
                data.displayName = (0, common_1.getProperDisplayName)(data.displayName);
            }
            if (data.attributes) {
                if (!(0, common_1.hasAttributeValue)(data.attributes.country)) {
                    const location = (0, common_1.getAttributeValue)(data.attributes.location);
                    const country = (0, common_1.getCountry)(location);
                    if (country) {
                        data.attributes.country = {
                            ...data.attributes.country,
                            system: country,
                            default: (_b = (_a = data.attributes.country) === null || _a === void 0 ? void 0 : _a.default) !== null && _b !== void 0 ? _b : country,
                        };
                    }
                }
            }
            const record = await memberRepository_1.default.update(id, data, repoOptions, {
                manualChange,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Invalidate member query cache after update
            // Pass invalidateCache from options to control whether to clear list caches
            await (0, common_services_1.invalidateMemberQueryCache)(this.options.redis, [id], invalidateCache);
            await (0, common_services_1.signalMemberUpdate)(this.options.temporal, id, {
                memberOrganizationIds: (data.organizations || []).map((o) => o.id),
                syncToOpensearch,
            });
            return record;
        }
        catch (error) {
            if (error.name && error.name.includes('Sequelize')) {
                this.log.error(error, {
                    query: error.sql,
                    errorMessage: error.original.message,
                }, 'Error during member update!');
            }
            else {
                this.log.error(error, 'Error during member update!');
            }
            if (transaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'member');
            throw error;
        }
    }
    async destroyBulk(ids) {
        const normalizedIds = MemberService.normalizeIds(ids);
        if (normalizedIds.length === 0) {
            return;
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const searchSyncService = new searchSyncService_1.default(this.options);
        try {
            await memberRepository_1.default.destroyBulk(normalizedIds, {
                ...this.options,
                transaction,
            }, true);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Invalidate member query cache after bulk delete
            // Pass invalidateAll=true to also clear list caches since deletion affects list views
            await (0, common_services_1.invalidateMemberQueryCache)(this.options.redis, normalizedIds, true);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
        for (const id of normalizedIds) {
            await searchSyncService.triggerRemoveMember(id);
        }
    }
    async destroyAll(ids) {
        const normalizedIds = MemberService.normalizeIds(ids);
        if (normalizedIds.length === 0) {
            return;
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const searchSyncService = new searchSyncService_1.default(this.options);
        try {
            for (const id of normalizedIds) {
                await memberRepository_1.default.destroy(id, {
                    ...this.options,
                    transaction,
                }, true);
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Invalidate member query cache after deletion
            // Pass invalidateAll=true to also clear list caches since deletion affects list views
            await (0, common_services_1.invalidateMemberQueryCache)(this.options.redis, normalizedIds, true);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
        for (const id of normalizedIds) {
            await searchSyncService.triggerRemoveMember(id);
        }
    }
    async findById(id, segmentId, include = {}, includeAllAttributes = false) {
        return memberRepository_1.default.findById(id, this.options, {
            segmentId,
        }, include, includeAllAttributes);
    }
    async findAllAutocomplete(data) {
        const qx = (0, sequelizeQueryExecutor_1.optionsQx)(this.options);
        const bgQx = (0, sequelizeQueryExecutor_1.optionsBgQx)(this.options);
        return (0, members_1.queryMembersAdvanced)(qx, bgQx, this.options.redis, {
            filter: data.filter,
            offset: data.offset,
            orderBy: data.orderBy,
            limit: data.limit,
            segmentId: data.segments[0],
            include: {
                segments: true,
            },
        });
    }
    async query(data, exportMode = false) {
        const memberAttributeSettings = (await memberAttributeSettingsRepository_1.default.findAndCountAll({}, this.options)).rows.filter((setting) => setting.type !== types_1.MemberAttributeType.SPECIAL);
        const segmentId = (data.segments || [])[0];
        if (!segmentId) {
            throw new common_1.Error400(this.options.language, 'member.segmentsRequired');
        }
        const qx = (0, sequelizeQueryExecutor_1.optionsQx)(this.options);
        const bgQx = (0, sequelizeQueryExecutor_1.optionsBgQx)(this.options);
        return (0, members_1.queryMembersAdvanced)(qx, bgQx, this.options.redis, {
            ...data,
            segmentId,
            attributesSettings: memberAttributeSettings,
            include: {
                memberOrganizations: true,
                lfxMemberships: true,
                identities: true,
                attributes: true,
                maintainers: true,
            },
            exportMode,
        });
    }
    async queryForCsv(data) {
        var _a;
        data.limit = 10000000000000;
        const found = await this.query(data, true);
        const relations = [{ relation: 'organizations', attributes: ['name'] }];
        for (const relation of relations) {
            for (const member of found.rows) {
                member[relation.relation] = (_a = member[relation.relation]) === null || _a === void 0 ? void 0 : _a.map((i) => ({
                    id: i.id,
                    ...lodash_1.default.pick(i, relation.attributes),
                }));
            }
        }
        return found;
    }
    async findMembersWithMergeSuggestions(args) {
        return memberRepository_1.default.findMembersWithMergeSuggestions(args, this.options);
    }
    async findMembersWithBotSuggestions(args) {
        var _a, _b;
        const segments = sequelizeRepository_1.default.getSegmentIds(this.options);
        const segmentId = (segments === null || segments === void 0 ? void 0 : segments.length) > 0 ? segments[0] : null;
        if (!segmentId) {
            throw new common_1.Error400(this.options.language, 'member.segmentsRequired');
        }
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, members_1.fetchMemberBotSuggestionsBySegment)(qx, segmentId, (_a = args.limit) !== null && _a !== void 0 ? _a : 10, (_b = args.offset) !== null && _b !== void 0 ? _b : 0);
    }
}
MemberService.MEMBER_MERGE_FIELDS = [
    'id',
    'reach',
    'tasks',
    'joinedAt',
    'tenantId',
    'attributes',
    'displayName',
    'affiliations',
    'contributions',
    'manuallyCreated',
    'manuallyChangedFields',
];
exports.default = MemberService;
//# sourceMappingURL=memberService.js.map