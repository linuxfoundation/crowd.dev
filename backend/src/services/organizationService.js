"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const lodash_1 = __importDefault(require("lodash"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const lfx_memberships_1 = require("@crowd/data-access-layer/src/lfx_memberships");
const member_organization_affiliation_1 = require("@crowd/data-access-layer/src/member-organization-affiliation");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const repo_1 = require("@crowd/data-access-layer/src/mergeActions/repo");
const organizations_1 = require("@crowd/data-access-layer/src/organizations");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const temporal_1 = require("@crowd/temporal");
const types_1 = require("@crowd/types");
const memberOrganizationRepository_1 = __importDefault(require("@/database/repositories/memberOrganizationRepository"));
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const getObjectWithoutKey_1 = __importDefault(require("@/utils/getObjectWithoutKey"));
const mergeActionsRepository_1 = require("../database/repositories/mergeActionsRepository");
const organizationRepository_1 = __importDefault(require("../database/repositories/organizationRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const telemetryTrack_1 = __importDefault(require("../segment/telemetryTrack"));
const mergeFunctions_1 = require("./helpers/mergeFunctions");
const searchSyncService_1 = __importDefault(require("./searchSyncService"));
class OrganizationService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async unmergePreview(organizationId, identity, revertPreviousMerge = false) {
        try {
            const organization = await organizationRepository_1.default.findById(organizationId, this.options);
            const identities = await organizationRepository_1.default.getIdentities([organizationId], this.options);
            const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
            const attributes = organizationRepository_1.default.convertOrgAttributesForDisplay(await (0, organizations_1.findOrgAttributes)(qx, organization.id));
            if (!identities.some((i) => i.platform === identity.platform &&
                i.value === identity.value &&
                i.type === identity.type &&
                i.verified === identity.verified)) {
                throw new Error(`Organization doesn't have the identity sent to be unmerged!`);
            }
            organization.identities = identities;
            if (revertPreviousMerge) {
                const mergeAction = await mergeActionsRepository_1.MergeActionsRepository.findMergeBackup(organizationId, types_1.MergeActionType.ORG, identity, this.options);
                if (!mergeAction) {
                    throw new Error('No previous merge action found to revert for organization!');
                }
                const primaryBackup = mergeAction.unmergeBackup.primary;
                const secondaryBackup = mergeAction.unmergeBackup.secondary;
                // Construct primary organization with best effort
                for (const key of OrganizationService.ORGANIZATION_MERGE_FIELDS) {
                    if (primaryBackup[key] !== organization[key] &&
                        secondaryBackup[key] === organization[key]) {
                        organization[key] = primaryBackup[key] || null;
                    }
                }
                // Remove identities coming from secondary backup
                organization.identities = organization.identities.filter((i) => !secondaryBackup.identities.some((s) => s.platform === i.platform &&
                    s.value === i.value &&
                    s.type === i.type &&
                    s.verified === i.verified));
                return {
                    mergeActionId: mergeAction.id,
                    primary: {
                        ...lodash_1.default.pick(organization, OrganizationService.ORGANIZATION_MERGE_FIELDS),
                        identities: organization.identities,
                        attributes,
                        activityCount: primaryBackup.activityCount,
                        memberCount: primaryBackup.memberCount,
                    },
                    secondary: secondaryBackup,
                };
            }
            // Identity extraction preview will be generated if revertMerge flag is not set
            const secondaryIdentities = [identity];
            const primaryIdentities = organization.identities.filter((i) => !secondaryIdentities.some((s) => s.platform === i.platform &&
                s.value === i.value &&
                s.type === i.type &&
                s.verified === i.verified));
            if (primaryIdentities.length === 0) {
                throw new common_1.Error400(this.options.language, 'organization.unmerge.errors.cannotExtractSingleIdentity');
            }
            let secondaryMemberCount;
            let secondaryActivityCount;
            // we can deduce the activity count and member count if primary member doesn't have an identity with same platform as extracted identity
            if (primaryIdentities.some((i) => i.platform === identity.platform)) {
                secondaryActivityCount = 0;
                secondaryMemberCount = 0;
            }
            else {
                // find activity count & member count by using activity platform
                secondaryActivityCount = await organizationRepository_1.default.getActivityCountInPlatform(organizationId, identity.platform, this.options);
                secondaryMemberCount = await organizationRepository_1.default.getMemberCountInPlatform(organizationId, identity.platform, this.options);
            }
            // clean up linkedin identity value
            if (identity.platform === 'linkedin') {
                identity.value = identity.value.split(':').pop();
            }
            return {
                primary: {
                    ...lodash_1.default.pick(organization, OrganizationService.ORGANIZATION_MERGE_FIELDS),
                    identities: primaryIdentities,
                    attributes,
                    memberCount: organization.memberCount - secondaryMemberCount,
                    activityCount: organization.activityCount - secondaryActivityCount,
                },
                secondary: {
                    id: (0, crypto_1.randomUUID)(),
                    identities: secondaryIdentities,
                    displayName: identity.value,
                    attributes: {
                        name: {
                            default: identity.value,
                            custom: [identity.value],
                        },
                    },
                    activityCount: secondaryActivityCount,
                    memberCount: secondaryMemberCount,
                    isTeamOrganization: false,
                },
            };
        }
        catch (err) {
            this.options.log.error(err, 'Error while generating unmerge/identity extraction preview!');
            throw err;
        }
    }
    async canRevertMerge(organizationId, identity) {
        try {
            // Get the identities of the organization
            const organizationIdentities = await organizationRepository_1.default.getIdentities([organizationId], this.options);
            // Check if the organization has the identity to be unmerged
            if (!organizationIdentities.some((i) => i.platform === identity.platform &&
                i.value === identity.value &&
                i.type === identity.type &&
                i.verified === identity.verified)) {
                throw new Error(`Organization doesn't have the identity sent to be unmerged!`);
            }
            // Check if there was a previous merge involving this identity
            const mergeAction = await mergeActionsRepository_1.MergeActionsRepository.findMergeBackup(organizationId, types_1.MergeActionType.ORG, identity, this.options);
            if (!mergeAction) {
                return false;
            }
            const secondaryBackup = mergeAction.unmergeBackup.secondary;
            // Check if the primary organization would still have identities after reverting
            const remainingIdentitiesInCurrentOrg = organizationIdentities.filter((i) => !secondaryBackup.identities.some((s) => s.platform === i.platform && s.value === i.value && s.type === identity.type));
            return remainingIdentitiesInCurrentOrg.length > 0;
        }
        catch (err) {
            this.options.log.error(err, 'Error while checking if organization merge can be reverted!');
            throw err;
        }
    }
    async unmerge(organizationId, payload) {
        let tx;
        try {
            const { organization, secondaryOrganization } = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.organizationUnmergeAction)(organizationId, async (captureOldState, captureNewState) => {
                var _a;
                const organization = await organizationRepository_1.default.findById(organizationId, this.options);
                captureOldState({
                    primary: organization,
                });
                const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
                tx = repoOptions.transaction;
                // remove identities in secondary organization from primary
                await organizationRepository_1.default.removeIdentitiesFromOrganization(organizationId, payload.secondary.identities.filter((i) => i.verified === undefined || // backwards compatibility for old identity backups
                    i.verified === true ||
                    (i.verified === false &&
                        !payload.primary.identities.some((pi) => pi.verified === false &&
                            pi.platform === i.platform &&
                            pi.value === i.value &&
                            pi.type === i.type))), repoOptions);
                // create the secondary org
                const secondaryOrganization = await organizationRepository_1.default.create(payload.secondary, repoOptions);
                await (0, repo_1.addMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(this.options), types_1.MergeActionType.ORG, organizationId, secondaryOrganization.id, types_1.MergeActionStep.UNMERGE_STARTED, types_1.MergeActionState.IN_PROGRESS, undefined, (_a = this.options.currentUser) === null || _a === void 0 ? void 0 : _a.id);
                if (payload.mergeActionId) {
                    const mergeAction = await mergeActionsRepository_1.MergeActionsRepository.findById(payload.mergeActionId, this.options);
                    if (mergeAction.unmergeBackup.secondary.memberOrganizations.length > 0) {
                        for (const role of mergeAction.unmergeBackup.secondary.memberOrganizations) {
                            await (0, data_access_layer_1.addMemberRole)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), {
                                ...role,
                                organizationId: secondaryOrganization.id,
                            });
                        }
                        const memberOrganizations = await memberOrganizationRepository_1.default.findRolesInOrganization(organization.id, repoOptions);
                        const primaryUnmergedRoles = await (0, common_services_1.unmergeRoles)(memberOrganizations, mergeAction.unmergeBackup.primary.memberOrganizations, mergeAction.unmergeBackup.secondary.memberOrganizations, types_1.MemberRoleUnmergeStrategy.SAME_ORGANIZATION);
                        // check if anything to delete in primary
                        const rolesToDelete = memberOrganizations.filter((r) => r.source !== 'ui' &&
                            !primaryUnmergedRoles.some((pr) => pr.memberId === r.memberId &&
                                pr.title === r.title &&
                                pr.dateStart === r.dateStart &&
                                pr.dateEnd === r.dateEnd));
                        for (const role of rolesToDelete) {
                            await (0, data_access_layer_1.removeMemberRole)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), role);
                        }
                    }
                }
                // delete identity related stuff, we already moved these
                delete payload.primary.identities;
                captureNewState({
                    primary: payload.primary,
                    secondary: secondaryOrganization,
                });
                // update rest of the primary org fields
                await organizationRepository_1.default.update(organizationId, payload.primary, repoOptions, false, false);
                // add primary and secondary to no merge so they don't get suggested again
                await organizationRepository_1.default.addNoMerge(organizationId, secondaryOrganization.id, repoOptions);
                // trigger entity-merging-worker to move activities in the background
                await sequelizeRepository_1.default.commitTransaction(tx);
                return { organization, secondaryOrganization };
            }));
            await (0, repo_1.setMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(this.options), types_1.MergeActionType.ORG, organizationId, secondaryOrganization.id, {
                step: types_1.MergeActionStep.UNMERGE_SYNC_DONE,
            });
            // responsible for moving organization's activities, syncing to opensearch afterwards, recalculating activity.organizationIds and notifying frontend via websockets
            await this.options.temporal.workflow.start('finishOrganizationUnmerging', {
                taskQueue: 'entity-merging',
                workflowId: `finishOrganizationUnmerging/${organization.id}/${secondaryOrganization.id}`,
                retry: {
                    maximumAttempts: 10,
                },
                args: [
                    organization.id,
                    secondaryOrganization.id,
                    organization.displayName,
                    secondaryOrganization.displayName,
                    this.options.currentUser.id,
                ],
            });
        }
        catch (err) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw err;
        }
    }
    async mergeSync(originalId, toMergeId, segmentId) {
        this.options.log.info({ originalId, toMergeId }, 'Merging organizations!');
        const removeExtraFields = (organization) => (0, getObjectWithoutKey_1.default)(organization, [
            'activityCount',
            'memberCount',
            'activeOn',
            'segments',
            'lastActive',
            'joinedAt',
            'identities',
        ]);
        let tx;
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const mergeActions = await (0, repo_1.queryMergeActions)(qx, {
            fields: ['id', 'state'],
            filter: {
                and: [
                    {
                        state: {
                            eq: types_1.MergeActionState.IN_PROGRESS,
                        },
                    },
                    {
                        or: [
                            { primaryId: { eq: originalId } },
                            { secondaryId: { eq: originalId } },
                            { primaryId: { eq: toMergeId } },
                            { secondaryId: { eq: toMergeId } },
                        ],
                    },
                ],
            },
            limit: 1,
            orderBy: '"updatedAt" DESC',
        });
        // prevent multiple merge operations
        if (mergeActions.length > 0) {
            throw new common_1.Error409(this.options.language, 'merge.errors.multiple', mergeActions[0].state);
        }
        let orgAffiliationChanges = false;
        try {
            const { original, toMerge } = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.organizationMergeAction)(originalId, async (captureOldState, captureNewState) => {
                var _a;
                this.log.info('[Merge Organizations] - Finding organizations!');
                let original = await organizationRepository_1.default.findById(originalId, this.options, segmentId);
                let toMerge = await organizationRepository_1.default.findById(toMergeId, this.options, segmentId);
                const originalWithLfxMembership = await (0, lfx_memberships_1.hasLfxMembership)(qx, {
                    organizationId: originalId,
                });
                const toMergeWithLfxMembership = await (0, lfx_memberships_1.hasLfxMembership)(qx, {
                    organizationId: toMergeId,
                });
                if (originalWithLfxMembership && toMergeWithLfxMembership) {
                    await organizationRepository_1.default.addNoMerge(originalId, toMergeId, this.options);
                    this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Skipping merge of two LFX membership orgs!');
                    return {
                        status: 203,
                        mergedId: originalId,
                    };
                }
                if (toMergeWithLfxMembership) {
                    throw new common_1.Error400(this.options.language, 'merge.errors.mergeLfxSecondary');
                }
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Found organizations!');
                captureOldState({
                    primary: original,
                    secondary: toMerge,
                });
                const backup = {
                    primary: {
                        ...lodash_1.default.pick(original, OrganizationService.ORGANIZATION_MERGE_FIELDS),
                        identities: await organizationRepository_1.default.getIdentities([originalId], this.options),
                        memberOrganizations: await memberOrganizationRepository_1.default.findRolesInOrganization(originalId, this.options),
                    },
                    secondary: {
                        ...lodash_1.default.pick(toMerge, OrganizationService.ORGANIZATION_MERGE_FIELDS),
                        identities: await organizationRepository_1.default.getIdentities([toMergeId], this.options),
                        memberOrganizations: await memberOrganizationRepository_1.default.findRolesInOrganization(toMergeId, this.options),
                    },
                };
                if (original.id === toMerge.id) {
                    return {
                        status: 203,
                        mergedId: originalId,
                    };
                }
                // not using transaction here on purpose,
                // so this change is visible until we finish
                await (0, repo_1.addMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(this.options), types_1.MergeActionType.ORG, originalId, toMergeId, types_1.MergeActionStep.MERGE_STARTED, types_1.MergeActionState.IN_PROGRESS, backup, (_a = this.options.currentUser) === null || _a === void 0 ? void 0 : _a.id);
                const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
                tx = repoOptions.transaction;
                const allIdentities = await organizationRepository_1.default.getIdentities([originalId, toMergeId], repoOptions);
                const originalIdentities = allIdentities.filter((i) => i.organizationId === originalId);
                const toMergeIdentities = allIdentities.filter((i) => i.organizationId === toMergeId);
                const identitiesToMove = [];
                const identitiesToUpdate = [];
                for (const identity of toMergeIdentities) {
                    const existing = originalIdentities.find((i) => i.platform === identity.platform &&
                        i.type === identity.type &&
                        i.value === identity.value);
                    if (!existing) {
                        identitiesToMove.push(identity);
                    }
                    else if (!existing.verified && identity.verified) {
                        identitiesToUpdate.push(identity);
                    }
                }
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Moving identities between organizations!');
                // move non existing identities
                await organizationRepository_1.default.moveIdentitiesBetweenOrganizations(toMergeId, originalId, identitiesToMove, repoOptions);
                // remove identities from secondary that we gonna verify in primary
                await organizationRepository_1.default.removeIdentitiesFromOrganization(toMergeId, identitiesToUpdate, repoOptions);
                // verify existing unverified identities
                for (const identity of identitiesToUpdate) {
                    await organizationRepository_1.default.updateIdentity(originalId, identity, repoOptions);
                }
                // remove aggregate fields and relationships
                original = removeExtraFields(original);
                toMerge = removeExtraFields(toMerge);
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Generating merge object!');
                // Performs a merge and returns the fields that were changed so we can update
                const toUpdate = await OrganizationService.organizationsMerge(original, toMerge);
                captureNewState({ primary: toUpdate });
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Generating merge object done!');
                const txService = new OrganizationService(repoOptions);
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Updating original organisation!');
                // check if website is being updated, if yes we need to set toMerge.website to null before doing the update
                // because of website unique constraint
                if (toUpdate.website && toUpdate.website === toMerge.website) {
                    await txService.update(toMergeId, { website: null }, false, false);
                }
                // Update original organization
                await txService.update(originalId, toUpdate, false, false, false, true);
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Updating original organisation done!');
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Moving members to original organisation!');
                const { shouldRecalculateAffiliations } = await (0, data_access_layer_1.moveMembersBetweenOrganizations)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), toMergeId, originalId);
                if (shouldRecalculateAffiliations) {
                    orgAffiliationChanges = true;
                }
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Moving members to original organisation done!');
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Including original organisation into secondary organisation segments!');
                const secondMemberSegments = await organizationRepository_1.default.getOrganizationSegments(toMergeId, repoOptions);
                if (secondMemberSegments.length > 0) {
                    await (0, organizations_1.addOrgsToSegments)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), secondMemberSegments.map((s) => s.id), [originalId]);
                }
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Including original organisation into secondary organisation segments done!');
                await sequelizeRepository_1.default.commitTransaction(tx);
                this.log.info({ originalId, toMergeId }, '[Merge Organizations] - Transaction commited!');
                await (0, repo_1.setMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(this.options), types_1.MergeActionType.ORG, originalId, toMergeId, {
                    step: types_1.MergeActionStep.MERGE_SYNC_DONE,
                });
                return { original, toMerge };
            }));
            const projectGroupSegmentIds = await (0, segments_1.getOrganizationsCommonProjectGroupSegmentIds)(qx, [
                originalId,
                toMergeId,
            ]);
            // Precomputed per-project-group counts are only refreshed by cron every few hours.
            // Decrement here so merges from the UI are reflected immediately.
            await (0, segments_1.decrementOrganizationMergeSuggestionCounts)(qx, projectGroupSegmentIds);
            await this.options.temporal.workflow.start('finishOrganizationMerging', {
                taskQueue: 'entity-merging',
                workflowId: `finishOrganizationMerging/${originalId}/${toMergeId}`,
                retry: {
                    maximumAttempts: 10,
                },
                args: [
                    originalId,
                    toMergeId,
                    original.displayName,
                    toMerge.displayName,
                    orgAffiliationChanges,
                    this.options.currentUser.id,
                ],
            });
            this.options.log.info({ originalId, toMergeId }, 'Organizations merged!');
            return {
                status: 200,
                mergedId: originalId,
            };
        }
        catch (err) {
            this.options.log.error(err, 'Error while merging organizations!', {
                originalId,
                toMergeId,
            });
            await (0, repo_1.setMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(this.options), types_1.MergeActionType.ORG, originalId, toMergeId, {
                state: types_1.MergeActionState.ERROR,
            });
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw err;
        }
    }
    static organizationsMerge(originalObject, toMergeObject) {
        return (0, common_1.mergeObjects)(originalObject, toMergeObject, {
            importHash: mergeFunctions_1.keepPrimary,
            createdAt: mergeFunctions_1.keepPrimary,
            updatedAt: mergeFunctions_1.keepPrimary,
            deletedAt: mergeFunctions_1.keepPrimary,
            tenantId: mergeFunctions_1.keepPrimary,
            createdById: mergeFunctions_1.keepPrimary,
            updatedById: mergeFunctions_1.keepPrimary,
            isTeamOrganization: mergeFunctions_1.keepPrimaryIfExists,
            isAffiliationBlocked: mergeFunctions_1.keepPrimary,
            lastEnrichedAt: mergeFunctions_1.keepPrimary,
            searchSyncedAt: mergeFunctions_1.keepPrimary,
            manuallyCreated: mergeFunctions_1.keepPrimary,
            // default attributes
            description: mergeFunctions_1.keepPrimaryIfExists,
            logo: mergeFunctions_1.keepPrimaryIfExists,
            tags: mergeFunctions_1.mergeUniqueStringArrayItems,
            employees: mergeFunctions_1.keepPrimaryIfExists,
            revenueRange: mergeFunctions_1.keepPrimaryIfExists,
            location: mergeFunctions_1.keepPrimaryIfExists,
            type: mergeFunctions_1.keepPrimaryIfExists,
            size: mergeFunctions_1.keepPrimaryIfExists,
            headline: mergeFunctions_1.keepPrimaryIfExists,
            industry: mergeFunctions_1.keepPrimaryIfExists,
            founded: mergeFunctions_1.keepPrimaryIfExists,
            displayName: mergeFunctions_1.keepPrimary,
            employeeChurnRate: mergeFunctions_1.keepPrimaryIfExists,
            employeeGrowthRate: mergeFunctions_1.keepPrimaryIfExists,
        });
    }
    async addToNoMerge(organizationId, noMergeId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const txOptions = { ...this.options, transaction };
        try {
            await organizationRepository_1.default.addNoMerge(organizationId, noMergeId, txOptions);
            await organizationRepository_1.default.addNoMerge(noMergeId, organizationId, txOptions);
            await organizationRepository_1.default.removeToMerge(organizationId, noMergeId, txOptions);
            await organizationRepository_1.default.removeToMerge(noMergeId, organizationId, txOptions);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const projectGroupSegmentIds = await (0, segments_1.getOrganizationsCommonProjectGroupSegmentIds)(qx, [
            organizationId,
            noMergeId,
        ]);
        // Precomputed per-project-group counts are only refreshed by cron every few hours.
        // Decrement here so no-merge from the UI is reflected immediately.
        await (0, segments_1.decrementOrganizationMergeSuggestionCounts)(qx, projectGroupSegmentIds);
    }
    async createOrUpdate(data, syncOptions = { doSync: true, mode: types_1.SyncMode.ASYNCHRONOUS }) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const txOptions = { ...this.options, transaction };
        if (!data.identities) {
            data.identities = [];
        }
        if (data.name && data.identities.length === 0) {
            data.identities = [
                {
                    value: data.name,
                    type: types_1.OrganizationIdentityType.USERNAME,
                    platform: 'custom',
                    verified: true,
                    source: 'ui',
                    sourceId: null,
                    integrationId: null,
                },
            ];
            delete data.name;
        }
        const verifiedIdentities = data.identities.filter((i) => i.verified);
        if (verifiedIdentities.length === 0) {
            const message = `Missing organization identity while creating/updating organization!`;
            this.log.error(data, message);
            throw new Error(message);
        }
        try {
            // Normalize the website identities
            for (const i of data.identities.filter((i) => [
                types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                types_1.OrganizationIdentityType.ALTERNATIVE_DOMAIN,
            ].includes(i.type))) {
                i.value = (0, common_1.normalizeHostname)(i.value);
            }
            let record;
            const existing = await organizationRepository_1.default.findByVerifiedIdentities(verifiedIdentities, txOptions);
            const qx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
            if (existing) {
                record = existing;
                if (record.attributes) {
                    const defaultColumns = await organizationRepository_1.default.updateOrgAttributes(record.id, record, txOptions);
                    if (Object.keys(defaultColumns).length > 0) {
                        record = await organizationRepository_1.default.update(existing.id, defaultColumns, txOptions);
                    }
                }
                await (0, organizations_1.upsertOrgIdentities)(qx, record.id, data.identities);
            }
            else {
                if (data.displayName) {
                    // Block organization affiliation if a LF segment (project, subproject, or project group)
                    // has the same name as the organization when creating one.
                    const lfSegment = await (0, segments_1.findLfSegmentByName)(qx, data.displayName);
                    if (lfSegment) {
                        this.log.info({ displayName: data.displayName }, 'Found segment with the same name as the organization, blocking affiliation!');
                        data.isAffiliationBlocked = true;
                    }
                }
                record = await organizationRepository_1.default.create(data, txOptions);
                (0, telemetryTrack_1.default)('Organization created', {
                    id: record.id,
                    createdAt: record.createdAt,
                }, txOptions);
                for (const i of data.identities) {
                    await organizationRepository_1.default.addIdentity(record.id, i, txOptions);
                }
                if (data.attributes) {
                    const defaultColumns = await organizationRepository_1.default.updateOrgAttributes(record.id, data, txOptions);
                    if (Object.keys(defaultColumns).length > 0) {
                        record = await organizationRepository_1.default.update(record.id, defaultColumns, txOptions);
                    }
                }
            }
            const result = await organizationRepository_1.default.findById(record.id, txOptions);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            if (syncOptions.doSync) {
                await this.startOrganizationUpdateWorkflow(record.id, {
                    syncToOpensearch: syncOptions.doSync,
                });
            }
            return result;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'organization');
            throw error;
        }
    }
    async findOrganizationsWithMergeSuggestions(args) {
        return organizationRepository_1.default.findOrganizationsWithMergeSuggestions(args, this.options);
    }
    async update(id, data, overrideIdentities = false, syncToOpensearch = true, manualChange = false, skipAffiliationBlockUpdate = false) {
        let tx;
        let recalculateAffiliations = false;
        try {
            const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
            const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
            tx = repoOptions.transaction;
            // findOrgById to get the existing organization
            const existingOrg = await (0, organizations_1.findOrgById)(qx, id, [
                organizations_1.OrganizationField.ID,
                organizations_1.OrganizationField.IS_AFFILIATION_BLOCKED,
            ]);
            if (!existingOrg) {
                throw new common_1.Error404(this.options.language, 'Organization not found!');
            }
            if (data.identities) {
                // Normalize the website identities
                for (const i of data.identities.filter((i) => [
                    types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                    types_1.OrganizationIdentityType.ALTERNATIVE_DOMAIN,
                ].includes(i.type))) {
                    i.value = (0, common_1.normalizeHostname)(i.value);
                }
                const existingIdentities = await organizationRepository_1.default.getIdentities(id, repoOptions);
                const toUpdate = [];
                const toCreate = [];
                for (const i of data.identities) {
                    const existing = existingIdentities.find((ei) => ei.value === i.value && ei.platform === i.platform && ei.type === i.type);
                    if (!existing) {
                        toCreate.push(i);
                    }
                    else if (existing && existing.verified !== i.verified) {
                        toUpdate.push(i);
                    }
                }
                const toUpdateVerified = toUpdate.filter((i) => i.verified);
                const verified = toUpdateVerified.concat(toCreate);
                if (verified.length > 0) {
                    const existing = await organizationRepository_1.default.findByVerifiedIdentities(verified, repoOptions);
                    if (existing && existing.id !== id) {
                        throw new Error(`Organization identities ${JSON.stringify(verified)} already exist in another organization!`);
                    }
                }
                if (toCreate.length > 0) {
                    for (const i of toCreate) {
                        // add the identity
                        await organizationRepository_1.default.addIdentity(id, i, repoOptions);
                    }
                }
                if (toUpdate.length > 0) {
                    for (const i of toUpdate) {
                        // update the identity
                        await organizationRepository_1.default.updateIdentity(id, i, repoOptions);
                    }
                }
            }
            const record = await organizationRepository_1.default.update(id, data, repoOptions, overrideIdentities, manualChange);
            if (!skipAffiliationBlockUpdate &&
                typeof data.isAffiliationBlocked === 'boolean' &&
                data.isAffiliationBlocked !== existingOrg.isAffiliationBlocked) {
                await (0, member_organization_affiliation_1.applyOrganizationAffiliationPolicyToMembers)(qx, record.id, !data.isAffiliationBlocked);
                if (data.isAffiliationBlocked) {
                    await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, { organizationId: record.id });
                }
                recalculateAffiliations = true;
            }
            await sequelizeRepository_1.default.commitTransaction(tx);
            if (syncToOpensearch || recalculateAffiliations) {
                await this.startOrganizationUpdateWorkflow(record.id, {
                    syncToOpensearch,
                    recalculateAffiliations,
                });
            }
            return record;
        }
        catch (error) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'organization');
            throw error;
        }
    }
    async destroyAll(ids) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            for (const id of ids) {
                await organizationRepository_1.default.destroy(id, {
                    ...this.options,
                    transaction,
                }, true);
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            const searchSyncService = new searchSyncService_1.default(this.options, types_1.SyncMode.ASYNCHRONOUS);
            for (const id of ids) {
                await searchSyncService.triggerRemoveOrganization(id);
            }
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async findById(id, segmentId) {
        return organizationRepository_1.default.findById(id, this.options, segmentId);
    }
    async findAllAutocomplete(data) {
        const { filter, orderBy, limit, offset, segments } = data;
        return organizationRepository_1.default.findAndCountAll({
            filter,
            orderBy,
            limit,
            offset,
            segmentId: segments.length > 0 ? segments[0] : undefined,
            fields: ['id', 'segmentId', 'displayName', 'memberCount', 'activityCount', 'logo'],
            include: { aggregates: false, identities: false, lfxMemberships: true },
        }, this.options);
    }
    async findAndCountAll(args) {
        return organizationRepository_1.default.findAndCountAll(args, this.options);
    }
    async findByIds(ids) {
        return organizationRepository_1.default.findByIds(ids, this.options);
    }
    async query(data) {
        const { filter: rawFilter, orderBy, limit, offset, segments, search: rawSearch } = data;
        const searchTerm = typeof rawSearch === 'string' && rawSearch.trim() ? rawSearch.trim() : undefined;
        // Strip frontend-state keys that are never valid filter columns or operators.
        // These can appear when the raw Pinia filter state is sent instead of the
        // processed output of buildApiFilter.
        const { search: _s, relation: _r, order: _o, settings: _st, ...filter } = rawFilter !== null && rawFilter !== void 0 ? rawFilter : {};
        return organizationRepository_1.default.findAndCountAll({
            filter,
            search: searchTerm,
            orderBy,
            limit,
            offset,
            segmentId: segments.length > 0 ? segments[0] : undefined,
            fields: ['id', 'segmentId', 'displayName', 'memberCount', 'activityCount', 'logo'],
            include: { aggregates: true, identities: true, lfxMemberships: true },
        }, this.options);
    }
    async listOrganizationsAcrossAllSegments(args) {
        const { filter, orderBy, limit, offset } = args;
        return organizationRepository_1.default.findAndCountAll({
            filter,
            orderBy,
            limit,
            offset,
            segmentId: undefined,
            fields: ['id', 'logo', 'displayName', 'isTeamOrganization'],
            include: { aggregates: true, identities: true, segments: true, lfxMemberships: true },
        }, this.options);
    }
    async destroyBulk(ids) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            await organizationRepository_1.default.destroyBulk(ids, {
                ...this.options,
                transaction,
            }, true);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            const searchSyncService = new searchSyncService_1.default(this.options);
            for (const id of ids) {
                await searchSyncService.triggerRemoveOrganization(id);
            }
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async startOrganizationUpdateWorkflow(organizationId, { syncToOpensearch = false, recalculateAffiliations = false }) {
        await this.options.temporal.workflow.start('organizationUpdate', {
            taskQueue: 'profiles',
            workflowId: `${types_1.TemporalWorkflowId.ORGANIZATION_UPDATE}/${organizationId}`,
            workflowIdReusePolicy: temporal_1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
            retry: {
                maximumAttempts: 10,
            },
            args: [
                {
                    organization: {
                        id: organizationId,
                    },
                    recalculateAffiliations,
                    syncOptions: {
                        doSync: syncToOpensearch,
                    },
                },
            ],
        });
    }
}
OrganizationService.ORGANIZATION_MERGE_FIELDS = [
    'displayName',
    'description',
    'logo',
    'headline',
    'joinedAt',
    'isTeamOrganization',
    'isAffiliationBlocked',
    'manuallyCreated',
    'activityCount',
    'memberCount',
];
exports.default = OrganizationService;
//# sourceMappingURL=organizationService.js.map