"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const collections_1 = require("@crowd/data-access-layer/src/collections");
const member_organization_affiliation_1 = require("@crowd/data-access-layer/src/member-organization-affiliation");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const memberRepository_1 = __importDefault(require("../database/repositories/memberRepository"));
const segmentRepository_1 = __importDefault(require("../database/repositories/segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const collectionService_1 = require("./collectionService");
const organizationService_1 = __importDefault(require("./organizationService"));
class SegmentService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async update(id, data) {
        const segment = await this.findById(id);
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const segmentRepository = new segmentRepository_1.default({ ...this.options, transaction });
            // Validate name and slug uniqueness if being updated to prevent bypassing creation restrictions
            if (data.name || data.slug) {
                await this.validateUpdateDuplicates(id, segment, data, segmentRepository);
            }
            // make sure non-lf projects' slug are namespaced appropriately
            if (data.isLF === false)
                data.slug = (0, common_1.validateNonLfSlug)(data.slug);
            // do the update
            await segmentRepository.update(id, data);
            // update relation fields of parent objects
            if (!(0, segments_1.isSegmentSubproject)(segment) && (data.name || data.slug)) {
                await segmentRepository.updateChildrenBulk(segment, {
                    name: data.name,
                    slug: data.slug,
                    isLF: data.isLF,
                });
            }
            if ((0, segments_1.isSegmentSubproject)(segment) && data.slug && data.slug !== segment.slug) {
                const collectionService = new collectionService_1.CollectionService({ ...this.options, transaction });
                const projects = await collectionService.findInsightsProjectsBySegmentId(segment.id);
                if (projects.length > 0) {
                    const normalizedSlug = data.slug.replace(/^nonlf_/, '');
                    await collectionService.updateInsightsProject(projects[0].id, { slug: normalizedSlug });
                }
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return await this.findById(id);
        }
        catch (error) {
            if (error === null || error === void 0 ? void 0 : error.message.includes("must match its parent's isLF value")) {
                // No rollback needed here, check_segment_isLF_consistency() failed at commit due to a deferred constraint.
                throw new common_1.Error400(this.options.language, `settings.segments.errors.isLfNotMatchingParent`);
            }
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async createProjectGroup(data) {
        var _a;
        // project groups shouldn't have parentSlug or grandparentSlug
        if (data.parentSlug || data.grandparentSlug) {
            throw new Error(`Project groups can't have parent or grandparent segments.`);
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction });
        const collectionService = new collectionService_1.CollectionService({ ...this.options, transaction });
        const segmentRepository = new segmentRepository_1.default({ ...this.options, transaction });
        try {
            // Check for conflicts with existing segments
            await this.validateSegmentConflicts(segmentRepository, data.name, data.slug, types_1.SegmentLevel.PROJECT_GROUP, data.isLF);
            // create project group
            const projectGroup = await segmentRepository.create(data);
            await collectionService.createCollection({
                name: data.name,
                categoryId: null,
                description: '',
                slug: data.slug,
                starred: (_a = data.isLF) !== null && _a !== void 0 ? _a : false,
            });
            // create project counterpart
            const project = await segmentRepository.create({
                ...data,
                parentSlug: data.slug,
                parentName: data.name,
                parentId: projectGroup.id,
            });
            // create subproject counterpart
            await this.createSubprojectInternal({
                ...data,
                parentSlug: data.slug,
                grandparentSlug: data.slug,
                parentName: data.name,
                grandparentName: data.name,
                parentId: project.id,
                grandparentId: projectGroup.id,
            }, qx, transaction);
            // Only apply project-org affiliation blocking for LF segments.
            // Use the persisted segment flag (not raw input) as the source of truth.
            const orgIds = projectGroup.isLF
                ? await this.blockOrganizationAffiliationIfSegmentNameMatches(projectGroup.name, transaction)
                : [];
            await sequelizeRepository_1.default.commitTransaction(transaction);
            if (orgIds.length > 0) {
                const organizationService = new organizationService_1.default(this.options);
                for (const orgId of orgIds) {
                    // Trigger org update workflow to recalculate affiliations
                    await organizationService.startOrganizationUpdateWorkflow(orgId, {
                        syncToOpensearch: true,
                        recalculateAffiliations: true,
                    });
                }
            }
            return await this.findById(projectGroup.id);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async createProject(data) {
        // project groups shouldn't have parentSlug or grandparentSlug
        if (data.grandparentSlug) {
            throw new Error(`Projects can't have grandparent segments.`);
        }
        if (!data.parentSlug) {
            throw new Error('Missing parentSlug. Projects must belong to a project group.');
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction });
        const segmentRepository = new segmentRepository_1.default({ ...this.options, transaction });
        const parent = await segmentRepository.findBySlug(data.parentSlug, types_1.SegmentLevel.PROJECT_GROUP);
        if (parent === null) {
            throw new Error(`Project group ${data.parentName} does not exist.`);
        }
        try {
            // Check for conflicts with existing segments
            await this.validateSegmentConflicts(segmentRepository, data.name, data.slug, types_1.SegmentLevel.PROJECT, data.isLF);
            if (parent.isLF !== data.isLF)
                throw new common_1.Error400(this.options.language, `settings.segments.errors.isLfNotMatchingParent`);
            if (data.isLF === false)
                data.slug = (0, common_1.validateNonLfSlug)(data.slug);
            // create project
            const project = await segmentRepository.create({ ...data, parentId: parent.id });
            // create subproject counterpart
            await this.createSubprojectInternal({
                ...data,
                parentSlug: data.slug,
                grandparentSlug: data.parentSlug,
                name: data.name,
                parentName: data.name,
                grandparentName: parent.name,
                parentId: project.id,
                grandparentId: parent.id,
            }, qx, transaction);
            // Only apply project-org affiliation blocking for LF segments.
            // Use the persisted segment flag (not raw input) as the source of truth.
            const orgIds = project.isLF
                ? await this.blockOrganizationAffiliationIfSegmentNameMatches(project.name, transaction)
                : [];
            await sequelizeRepository_1.default.commitTransaction(transaction);
            if (orgIds.length > 0) {
                const organizationService = new organizationService_1.default(this.options);
                for (const orgId of orgIds) {
                    // Trigger org update workflow to recalculate affiliations
                    await organizationService.startOrganizationUpdateWorkflow(orgId, {
                        syncToOpensearch: true,
                        recalculateAffiliations: true,
                    });
                }
            }
            return await this.findById(project.id);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async createSubproject(data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction });
        try {
            const subproject = await this.createSubprojectInternal(data, qx, transaction);
            const orgIds = subproject.isLF
                ? await this.blockOrganizationAffiliationIfSegmentNameMatches(subproject.name, transaction)
                : [];
            await sequelizeRepository_1.default.commitTransaction(transaction);
            if (orgIds.length > 0) {
                const organizationService = new organizationService_1.default(this.options);
                for (const orgId of orgIds) {
                    await organizationService.startOrganizationUpdateWorkflow(orgId, {
                        syncToOpensearch: true,
                        recalculateAffiliations: true,
                    });
                }
            }
            return await this.findById(subproject.id);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async createSubprojectInternal(data, qx, transaction) {
        if (!data.parentSlug) {
            throw new Error('Missing parentSlug. Subprojects must belong to a project.');
        }
        if (!data.grandparentSlug) {
            throw new Error('Missing grandparentSlug. Subprojects must belong to a project group.');
        }
        const segmentRepository = new segmentRepository_1.default({ ...this.options, transaction });
        const collectionService = new collectionService_1.CollectionService({ ...this.options, transaction });
        const parent = await segmentRepository.findBySlug(data.parentSlug, types_1.SegmentLevel.PROJECT);
        if (!parent) {
            throw new Error(`Project ${data.parentSlug} does not exist.`);
        }
        if (parent.isLF === false) {
            data.slug = (0, common_1.validateNonLfSlug)(data.slug);
        }
        // Check for conflicts with existing segments
        await this.validateSegmentConflicts(segmentRepository, data.name, data.slug, types_1.SegmentLevel.SUB_PROJECT, parent.isLF);
        const grandparent = await segmentRepository.findBySlug(data.grandparentSlug, types_1.SegmentLevel.PROJECT_GROUP);
        if (!grandparent) {
            throw new Error(`Project group ${data.grandparentSlug} does not exist.`);
        }
        const subproject = await segmentRepository.create({
            ...data,
            parentId: parent.id,
            grandparentId: grandparent.id,
            isLF: parent.isLF,
        });
        const collections = await (0, collections_1.findBySlug)(qx, data.grandparentSlug);
        const [existingProject] = await collectionService.findInsightsProjectsBySlug(subproject.slug);
        const normalizedSlug = subproject.slug.replace(/^nonlf_/, '');
        const projectData = {
            segmentId: subproject.id,
            name: subproject.name,
            slug: normalizedSlug,
            ...(parent.isLF && { collections: collections.map((c) => c.id), starred: false }),
        };
        const mustUpdateProject = existingProject && !existingProject.segmentId;
        if (mustUpdateProject) {
            await collectionService.updateInsightsProject(existingProject.id, projectData);
        }
        else {
            await collectionService.createInsightsProject(projectData);
        }
        return subproject;
    }
    async findById(id) {
        return new segmentRepository_1.default(this.options).findById(id);
    }
    async findByIds(ids) {
        return new segmentRepository_1.default(this.options).findByIds(ids);
    }
    async queryProjectGroups(search) {
        const result = await new segmentRepository_1.default(this.options).queryProjectGroups(search);
        // if (result.rows.length) {
        //   const membersCountPerSegment = await MemberRepository.countMembersPerSegment(
        //     this.options,
        //     result.rows.map((s) => s.id),
        //   )
        //   this.setMembersCount(result.rows, SegmentLevel.PROJECT_GROUP, membersCountPerSegment)
        // }
        return result;
    }
    async queryProjects(search) {
        const result = await new segmentRepository_1.default(this.options).queryProjects(search);
        return result;
    }
    async querySubprojects(search) {
        const result = await new segmentRepository_1.default(this.options).querySubprojects(search);
        return result;
    }
    async querySubprojectsLite(search) {
        const result = await new segmentRepository_1.default(this.options).querySubprojectsLite(search);
        return result;
    }
    async createActivityType(data, platform = types_1.PlatformType.OTHER) {
        if (!data.type) {
            throw new common_1.Error400(this.options.language, 'settings.activityTypes.errors.typeRequiredWhenCreating');
        }
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(this.options);
        const typeKey = data.type.toLowerCase();
        const platformKey = platform.toLowerCase();
        const activityTypes = segmentRepository_1.default.getActivityTypes(this.options);
        if (!activityTypes.custom[platformKey]) {
            activityTypes.custom[platformKey] = {};
        }
        // check key already exists
        if (activityTypes.custom && activityTypes.custom[platformKey][typeKey]) {
            return activityTypes;
        }
        activityTypes.custom[platformKey][typeKey] = {
            display: {
                default: data.type,
                short: data.type,
                channel: '',
            },
            calculateSentiment: false,
        };
        const updated = await new segmentRepository_1.default(this.options).update(segment.id, {
            customActivityTypes: activityTypes.custom,
        });
        return updated.activityTypes;
    }
    /**
     * unnest activity types with platform for easy access/manipulation
     * custom : {
     *    platform: {
     *         type1: settings1,
     *         type2: settings2
     *    }
     * }
     *
     * is transformed into
     * {
     *    type1: {...settings1, platform},
     *    type2: {...settings2, platform}
     * }
     *
     */
    static unnestActivityTypes(activityTypes) {
        return Object.keys(activityTypes.custom)
            .filter((k) => activityTypes.custom[k])
            .reduce((acc, platform) => {
            const unnestWithPlatform = Object.keys(activityTypes.custom[platform]).reduce((acc2, key) => {
                acc2[key] = { ...activityTypes.custom[platform][key], platform };
                return acc2;
            }, {});
            acc = { ...acc, ...unnestWithPlatform };
            return acc;
        }, {});
    }
    async updateActivityType(key, data) {
        if (!data.type) {
            throw new common_1.Error400(this.options.language, 'settings.activityTypes.errors.typeRequiredWhenUpdating');
        }
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(this.options);
        const activityTypes = segmentRepository_1.default.getActivityTypes(this.options);
        const activityTypesUnnested = SegmentService.unnestActivityTypes(activityTypes);
        // if key doesn't exist, throw 400
        if (!activityTypesUnnested[key]) {
            throw new common_1.Error400(this.options.language, 'settings.activityTypes.errors.notFound', key);
        }
        activityTypes.custom[activityTypesUnnested[key].platform][key] = {
            display: {
                default: data.type,
                short: data.type,
                channel: '',
            },
            calculateSentiment: false,
        };
        const updated = await new segmentRepository_1.default(this.options).update(segment.id, {
            customActivityTypes: activityTypes.custom,
        });
        return updated.activityTypes;
    }
    async destroyActivityType(key) {
        const activityTypes = segmentRepository_1.default.getActivityTypes(this.options);
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(this.options);
        const activityTypesUnnested = SegmentService.unnestActivityTypes(activityTypes);
        if (activityTypesUnnested[key]) {
            delete activityTypes.custom[activityTypesUnnested[key].platform][key];
            const updated = await new segmentRepository_1.default(this.options).update(segment.id, {
                customActivityTypes: activityTypes.custom,
            });
            return updated.activityTypes;
        }
        return activityTypes;
    }
    static listActivityTypes(options) {
        return segmentRepository_1.default.getActivityTypes(options);
    }
    /**
     * update activity channels after checking for duplicates with platform key
     */
    async updateActivityChannels(data) {
        if (!data.channel) {
            throw new common_1.Error400(this.options.language, 'settings.activityChannels.errors.typeRequiredWhenCreating');
        }
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(this.options);
        const segmentRepository = new segmentRepository_1.default(this.options);
        await segmentRepository.addActivityChannel(segment.id, data.platform, data.channel);
    }
    async getSegmentSubprojects(segments) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, segments_1.getSegmentSubprojects)(qx, segments);
    }
    static getTenantActivityTypes(subprojects) {
        if (!(subprojects === null || subprojects === void 0 ? void 0 : subprojects.length)) {
            return { custom: {}, default: {} };
        }
        return subprojects.reduce((acc, subproject) => {
            const activityTypes = (0, segments_1.buildSegmentActivityTypes)(subproject);
            return {
                custom: {
                    ...acc.custom,
                    ...activityTypes.custom,
                },
                default: {
                    ...acc.default,
                    ...activityTypes.default,
                },
            };
        }, { custom: {}, default: {} });
    }
    static async getTenantActivityChannels(segments, options) {
        const segmentRepository = new segmentRepository_1.default(options);
        const activityChannels = await segmentRepository.fetchTenantActivityChannels(segments);
        return activityChannels;
    }
    collectSubprojectIds(segments, level) {
        if (level === types_1.SegmentLevel.PROJECT_GROUP) {
            return segments.map((s) => this.collectSubprojectIds(s.projects, types_1.SegmentLevel.PROJECT)).flat();
        }
        if (level === types_1.SegmentLevel.PROJECT) {
            return segments
                .map((s) => this.collectSubprojectIds(s.subprojects, types_1.SegmentLevel.SUB_PROJECT))
                .flat();
        }
        if (level === types_1.SegmentLevel.SUB_PROJECT) {
            return segments.map((s) => s.id);
        }
        throw new Error(`Unknown segment level: ${level}`);
    }
    setMembersCount(segments, level, membersCountPerSegment) {
        if (level === types_1.SegmentLevel.PROJECT_GROUP) {
            let total = 0;
            for (const projectGroup of segments) {
                projectGroup.members = this.setMembersCount(projectGroup.projects, types_1.SegmentLevel.PROJECT, membersCountPerSegment);
                total += projectGroup.members;
            }
            return total;
        }
        if (level === types_1.SegmentLevel.PROJECT) {
            let total = 0;
            for (const project of segments) {
                project.members = this.setMembersCount(project.subprojects, types_1.SegmentLevel.SUB_PROJECT, membersCountPerSegment);
                total += project.members;
            }
            return total;
        }
        if (level === types_1.SegmentLevel.SUB_PROJECT) {
            let total = 0;
            for (const subproject of segments) {
                subproject.members = membersCountPerSegment[subproject.id] || 0;
                total += subproject.members;
            }
            return total;
        }
        throw new Error(`Unknown segment level: ${level}`);
    }
    async addMemberCounts(segments, level) {
        const subprojectIds = this.collectSubprojectIds(segments, level);
        if (!subprojectIds.length) {
            return;
        }
        const membersCountPerSegment = await memberRepository_1.default.countMembersPerSegment(this.options, subprojectIds);
        this.setMembersCount(segments, level, membersCountPerSegment);
    }
    /**
     * Validates that a segment name and/or slug don't conflict with existing segments.
     * This is a centralized validation function used for both creation and updates.
     *
     * @param segmentRepository - Repository instance for database operations
     * @param name - Segment name to check for conflicts (optional)
     * @param slug - Segment slug to check for conflicts (optional)
     * @param segmentType - Type of segment being validated (PROJECT_GROUP, PROJECT, SUB_PROJECT)
     * @param isLF - Whether this is a Linux Foundation segment (affects slug formatting)
     * @param excludeId - Segment ID to exclude from conflict checking (used for updates)
     *
     * @throws Error400 with appropriate error message if conflicts are found
     */
    async validateSegmentConflicts(segmentRepository, name, slug, segmentType, isLF, excludeId) {
        // Validate slug conflicts if slug is provided
        if (slug) {
            // For projects and sub-projects, we need to check both LF and non-LF formats
            // to prevent conflicts across both formats
            if (segmentType === types_1.SegmentLevel.PROJECT || segmentType === types_1.SegmentLevel.SUB_PROJECT) {
                const baseSlug = slug.startsWith('nonlf_') ? slug.substring(6) : slug;
                const nonLfSlug = `nonlf_${baseSlug}`;
                // Check for conflicts with LF format (no prefix)
                const existingLfBySlug = await segmentRepository.findBySlug(baseSlug, segmentType);
                if (existingLfBySlug && (!excludeId || existingLfBySlug.id !== excludeId)) {
                    await this.throwSegmentConflictError(segmentRepository, existingLfBySlug, 'slug', slug);
                }
                // Check for conflicts with non-LF format (nonlf_ prefix)
                const existingNonLfBySlug = await segmentRepository.findBySlug(nonLfSlug, segmentType);
                if (existingNonLfBySlug && (!excludeId || existingNonLfBySlug.id !== excludeId)) {
                    await this.throwSegmentConflictError(segmentRepository, existingNonLfBySlug, 'slug', slug);
                }
            }
            else {
                // For project groups, just check the exact slug
                const existingBySlug = await segmentRepository.findBySlug(slug, segmentType);
                if (existingBySlug && (!excludeId || existingBySlug.id !== excludeId)) {
                    await this.throwSegmentConflictError(segmentRepository, existingBySlug, 'slug', slug);
                }
            }
        }
        // Validate name conflicts if name is provided
        if (name) {
            const existingByName = await segmentRepository.findByName(name, segmentType);
            // If we found a conflicting segment and it's not the one we're updating
            if (existingByName && (!excludeId || existingByName.id !== excludeId)) {
                await this.throwSegmentConflictError(segmentRepository, existingByName, 'name', name);
            }
        }
    }
    async blockOrganizationAffiliationIfSegmentNameMatches(segmentName, transaction) {
        const qx = sequelizeRepository_1.default.getQueryExecutor({
            ...this.options,
            transaction,
        });
        // Check if there is an existing organization with segment name
        const organizations = await (0, data_access_layer_1.findOrganizationsByName)(qx, segmentName);
        if (organizations.length === 0) {
            return [];
        }
        const result = [];
        for (const o of organizations) {
            if (!o.isAffiliationBlocked) {
                const updatedOrgId = await (0, data_access_layer_1.updateOrganization)(qx, o.id, { isAffiliationBlocked: true });
                if (updatedOrgId) {
                    await (0, member_organization_affiliation_1.applyOrganizationAffiliationPolicyToMembers)(qx, updatedOrgId, false);
                    await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, { organizationId: updatedOrgId });
                    result.push(updatedOrgId);
                }
            }
        }
        return result;
    }
    /**
     * Throws an appropriate error message when a segment conflict is detected.
     * This method dynamically generates error messages based on the existing conflicting segment,
     * including the correct parent name from the database (not from the input data).
     *
     * @param segmentRepository - Repository instance for database operations
     * @param existingSegment - The segment that already exists and conflicts
     * @param conflictType - Whether the conflict is on 'name' or 'slug'
     * @param conflictValue - The conflicting name or slug value
     *
     * @throws Error400 with localized error message and appropriate parameters
     */
    async throwSegmentConflictError(segmentRepository, existingSegment, conflictType, conflictValue) {
        const existingSegmentType = SegmentService.getSegmentType(existingSegment);
        let errorKey;
        let parentName;
        switch (existingSegmentType) {
            case types_1.SegmentLevel.PROJECT_GROUP: {
                // Project groups don't have parents, so no parent name needed
                errorKey =
                    conflictType === 'slug'
                        ? 'settings.segments.errors.projectGroupSlugExists'
                        : 'settings.segments.errors.projectGroupNameExists';
                break;
            }
            case types_1.SegmentLevel.PROJECT: {
                errorKey =
                    conflictType === 'slug'
                        ? 'settings.segments.errors.projectSlugExists'
                        : 'settings.segments.errors.projectNameExists';
                // Fetch the actual parent (project group) name from the database
                // This fixes the bug where we were using the wrong parent name
                const projectParent = await segmentRepository.findById(existingSegment.parentId);
                parentName = projectParent === null || projectParent === void 0 ? void 0 : projectParent.name;
                break;
            }
            case types_1.SegmentLevel.SUB_PROJECT: {
                errorKey =
                    conflictType === 'slug'
                        ? 'settings.segments.errors.subprojectSlugExists'
                        : 'settings.segments.errors.subprojectNameExists';
                // Fetch the actual parent (project) name from the database
                // This fixes the bug where we were using the wrong parent name
                const subprojectParent = await segmentRepository.findById(existingSegment.parentId);
                parentName = subprojectParent === null || subprojectParent === void 0 ? void 0 : subprojectParent.name;
                break;
            }
            default:
                throw new Error(`Unknown segment type: ${existingSegmentType}`);
        }
        // Throw error with appropriate parameters based on segment type
        if (parentName) {
            throw new common_1.Error400(this.options.language, errorKey, conflictValue, parentName);
        }
        else {
            throw new common_1.Error400(this.options.language, errorKey, conflictValue);
        }
    }
    /**
     * Validates that segment updates don't create conflicts with existing segments.
     * Only validates fields that are actually being changed to avoid unnecessary checks.
     *
     * @param segmentId - ID of the segment being updated (excluded from conflict checks)
     * @param segment - The current segment data before update
     * @param data - The update data containing potentially changed fields
     * @param segmentRepository - Repository instance for database operations
     *
     * @throws Error400 if the update would create conflicts with existing segments
     */
    async validateUpdateDuplicates(segmentId, segment, data, segmentRepository) {
        const segmentType = SegmentService.getSegmentType(segment);
        // Only validate fields that are actually being changed
        await this.validateSegmentConflicts(segmentRepository, data.name !== segment.name ? data.name : undefined, data.slug !== segment.slug ? data.slug : undefined, segmentType, data.isLF !== undefined ? data.isLF : segment.isLF, segmentId);
    }
    static getSegmentType(segment) {
        // Fallback to parent/grandparent logic if type not available
        if (!segment.parentSlug && !segment.grandparentSlug) {
            return types_1.SegmentLevel.PROJECT_GROUP;
        }
        if (segment.parentSlug && !segment.grandparentSlug) {
            return types_1.SegmentLevel.PROJECT;
        }
        if (segment.parentSlug && segment.grandparentSlug) {
            return types_1.SegmentLevel.SUB_PROJECT;
        }
        throw new Error('Unable to determine segment type');
    }
    static async refreshSegments(options) {
        const repo = new segmentRepository_1.default(options);
        for (let i = 0; i < options.currentSegments.length; i++) {
            options.currentSegments[i] = await repo.findById(options.currentSegments[i].id);
        }
    }
}
exports.default = SegmentService;
//# sourceMappingURL=segmentService.js.map