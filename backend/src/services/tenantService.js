"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const integrations_1 = require("@crowd/integrations");
const types_1 = require("@crowd/types");
const customViewRepository_1 = __importDefault(require("@/database/repositories/customViewRepository"));
const customView_1 = require("@/types/customView");
const configTypes_1 = require("../conf/configTypes");
const index_1 = require("../conf/index");
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const tenantRepository_1 = __importDefault(require("../database/repositories/tenantRepository"));
const tenantUserRepository_1 = __importDefault(require("../database/repositories/tenantUserRepository"));
const permissions_1 = __importDefault(require("../security/permissions"));
const roles_1 = __importDefault(require("../security/roles"));
const memberAttributeSettingsService_1 = __importDefault(require("./memberAttributeSettingsService"));
const segmentService_1 = __importDefault(require("./segmentService"));
const settingsService_1 = __importDefault(require("./settingsService"));
const permissionChecker_1 = __importDefault(require("./user/permissionChecker"));
class TenantService {
    constructor(options) {
        this.options = options;
    }
    /**
     * Creates the default tenant or joins the default with
     * roles passed.
     * If default roles are empty, the admin will have to asign the roles
     * to new users.
     */
    async createOrJoinDefault({ roles }, transaction) {
        const tenant = await tenantRepository_1.default.findDefault({
            ...this.options,
            transaction,
        });
        if (tenant) {
            const tenantUser = await tenantUserRepository_1.default.findByTenantAndUser(tenant.id, this.options.currentUser.id, {
                ...this.options,
                transaction,
            });
            // In this situation, the user has used the invitation token
            // and it is already part of the tenant
            if (tenantUser) {
                return undefined;
            }
            return tenantUserRepository_1.default.create(tenant, this.options.currentUser, roles, {
                ...this.options,
                transaction,
            });
        }
        const record = await this.create({
            id: common_1.DEFAULT_TENANT_ID,
            name: 'default',
            url: 'default',
            integrationsRequired: [],
        });
        await settingsService_1.default.findOrCreateDefault({
            ...this.options,
            currentTenant: record,
            transaction,
        });
        const tenantUserRepoRecord = await tenantUserRepository_1.default.create(record, this.options.currentUser, [roles_1.default.values.admin], {
            ...this.options,
            transaction,
        });
        return tenantUserRepoRecord;
    }
    async joinWithDefaultRolesOrAskApproval({ roles, tenantId }, { transaction }) {
        const tenant = await tenantRepository_1.default.findById(tenantId, {
            ...this.options,
            transaction,
        });
        if (!tenant) {
            this.options.log.error(`Tenant not found: ${tenantId}`);
        }
        const tenantUser = await tenantUserRepository_1.default.findByTenantAndUser(tenant.id, this.options.currentUser.id, {
            ...this.options,
            transaction,
        });
        if (tenantUser) {
            // If found the invited tenant user via email
            // accepts the invitation
            if (tenantUser.status === 'invited') {
                return tenantUserRepository_1.default.acceptInvitation(tenantUser.invitationToken, {
                    ...this.options,
                    transaction,
                });
            }
            // In this case the tenant user already exists
            // and it's accepted or with empty permissions
            return undefined;
        }
        return tenantUserRepository_1.default.create(tenant, this.options.currentUser, roles, {
            ...this.options,
            transaction,
        });
    }
    // In case this user has been invited
    // but havent used the invitation token
    async joinDefaultUsingInvitedEmail(transaction) {
        const tenant = await tenantRepository_1.default.findDefault({
            ...this.options,
            transaction,
        });
        if (!tenant) {
            return undefined;
        }
        const tenantUser = await tenantUserRepository_1.default.findByTenantAndUser(tenant.id, this.options.currentUser.id, {
            ...this.options,
            transaction,
        });
        if (!tenantUser || tenantUser.status !== 'invited') {
            return undefined;
        }
        return tenantUserRepository_1.default.acceptInvitation(tenantUser.invitationToken, {
            ...this.options,
            transaction,
        });
    }
    async create(data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            if (index_1.TENANT_MODE === configTypes_1.TenantMode.SINGLE) {
                const count = await tenantRepository_1.default.count(null, {
                    ...this.options,
                    transaction,
                });
                if (count > 0) {
                    throw new common_1.Error400(this.options.language, 'tenant.exists');
                }
            }
            if (data.integrationsRequired) {
                // Convert all to lowercase
                data.integrationsRequired = data.integrationsRequired.map((item) => item.toLowerCase());
            }
            const record = await tenantRepository_1.default.create(data, {
                ...this.options,
                transaction,
            });
            const segment = await new segmentService_1.default({
                ...this.options,
                currentTenant: record,
                transaction,
            }).createProjectGroup({
                name: data.name,
                url: data.url,
                slug: data.url || (await tenantRepository_1.default.generateTenantUrl(data.name, this.options)),
                status: types_1.SegmentStatus.ACTIVE,
            });
            this.options.currentSegments = [segment.projects[0].subprojects[0]];
            await settingsService_1.default.findOrCreateDefault({
                ...this.options,
                currentTenant: record,
                transaction,
            });
            const memberAttributeSettingsService = new memberAttributeSettingsService_1.default({
                ...this.options,
                currentTenant: record,
            });
            // create default member attribute settings
            await memberAttributeSettingsService.createPredefined(integrations_1.DEFAULT_MEMBER_ATTRIBUTES, transaction);
            await tenantUserRepository_1.default.create(record, this.options.currentUser, [roles_1.default.values.admin], {
                ...this.options,
                transaction,
            });
            // create default custom views
            for (const entity of Object.values(customView_1.defaultCustomViews)) {
                for (const customView of entity) {
                    await customViewRepository_1.default.create(customView, {
                        ...this.options,
                        transaction,
                        currentTenant: record,
                    });
                }
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async update(id, data, force = false) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            let record = await tenantRepository_1.default.findById(id, {
                ...this.options,
                transaction,
                currentTenant: { id },
            });
            if (!force) {
                new permissionChecker_1.default({
                    ...this.options,
                    currentTenant: { id },
                }).validateHas(permissions_1.default.values.tenantEdit);
            }
            record = await tenantRepository_1.default.update(id, data, {
                ...this.options,
                transaction,
                currentTenant: record,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async viewOrganizations() {
        return settingsService_1.default.save({ organizationsViewed: true }, this.options);
    }
    async viewContacts() {
        return settingsService_1.default.save({ contactsViewed: true }, this.options);
    }
    async destroyAll(ids) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            for (const id of ids) {
                const tenant = await tenantRepository_1.default.findById(id, {
                    ...this.options,
                    transaction,
                    currentTenant: { id },
                });
                new permissionChecker_1.default({
                    ...this.options,
                    currentTenant: tenant,
                }).validateHas(permissions_1.default.values.tenantDestroy);
                await tenantRepository_1.default.destroy(id, {
                    ...this.options,
                    transaction,
                    currentTenant: { id },
                });
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async findById(id, options) {
        options = options || {};
        return tenantRepository_1.default.findById(id, {
            ...this.options,
            ...options,
        });
    }
    async findByUrl(url, options) {
        options = options || {};
        return tenantRepository_1.default.findByUrl(url, {
            ...this.options,
            ...options,
        });
    }
    async findAllAutocomplete(search, limit) {
        return tenantRepository_1.default.findAllAutocomplete(search, limit, this.options);
    }
    async findAndCountAll(args) {
        return tenantRepository_1.default.findAndCountAll(args, this.options);
    }
    /**
     * Find all tenants bypassing default user filter
     * @param args filter argument
     * @returns count and rows of found tenants
     */
    static async _findAndCountAllForEveryUser(args) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const filterUsers = false;
        return tenantRepository_1.default.findAndCountAll(args, options, filterUsers);
    }
    async acceptInvitation(token, forceAcceptOtherEmail = false) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const tenantUser = await tenantUserRepository_1.default.findByInvitationToken(token, {
                ...this.options,
                transaction,
            });
            if (!tenantUser || tenantUser.status !== 'invited') {
                throw new common_1.Error404();
            }
            const isNotCurrentUserEmail = tenantUser.user.id !== this.options.currentUser.id;
            if (!forceAcceptOtherEmail && isNotCurrentUserEmail) {
                throw new common_1.Error400(this.options.language, 'tenant.invitation.notSameEmail', tenantUser.user.email, this.options.currentUser.email);
            }
            await tenantUserRepository_1.default.acceptInvitation(token, {
                ...this.options,
                currentTenant: { id: tenantUser.tenant.id },
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return tenantUser.tenant;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async declineInvitation(token) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const tenantUser = await tenantUserRepository_1.default.findByInvitationToken(token, {
                ...this.options,
                transaction,
            });
            if (!tenantUser || tenantUser.status !== 'invited') {
                throw new common_1.Error404();
            }
            await tenantUserRepository_1.default.destroy(tenantUser.tenant.id, this.options.currentUser.id, {
                ...this.options,
                transaction,
                currentTenant: { id: tenantUser.tenant.id },
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async import(data, importHash) {
        if (!importHash) {
            throw new common_1.Error400(this.options.language, 'importer.errors.importHashRequired');
        }
        if (await this._isImportHashExistent(importHash)) {
            throw new common_1.Error400(this.options.language, 'importer.errors.importHashExistent');
        }
        const dataToCreate = {
            ...data,
            importHash,
        };
        return this.create(dataToCreate);
    }
    async _isImportHashExistent(importHash) {
        const count = await tenantRepository_1.default.count({
            importHash,
        }, this.options);
        return count > 0;
    }
}
exports.default = TenantService;
//# sourceMappingURL=tenantService.js.map