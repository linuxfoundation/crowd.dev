"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = __importDefault(require("crypto"));
const lodash_1 = __importDefault(require("lodash"));
const roles_1 = __importDefault(require("../../security/roles"));
const segmentRepository_1 = __importDefault(require("./segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class TenantUserRepository {
    static async findByTenant(tenantId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const records = await options.database.tenantUser.findAll({
            where: {
                tenantId,
            },
            transaction,
        });
        return records;
    }
    static async findByTenantAndUser(tenantId, userId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.tenantUser.findOne({
            where: {
                tenantId,
                userId,
            },
            transaction,
        });
        return record;
    }
    static async findByInvitationToken(invitationToken, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.tenantUser.findOne({
            where: {
                invitationToken,
            },
            include: ['tenant', 'user'],
            transaction,
        });
        return record;
    }
    static async convertRoles(roles, options) {
        const segmentRepository = new segmentRepository_1.default(options);
        const adminSegments = [];
        roles = lodash_1.default.uniq(roles.map((role) => {
            if (role.startsWith(`${roles_1.default.values.admin}:`)) {
                adminSegments.push(role.split(':')[1].trim());
                return roles_1.default.values.projectAdmin;
            }
            return role;
        }));
        const adminSegmentIds = await segmentRepository.findBySourceIds(adminSegments);
        return {
            roles,
            adminSegments: adminSegmentIds,
        };
    }
    static async create(tenant, user, rawRoles, options) {
        rawRoles = rawRoles || [];
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const { roles, adminSegments } = await this.convertRoles(rawRoles, {
            currentTenant: tenant,
            ...options,
        });
        const status = selectStatus('active', roles);
        await options.database.tenantUser.create({
            tenantId: tenant.id,
            userId: user.id,
            status,
            roles,
            adminSegments,
        }, { transaction });
    }
    static async destroy(tenantId, id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const tenantUser = await this.findByTenantAndUser(tenantId, id, options);
        await tenantUser.destroy({ transaction });
    }
    static async updateRoles(tenantId, id, roles, options, isInvited = false) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        let tenantUser = await this.findByTenantAndUser(tenantId, id, options);
        if (!tenantUser) {
            tenantUser = await options.database.tenantUser.create({
                tenantId,
                userId: id,
                status: selectStatus('invited', []),
                invitationToken: crypto_1.default.randomBytes(20).toString('hex'),
                roles: [],
                invitedById: isInvited ? options.currentUser.id : undefined,
            }, { transaction });
        }
        const { roles: existingRoles } = tenantUser;
        let newRoles = [];
        if (options.addRoles) {
            newRoles = [...new Set([...existingRoles, ...roles])];
        }
        else if (options.removeOnlyInformedRoles) {
            newRoles = existingRoles.filter((existingRole) => !roles.includes(existingRole));
        }
        else {
            newRoles = roles || [];
        }
        tenantUser.roles = newRoles;
        tenantUser.status = selectStatus(tenantUser.status, newRoles);
        await tenantUser.save({
            transaction,
        });
        return tenantUser;
    }
    static async updateSettings(userId, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const tenantUser = await this.findByTenantAndUser(options.currentTenant.id, userId, options);
        await tenantUser.update({
            settings: { ...tenantUser.settings, ...data },
            updatedById: currentUser.id,
        }, { transaction });
        return tenantUser;
    }
    static async updateEagleEyeSettings(userId, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const tenantUser = await this.findByTenantAndUser(options.currentTenant.id, userId, options);
        await tenantUser.update({
            settings: {
                ...tenantUser.settings,
                eagleEye: { ...tenantUser.settings.eagleEye, ...data },
            },
            updatedById: currentUser.id,
        }, { transaction });
        return tenantUser;
    }
    static async acceptInvitation(invitationToken, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const invitationTenantUser = await this.findByInvitationToken(invitationToken, options);
        const isSameEmailFromInvitation = invitationTenantUser.userId === currentUser.id;
        const existingTenantUser = await this.findByTenantAndUser(invitationTenantUser.tenantId, currentUser.id, options);
        // There might be a case that the invite was sent to another email,
        // and the current user is also invited or is already a member
        if (existingTenantUser && existingTenantUser.id !== invitationTenantUser.id) {
            // destroys the new invite
            await this.destroy(invitationTenantUser.tenantId, invitationTenantUser.userId, options);
            // Merges the roles from the invitation and the current tenant user
            existingTenantUser.roles = [
                ...new Set([...existingTenantUser.roles, ...invitationTenantUser.roles]),
            ];
            // Change the status to active (in case the existing one is also invited)
            existingTenantUser.invitationToken = null;
            existingTenantUser.status = selectStatus('active', existingTenantUser.roles);
            await existingTenantUser.save({
                transaction,
            });
        }
        else {
            // In this case there's no tenant user for the current user and the tenant
            // Sometimes the invitation is sent not to the
            // correct email. In those cases the userId must be changed
            // to match the correct user.
            invitationTenantUser.userId = currentUser.id;
            invitationTenantUser.invitationToken = null;
            invitationTenantUser.status = selectStatus('active', invitationTenantUser.roles);
            await invitationTenantUser.save({
                transaction,
            });
        }
        const emailVerified = currentUser.emailVerified || isSameEmailFromInvitation;
        await options.database.user.update({
            emailVerified,
        }, { where: { id: currentUser.id }, transaction });
    }
    static async replaceRoles(tenantUserId, rawRoles, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const { roles, adminSegments } = await TenantUserRepository.convertRoles(rawRoles, options);
        await options.database.tenantUser.update({ roles, adminSegments, status: 'active', invitationToken: null }, {
            where: {
                id: tenantUserId,
            },
            transaction,
        });
    }
}
exports.default = TenantUserRepository;
function selectStatus(oldStatus, newRoles) {
    newRoles = newRoles || [];
    if (oldStatus === 'invited') {
        return oldStatus;
    }
    if (!newRoles.length) {
        return 'empty-permissions';
    }
    return 'active';
}
//# sourceMappingURL=tenantUserRepository.js.map