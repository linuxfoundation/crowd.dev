"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = __importDefault(require("crypto"));
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = __importDefault(require("sequelize"));
const common_1 = require("@crowd/common");
const sequelizeArrayUtils_1 = __importDefault(require("../utils/sequelizeArrayUtils"));
const sequelizeFilterUtils_1 = __importDefault(require("../utils/sequelizeFilterUtils"));
const userTenantUtils_1 = require("../utils/userTenantUtils");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const { Op } = sequelize_1.default;
class UserRepository {
    /**
     * Finds the user that owns the given tenant
     * @param tenantId
     * @returns User object with tenants populated
     */
    static async findUserOfTenant(tenantId) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const record = await options.database.user.findOne({
            tenants: tenantId,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record, options);
    }
    /**
     * Finds all users of a tenant.
     * @param tenantId
     * @returns
     */
    static async findAllUsersOfTenant(tenantId) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const records = await options.database.user.findAll({
            include: [
                {
                    model: options.database.tenantUser,
                    as: 'tenants',
                    where: { tenantId },
                },
            ],
        });
        if (records.length === 0) {
            throw new common_1.Error404();
        }
        return this._populateRelationsForRows(records, options);
    }
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.create({
            id: data.id || undefined,
            email: data.email,
            firstName: data.firstName || null,
            lastName: data.lastName || null,
            phoneNumber: data.phoneNumber || null,
            importHash: data.importHash || null,
            createdById: currentUser.id,
            updatedById: currentUser.id,
        }, { transaction });
        return this.findById(user.id, {
            ...options,
            bypassPermissionValidation: true,
        });
    }
    static async createFromAuth(data, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.create({
            email: data.email,
            firstName: data.firstName,
            lastName: data.lastName,
            fullName: data.fullName,
            password: data.password,
            acceptedTermsAndPrivacy: data.acceptedTermsAndPrivacy,
        }, { transaction });
        delete user.password;
        return this.findById(user.id, {
            ...options,
            bypassPermissionValidation: true,
        });
    }
    static async updateProfile(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findByPk(id, {
            transaction,
        });
        await user.update({
            firstName: data.firstName || null,
            lastName: data.lastName || null,
            phoneNumber: data.phoneNumber || null,
            acceptedTermsAndPrivacy: data.acceptedTermsAndPrivacy || false,
            updatedById: currentUser.id,
        }, { transaction });
        return this.findById(user.id, options);
    }
    static async updatePassword(id, password, invalidateOldTokens, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findByPk(id, {
            transaction,
        });
        const data = {
            password,
            updatedById: currentUser.id,
        };
        if (invalidateOldTokens) {
            data.jwtTokenInvalidBefore = new Date();
        }
        await user.update(data, { transaction });
        return this.findById(user.id, {
            ...options,
            bypassPermissionValidation: true,
        });
    }
    static async generateEmailVerificationToken(email, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findOne({
            where: { email },
            transaction,
        });
        const emailVerificationToken = crypto_1.default.randomBytes(20).toString('hex');
        const emailVerificationTokenExpiresAt = Date.now() + 24 * 60 * 60 * 1000;
        await user.update({
            emailVerificationToken,
            emailVerificationTokenExpiresAt,
            updatedById: currentUser.id,
        }, { transaction });
        return emailVerificationToken;
    }
    static async generatePasswordResetToken(email, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findOne({
            where: { email },
            transaction,
        });
        const passwordResetToken = crypto_1.default.randomBytes(20).toString('hex');
        const passwordResetTokenExpiresAt = Date.now() + 24 * 60 * 60 * 1000;
        await user.update({
            passwordResetToken,
            passwordResetTokenExpiresAt,
            updatedById: currentUser.id,
        }, { transaction });
        return passwordResetToken;
    }
    static async update(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findByPk(id, {
            transaction,
        });
        await user.update({
            firstName: data.firstName || null,
            lastName: data.lastName || null,
            phoneNumber: data.phoneNumber || null,
            provider: data.provider || null,
            providerId: data.providerId || null,
            emailVerified: data.emailVerified || null,
            updatedById: currentUser.id,
        }, { transaction });
        return this.findById(user.id, options);
    }
    static async findByEmail(email, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findOne({
            where: {
                [Op.and]: sequelizeFilterUtils_1.default.ilikeExact('user', 'email', email),
            },
            transaction,
        });
        return this._populateRelations(record, options);
    }
    static async findByProviderId(providerId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findOne({
            where: {
                [Op.and]: sequelizeFilterUtils_1.default.ilikeExact('user', 'providerId', providerId),
            },
            transaction,
        });
        return this._populateRelations(record, options);
    }
    static async findByEmailWithoutAvatar(email, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findOne({
            where: {
                [Op.and]: sequelizeFilterUtils_1.default.ilikeExact('user', 'email', email),
            },
            transaction,
        });
        return this._populateRelations(record, options);
    }
    static async findAndCountAll({ filter, limit = 0, offset = 0, orderBy = '' }, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const whereAnd = [];
        const include = [];
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        if (!filter || (!filter.role && !filter.status)) {
            include.push({
                model: options.database.tenantUser,
                as: 'tenants',
                where: {
                    tenantId: currentTenant.id,
                    status: 'active',
                },
            });
        }
        // Exclude help@crowd.dev
        whereAnd.push({
            email: {
                [Op.ne]: 'help@crowd.dev',
            },
        });
        if (filter) {
            if (filter.id) {
                whereAnd.push({
                    id: filter.id,
                });
            }
            if (filter.fullName) {
                whereAnd.push(sequelizeFilterUtils_1.default.ilikeIncludes('user', 'fullName', filter.fullName));
            }
            if (filter.email) {
                whereAnd.push(sequelizeFilterUtils_1.default.ilikeIncludes('user', 'email', filter.email));
            }
            if (filter.query) {
                whereAnd.push({
                    [Op.or]: [
                        sequelizeFilterUtils_1.default.ilikeIncludes('user', 'fullName', filter.query),
                        sequelizeFilterUtils_1.default.ilikeIncludes('user', 'email', filter.query),
                    ],
                });
            }
            if (filter.role) {
                const innerWhereAnd = [];
                innerWhereAnd.push({
                    tenantId: currentTenant.id,
                });
                innerWhereAnd.push(sequelizeArrayUtils_1.default.filter(`tenants`, `roles`, filter.role));
                include.push({
                    model: options.database.tenantUser,
                    as: 'tenants',
                    where: { [Op.and]: innerWhereAnd },
                });
            }
            if (filter.status) {
                include.push({
                    model: options.database.tenantUser,
                    as: 'tenants',
                    where: {
                        tenantId: currentTenant.id,
                        status: filter.status,
                    },
                });
            }
            if (filter.createdAtRange) {
                const [start, end] = filter.createdAtRange;
                if (start !== undefined && start !== null && start !== '') {
                    whereAnd.push({
                        createdAt: {
                            [Op.gte]: start,
                        },
                    });
                }
                if (end !== undefined && end !== null && end !== '') {
                    whereAnd.push({
                        createdAt: {
                            [Op.lte]: end,
                        },
                    });
                }
            }
        }
        const where = { [Op.and]: whereAnd };
        let { rows, count, // eslint-disable-line prefer-const
         } = await options.database.user.findAndCountAll({
            where,
            include,
            limit: limit ? Number(limit) : undefined,
            offset: offset ? Number(offset) : undefined,
            order: orderBy ? [orderBy.split('_')] : [['email', 'ASC']],
            transaction,
        });
        rows = await this._populateRelationsForRows(rows, options);
        rows = await this._mapUserForTenantForRows(rows, currentTenant, options);
        return { rows, count, limit: false, offset: 0 };
    }
    static async findAllAutocomplete(query, limit, options) {
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const whereAnd = [];
        const include = [
            {
                model: options.database.tenantUser,
                as: 'tenants',
                where: {
                    tenantId: currentTenant.id,
                },
            },
        ];
        whereAnd.push({
            email: {
                [Op.ne]: 'help@crowd.dev',
            },
        });
        if (query) {
            whereAnd.push({
                [Op.or]: [
                    {
                        id: sequelizeFilterUtils_1.default.uuid(query),
                    },
                    sequelizeFilterUtils_1.default.ilikeIncludes('user', 'fullName', query),
                    sequelizeFilterUtils_1.default.ilikeIncludes('user', 'email', query),
                ],
            });
        }
        const where = { [Op.and]: whereAnd };
        let users = await options.database.user.findAll({
            attributes: ['id', 'fullName', 'email'],
            where,
            include,
            limit: limit ? Number(limit) : undefined,
            order: [['fullName', 'ASC']],
        });
        users = await this._mapUserForTenantForRows(users, currentTenant, options);
        const buildText = (user) => {
            if (!user.fullName) {
                return user.email.split('@')[0];
            }
            return `${user.fullName}`;
        };
        return users.map((user) => ({
            id: user.id,
            label: buildText(user),
            email: user.email,
        }));
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const records = await options.database.sequelize.query(`
        SELECT
          "id",
          ROW_TO_JSON(users) AS json
        FROM users
        WHERE "deletedAt" IS NULL
          AND "id" = :id;
      `, {
            replacements: { id },
            transaction,
            model: options.database.user,
            mapToModel: true,
        });
        if (records.length !== 1) {
            throw new common_1.Error404();
        }
        let record = records[0];
        record = await this._populateRelations(record, options, {
            where: {
                status: 'active',
            },
        });
        record = {
            ...record,
            ...record.json,
        };
        delete record.json;
        // Remove sensitive fields
        delete record.password;
        delete record.emailVerificationToken;
        delete record.emailVerificationTokenExpiresAt;
        delete record.providerId;
        delete record.passwordResetToken;
        delete record.passwordResetTokenExpiresAt;
        delete record.jwtTokenInvalidBefore;
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        if (!options || !options.bypassPermissionValidation) {
            if (!(0, userTenantUtils_1.isUserInTenant)(record, currentTenant)) {
                throw new common_1.Error404();
            }
            record = await this._mapUserForTenant(record, currentTenant, options);
        }
        return record;
    }
    static async findByIdWithoutAvatar(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        let record = await options.database.user.findByPk(id, {
            transaction,
        });
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        record = await this._populateRelations(record, options);
        if (!options || !options.bypassPermissionValidation) {
            if (!(0, userTenantUtils_1.isUserInTenant)(record, currentTenant)) {
                throw new common_1.Error404();
            }
        }
        return record;
    }
    static async findByPasswordResetToken(token, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findOne({
            where: {
                passwordResetToken: token,
                // Find only not expired tokens
                passwordResetTokenExpiresAt: {
                    [options.database.Sequelize.Op.gt]: Date.now(),
                },
            },
            transaction,
        });
        return this._populateRelations(record, options);
    }
    static async findByEmailVerificationToken(token, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findOne({
            where: {
                emailVerificationToken: token,
                emailVerificationTokenExpiresAt: {
                    [options.database.Sequelize.Op.gt]: Date.now(),
                },
            },
            transaction,
        });
        return this._populateRelations(record, options);
    }
    static async markEmailVerified(id, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.findByPk(id, {
            transaction,
        });
        await user.update({
            emailVerified: true,
            updatedById: currentUser.id,
        }, { transaction });
        return true;
    }
    static async count(filter, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        return options.database.user.count({
            where: filter,
            transaction,
        });
    }
    static async findPassword(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.user.findByPk(id, {
            // raw is responsible
            // for bringing the password
            raw: true,
            transaction,
        });
        if (!record) {
            return null;
        }
        return record.password;
    }
    static async createFromSocial(provider, providerId, email, emailVerified, firstName, lastName, fullName, options) {
        const data = {
            email,
            emailVerified,
            providerId,
            provider,
            firstName,
            lastName,
            fullName,
            acceptedTermsAndPrivacy: false,
        };
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const user = await options.database.user.create(data, {
            transaction,
        });
        delete user.password;
        return this.findById(user.id, {
            ...options,
            bypassPermissionValidation: true,
        });
    }
    static cleanupForRelationships(userOrUsers) {
        if (!userOrUsers) {
            return userOrUsers;
        }
        if (Array.isArray(userOrUsers)) {
            return userOrUsers.map((user) => lodash_1.default.pick(user, ['id', 'firstName', 'lastName', 'email']));
        }
        return lodash_1.default.pick(userOrUsers, ['id', 'firstName', 'lastName', 'email']);
    }
    static async _populateRelationsForRows(rows, options) {
        if (!rows) {
            return rows;
        }
        return Promise.all(rows.map((record) => this._populateRelations(record, options)));
    }
    static async _populateRelations(record, options, filter = {}) {
        if (!record) {
            return record;
        }
        const output = record.get({ plain: true });
        output.tenants = await record.getTenants({
            ...filter,
            include: [
                {
                    model: options.database.tenant,
                    as: 'tenant',
                    required: true,
                    include: ['settings'],
                },
            ],
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
        return output;
    }
    /**
     * Maps the users data to show only the current tenant related info
     */
    static async _mapUserForTenantForRows(rows, tenant, options) {
        if (!rows) {
            return rows;
        }
        const segments = await options.database.segment.findAll({
            where: { tenantId: tenant.id },
        });
        return Promise.all(rows.map((record) => this._mapUserForTenant(record, tenant, options, segments)));
    }
    /**
     * Maps the user data to show only the current tenant related info
     */
    static async _mapUserForTenant(user, tenant, options, segments) {
        if (!user || !user.tenants) {
            return user;
        }
        const tenantUser = user.tenants.find((tenantUser) => tenantUser && tenantUser.tenant && String(tenantUser.tenant.id) === String(tenant.id));
        delete user.tenants;
        const status = tenantUser ? tenantUser.status : null;
        const roles = tenantUser ? tenantUser.roles : [];
        const adminSegments = tenantUser ? tenantUser.adminSegments : [];
        let adminSegmentsWithNames = adminSegments;
        if ((adminSegments === null || adminSegments === void 0 ? void 0 : adminSegments.length) > 0 && segments) {
            adminSegmentsWithNames = segments.filter((segment) => adminSegments.includes(segment.id));
        }
        // If the user is only invited,
        // tenant members can only see its email
        const otherData = status === 'active' ? user : {};
        return {
            ...otherData,
            id: user.id,
            email: user.email,
            fullName: user.fullName,
            roles,
            status,
            invitationToken: tenantUser === null || tenantUser === void 0 ? void 0 : tenantUser.invitationToken,
            adminSegments: adminSegmentsWithNames,
        };
    }
}
exports.default = UserRepository;
//# sourceMappingURL=userRepository.js.map