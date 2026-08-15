"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const bcrypt_1 = __importDefault(require("bcrypt"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const moment_1 = __importDefault(require("moment"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const conf_1 = require("../../conf");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const tenantUserRepository_1 = __importDefault(require("../../database/repositories/tenantUserRepository"));
const userRepository_1 = __importDefault(require("../../database/repositories/userRepository"));
const roles_1 = __importDefault(require("../../security/roles"));
const identify_1 = __importDefault(require("../../segment/identify"));
const track_1 = __importDefault(require("../../segment/track"));
const tenantService_1 = __importDefault(require("../tenantService"));
const BCRYPT_SALT_ROUNDS = 12;
const log = (0, logging_1.getServiceChildLogger)('AuthService');
class AuthService {
    static async signup(email, password, invitationToken, tenantId, firstName, lastName, acceptedTermsAndPrivacy, options = {}) {
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            email = email.toLowerCase();
            const existingUser = await userRepository_1.default.findByEmail(email, options);
            const passwordRegex = /^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z\d])([^ \t]{8,})$/;
            if (!passwordRegex.test(password)) {
                throw new common_1.Error400(options.language, 'auth.passwordInvalid');
            }
            // Generates a hashed password to hide the original one.
            const hashedPassword = await bcrypt_1.default.hash(password, BCRYPT_SALT_ROUNDS);
            // The user may already exist on the database in case it was invided.
            if (existingUser) {
                // If the user already have an password,
                // it means that it has already signed up
                const existingPassword = await userRepository_1.default.findPassword(existingUser.id, options);
                if (existingPassword) {
                    throw new common_1.Error400(options.language, 'auth.emailAlreadyInUse');
                }
                /**
                 * In the case of the user exists on the database (was invited)
                 * it only creates the new password
                 */
                await userRepository_1.default.updatePassword(existingUser.id, hashedPassword, false, {
                    ...options,
                    transaction,
                    bypassPermissionValidation: true,
                });
                // Handles onboarding process like
                // invitation, creation of default tenant,
                // or default joining the current tenant
                await this.handleOnboard(existingUser, { invitationToken, tenantId }, {
                    ...options,
                    transaction,
                });
                const token = jsonwebtoken_1.default.sign({ id: existingUser.id }, conf_1.API_CONFIG.jwtSecret, {
                    expiresIn: conf_1.API_CONFIG.jwtExpiresIn,
                });
                await sequelizeRepository_1.default.commitTransaction(transaction);
                // Identify in Segment
                (0, identify_1.default)(existingUser);
                (0, track_1.default)('Signed up', {
                    invitation: true,
                    email: existingUser.email,
                }, options, existingUser.id);
                return token;
            }
            firstName = firstName || email.split('@')[0];
            lastName = lastName || '';
            const fullName = `${firstName} ${lastName}`.trim();
            const newUser = await userRepository_1.default.createFromAuth({
                firstName,
                lastName,
                fullName,
                password: hashedPassword,
                email,
                acceptedTermsAndPrivacy,
            }, {
                ...options,
                transaction,
            });
            // Handles onboarding process like
            // invitation, creation of default tenant,
            // or default joining the current tenant
            await this.handleOnboard(newUser, { invitationToken, tenantId }, {
                ...options,
                transaction,
            });
            // Identify in Segment
            (0, identify_1.default)(newUser);
            (0, track_1.default)('Signed up', {
                invitation: true,
                email: newUser.email,
            }, options, newUser.id);
            const token = jsonwebtoken_1.default.sign({ id: newUser.id }, conf_1.API_CONFIG.jwtSecret, {
                expiresIn: conf_1.API_CONFIG.jwtExpiresIn,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return token;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    static async findByEmail(email, options = {}) {
        email = email.toLowerCase();
        return userRepository_1.default.findByEmail(email, options);
    }
    static async signin(email, password, invitationToken, tenantId, options = {}) {
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            email = email.toLowerCase();
            const user = await userRepository_1.default.findByEmail(email, options);
            if (!user) {
                throw new common_1.Error400(options.language, 'auth.userNotFound');
            }
            const currentPassword = await userRepository_1.default.findPassword(user.id, options);
            if (!currentPassword) {
                throw new common_1.Error400(options.language, 'auth.wrongPassword');
            }
            const passwordsMatch = await bcrypt_1.default.compare(password, currentPassword);
            if (!passwordsMatch) {
                throw new common_1.Error400(options.language, 'auth.wrongPassword');
            }
            // Handles onboarding process like
            // invitation, creation of default tenant,
            // or default joining the current tenant
            await this.handleOnboard(user, { invitationToken, tenantId }, {
                ...options,
                currentUser: user,
                transaction,
            });
            const token = jsonwebtoken_1.default.sign({ id: user.id }, conf_1.API_CONFIG.jwtSecret, {
                expiresIn: conf_1.API_CONFIG.jwtExpiresIn,
            });
            (0, identify_1.default)(user);
            (0, track_1.default)('Signed in', {
                email: user.email,
            }, options, user.id);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return token;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    static async handleOnboard(currentUser, { invitationToken = null, tenantId = null, roles = [] }, options) {
        if (roles === undefined) {
            roles = [];
        }
        if (invitationToken) {
            try {
                await tenantUserRepository_1.default.acceptInvitation(invitationToken, {
                    ...options,
                    currentUser,
                    bypassPermissionValidation: true,
                });
            }
            catch (error) {
                log.error(error, 'Error handling onboard!');
                // In case of invitation acceptance error, does not prevent
                // the user from sign up/in
            }
        }
        if (tenantId) {
            await new tenantService_1.default({
                ...options,
                currentUser,
            }).joinWithDefaultRolesOrAskApproval({
                tenantId,
                roles,
            }, options);
        }
        else {
            // In case is single tenant, and the user is signing in
            // with an invited email and for some reason doesn't have the token
            // it auto-assigns it
            await new tenantService_1.default({
                ...options,
                currentUser,
            }).joinDefaultUsingInvitedEmail(options.transaction);
            // Creates or join default Tenant
            await new tenantService_1.default({
                ...options,
                currentUser,
            }).createOrJoinDefault({
                roles,
            }, options.transaction);
        }
    }
    static async findByToken(token, options) {
        return new Promise((resolve, reject) => {
            jsonwebtoken_1.default.verify(token, conf_1.API_CONFIG.jwtSecret, (err, decoded) => {
                if (err) {
                    options.log.error(`Error verifying token with secret: ${conf_1.API_CONFIG.jwtSecret.substring(0, 5)}.....`, err);
                    reject(err);
                    return;
                }
                const { id } = decoded;
                const jwtTokenIat = decoded.iat;
                userRepository_1.default.findById(id, {
                    ...options,
                    bypassPermissionValidation: true,
                })
                    .then((user) => {
                    const isTokenManuallyExpired = user &&
                        user.jwtTokenInvalidBefore &&
                        moment_1.default.unix(jwtTokenIat).isBefore((0, moment_1.default)(user.jwtTokenInvalidBefore));
                    if (isTokenManuallyExpired) {
                        reject(new common_1.Error401());
                        return;
                    }
                    if (user) {
                        user.emailVerified = true;
                    }
                    resolve(user);
                })
                    .catch((error) => reject(error));
            });
        });
    }
    static async changePassword(oldPassword, newPassword, options) {
        const { currentUser } = options;
        const currentPassword = await userRepository_1.default.findPassword(options.currentUser.id, options);
        const passwordsMatch = await bcrypt_1.default.compare(oldPassword, currentPassword);
        if (!passwordsMatch) {
            throw new common_1.Error400(options.language, 'auth.passwordChange.invalidPassword');
        }
        const newHashedPassword = await bcrypt_1.default.hash(newPassword, BCRYPT_SALT_ROUNDS);
        return userRepository_1.default.updatePassword(currentUser.id, newHashedPassword, true, options);
    }
    static async signinFromSocial(provider, providerId, email, emailVerified, firstName, lastName, fullName, options = {}) {
        if (!email) {
            throw new Error('auth-no-email');
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            email = email.toLowerCase();
            let user = await userRepository_1.default.findByEmail(email, options);
            if (user) {
                (0, identify_1.default)(user);
                (0, track_1.default)('Signed in', {
                    [provider]: true,
                    email: user.email,
                }, options, user.id);
            }
            // If there was no provider, we can link it to the provider
            if (user && (user.provider === undefined || user.provider === null || user.emailVerified)) {
                await userRepository_1.default.update(user.id, {
                    firstName,
                    lastName,
                    provider,
                    providerId,
                    emailVerified,
                }, options);
                log.debug({ user }, 'User');
            }
            else if (user && (user.provider !== provider || user.providerId !== providerId)) {
                throw new Error('auth-invalid-provider');
            }
            if (!user) {
                user = await userRepository_1.default.createFromSocial(provider, providerId, email, emailVerified, firstName, lastName, fullName, options);
                (0, identify_1.default)(user);
                (0, track_1.default)('Signed up', {
                    [provider]: true,
                    email: user.email,
                }, options, user.id);
            }
            const token = jsonwebtoken_1.default.sign({ id: user.id }, conf_1.API_CONFIG.jwtSecret, {
                expiresIn: conf_1.API_CONFIG.jwtExpiresIn,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return token;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    static async signinFromSSO(provider, providerId, email, emailVerified, firstName, lastName, fullName, invitationToken, tenantId, roles, options = {}) {
        if (!email) {
            throw new Error('auth-no-email');
        }
        if (roles) {
            roles = AuthService.translateLFRoles(roles);
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            let user = await userRepository_1.default.findByProviderId(providerId, options);
            if (!user) {
                user = await userRepository_1.default.findByEmail(email, options);
            }
            if (user && (user.provider !== provider || user.providerId !== providerId)) {
                await userRepository_1.default.update(user.id, {
                    firstName,
                    lastName,
                    provider,
                    providerId,
                    emailVerified,
                }, options);
            }
            if (user) {
                (0, identify_1.default)(user);
                (0, track_1.default)('Signed in', {
                    [provider]: true,
                    email: user.email,
                }, options, user.id);
            }
            if (!user) {
                user = await userRepository_1.default.createFromSocial(provider, providerId, email, emailVerified, firstName, lastName, fullName, options);
                (0, identify_1.default)(user);
                (0, track_1.default)('Signed up', {
                    [provider]: true,
                    email: user.email,
                }, options, user.id);
            }
            if (invitationToken) {
                await this.handleOnboard(user, { invitationToken, tenantId, roles }, {
                    ...options,
                    transaction,
                });
            }
            else if (user.tenants.length === 0) {
                // if email ends with '@crowd.dev'
                if (email.endsWith('@crowd.dev') && conf_1.SSO_CONFIG.crowdTenantId) {
                    await this.handleOnboard(user, { tenantId: conf_1.SSO_CONFIG.crowdTenantId, roles }, {
                        ...options,
                        transaction,
                    });
                }
                else if (conf_1.SSO_CONFIG.lfTenantId) {
                    await this.handleOnboard(user, { tenantId: conf_1.SSO_CONFIG.lfTenantId, roles }, {
                        ...options,
                        transaction,
                    });
                }
                else {
                    await this.handleOnboard(user, { roles }, {
                        ...options,
                        transaction,
                    });
                }
            }
            else {
                for (const tenantUser of user.tenants) {
                    const tenantUserId = tenantUser.dataValues.id;
                    await tenantUserRepository_1.default.replaceRoles(tenantUserId, roles, {
                        ...options,
                        transaction,
                        currentTenant: {
                            id: tenantUser.dataValues.tenantId,
                        },
                    });
                }
            }
            const token = jsonwebtoken_1.default.sign({ id: user.id }, conf_1.API_CONFIG.jwtSecret, {
                expiresIn: conf_1.API_CONFIG.jwtExpiresIn,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return token;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    static translateLFRoles(roles) {
        return roles.map((role) => {
            if (role === 'viewer') {
                return roles_1.default.values.readonly;
            }
            return role;
        });
    }
}
exports.default = AuthService;
//# sourceMappingURL=authService.js.map