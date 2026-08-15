"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert_1 = __importDefault(require("assert"));
const lodash_1 = __importDefault(require("lodash"));
const common_1 = require("@crowd/common");
const permissions_1 = __importDefault(require("../../security/permissions"));
const roles_1 = __importDefault(require("../../security/roles"));
const roles = roles_1.default.values;
/**
 * Checks the Permission of the User on a Tenant.
 */
class PermissionChecker {
    constructor({ currentTenant, language, currentUser, currentSegments }) {
        var _a;
        this.currentTenant = currentTenant;
        this.language = language;
        this.currentUser = currentUser;
        this.currentSegments = currentSegments;
        this.adminSegments = !currentUser
            ? []
            : (_a = currentUser.tenants.find((t) => t.tenantId === currentTenant.id)) === null || _a === void 0 ? void 0 : _a.adminSegments;
    }
    /**
     * Validates if the user has a specific permission
     * and throws a Error403 if it doesn't.
     */
    validateHas(permission) {
        if (!this.has(permission)) {
            throw new common_1.Error403(this.language);
        }
    }
    /**
     * Validates if the user has any permission among specified
     * and throws Error403 if it doesn't
     */
    validateHasAny(permissions) {
        const hasOne = permissions.some((p) => this.has(p));
        if (!hasOne) {
            throw new common_1.Error403(this.language);
        }
    }
    /**
     * Checks if the user has a specific permission.
     */
    has(permission) {
        (0, assert_1.default)(permission, 'permission is required');
        if (!this.currentUser) {
            throw new common_1.Error403(this.language, 'no currentUser');
        }
        if (!this.isEmailVerified) {
            throw new common_1.Error403(this.language, 'email not verified');
        }
        const allowedRoles = this.findAllowedRoles(permission);
        if (lodash_1.default.isEqual(allowedRoles, [roles.projectAdmin])) {
            this.validateSegmentPermission();
        }
        return true;
    }
    /**
     * Validates if the user has access to a storage.
     */
    hasStorage(storageId) {
        (0, assert_1.default)(storageId, 'storageId is required');
        return this.allowedStorageIds().includes(storageId);
    }
    /**
     * Checks if the user has any of the allowed roles for the permission.
     */
    findAllowedRoles(permission) {
        const allowedRoles = this.currentUserRolesIds.filter((role) => permission.allowedRoles.some((allowedRole) => allowedRole === role));
        if (allowedRoles.length === 0) {
            throw new common_1.Error403(this.language, `not allowed by role. Current: ${this.currentUserRolesIds}. Allowed: ${permission.allowedRoles}`);
        }
        return allowedRoles;
    }
    validateSegmentPermission() {
        const allowed = this.currentSegments.some((segment) => this.adminSegments.includes(segment.id));
        if (!allowed) {
            throw new common_1.Error403(this.language, 'not allowed by segment. ' +
                `Request segments: ${this.currentSegments.map((s) => s.id)}. ` +
                `User admin segments: ${this.adminSegments}`);
        }
    }
    get isEmailVerified() {
        return this.currentUser.emailVerified;
    }
    /**
     * Returns the Current User Roles.
     */
    get currentUserRolesIds() {
        if (!this.currentUser || !this.currentUser.tenants) {
            return [];
        }
        const tenant = this.currentUser.tenants
            .filter((tenantUser) => tenantUser.status === 'active')
            .find((tenantUser) => tenantUser.tenant.id === this.currentTenant.id);
        if (!tenant) {
            return [];
        }
        const userRoles = tenant.roles;
        if (userRoles.includes(roles.projectAdmin)) {
            return lodash_1.default.uniq(userRoles.concat(roles.readonly));
        }
        return userRoles;
    }
    /**
     * Returns the allowed storage ids for the user.
     */
    allowedStorageIds() {
        let allowedStorageIds = [];
        permissions_1.default.asArray.forEach((permission) => {
            if (this.has(permission)) {
                allowedStorageIds = allowedStorageIds.concat((permission.allowedStorage || []).map((storage) => storage.id));
            }
        });
        return [...new Set(allowedStorageIds)];
    }
}
exports.default = PermissionChecker;
//# sourceMappingURL=permissionChecker.js.map