"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isUserInTenant = isUserInTenant;
function isUserInTenant(user, tenant) {
    if (!user) {
        return false;
    }
    if (!tenant || !tenant.id) {
        return true;
    }
    return user.tenants.some((tenantUser) => tenantUser.tenant.id === tenant.id);
}
//# sourceMappingURL=userTenantUtils.js.map