"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.tenantSubdomain = void 0;
const conf_1 = require("../conf");
const configTypes_1 = require("../conf/configTypes");
exports.tenantSubdomain = {
    frontendUrl(tenant) {
        const frontendUrlWithSubdomain = conf_1.API_CONFIG.frontendUrlWithSubdomain;
        if (conf_1.TENANT_MODE !== configTypes_1.TenantMode.MULTI_WITH_SUBDOMAIN ||
            !frontendUrlWithSubdomain ||
            !tenant ||
            !tenant.url) {
            return conf_1.API_CONFIG.frontendUrl;
        }
        return frontendUrlWithSubdomain.replace('[subdomain]', tenant.url);
    },
};
//# sourceMappingURL=tenantSubdomain.js.map