"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = identify;
const types_1 = require("@crowd/types");
const conf_1 = require("../conf");
function identify(user) {
    const Analytics = require('analytics-node');
    if (conf_1.SEGMENT_CONFIG.writeKey) {
        const analytics = new Analytics(conf_1.SEGMENT_CONFIG.writeKey);
        if (conf_1.API_CONFIG.edition === types_1.Edition.CROWD_HOSTED || conf_1.API_CONFIG.edition === types_1.Edition.LFX) {
            if (user.email !== 'help@crowd.dev') {
                analytics.identify({
                    userId: user.id,
                    traits: {
                        name: user.fullName,
                        email: user.email,
                        createdAt: user.createdAt,
                        tenants: user.tenants.map((tenantUser) => ({
                            id: tenantUser.tenant.id,
                            name: tenantUser.tenant.name,
                            url: tenantUser.tenant.url,
                        })),
                        // Hubspot custom traits
                        created_an_account: true,
                        created_an_account__date: user.createdAt,
                    },
                });
            }
        }
        else if (conf_1.API_CONFIG.edition === types_1.Edition.COMMUNITY) {
            if (!user.email.includes('crowd.dev')) {
                analytics.identify({
                    userId: user.id,
                    traits: {
                        createdAt: user.createdAt,
                        tenants: user.tenants.map((tenantUser) => ({
                            id: tenantUser.tenant.id,
                        })),
                    },
                });
            }
        }
    }
}
//# sourceMappingURL=identify.js.map