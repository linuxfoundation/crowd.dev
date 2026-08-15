"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = identifyTenant;
const types_1 = require("@crowd/types");
const conf_1 = require("../conf");
async function identifyTenant(req) {
    if (conf_1.SEGMENT_CONFIG.writeKey) {
        const Analytics = require('analytics-node');
        const analytics = new Analytics(conf_1.SEGMENT_CONFIG.writeKey);
        if (conf_1.API_CONFIG.edition === types_1.Edition.CROWD_HOSTED || conf_1.API_CONFIG.edition === types_1.Edition.LFX) {
            if (!req.currentUser.email.includes('help@crowd.dev')) {
                analytics.group({
                    userId: req.currentUser.id,
                    groupId: req.currentTenant.id,
                    traits: {
                        name: req.currentTenant.name,
                    },
                });
            }
        }
        else if (conf_1.API_CONFIG.edition === types_1.Edition.COMMUNITY) {
            if (!req.currentUser.email.includes('crowd.dev')) {
                analytics.group({
                    userId: req.currentUser.id,
                    groupId: req.currentTenant.id,
                    traits: {
                        createdAt: req.currentTenant.createdAt,
                    },
                });
            }
        }
    }
}
//# sourceMappingURL=identifyTenant.js.map