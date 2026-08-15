"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
const conf_1 = require("../../../conf");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHasAny([
        permissions_1.default.values.integrationCreate,
        permissions_1.default.values.integrationEdit,
    ]);
    if (req.query.keywords) {
        try {
            const promises = req.query.keywords.split(';').map((keyword) => axios_1.default.get(`https://api.stackexchange.com/2.3/search/advanced`, {
                params: {
                    site: 'stackoverflow',
                    q: `"${keyword}"`,
                    filter: 'total',
                    key: conf_1.STACKEXCHANGE_CONFIG.key,
                },
            }));
            const responses = await Promise.all(promises);
            if (responses.every((response) => response.status === 200)) {
                return req.responseHandler.success(req, res, {
                    total: responses.reduce((acc, response) => acc + response.data.total, 0),
                });
            }
        }
        catch (e) {
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
    }
    return req.responseHandler.error(req, res, new common_1.Error400(req.language));
};
//# sourceMappingURL=stackOverflowVolume.js.map