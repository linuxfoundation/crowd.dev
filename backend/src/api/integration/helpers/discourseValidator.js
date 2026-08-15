"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHasAny([
        permissions_1.default.values.integrationCreate,
        permissions_1.default.values.integrationEdit,
    ]);
    const { apiKey, apiUsername, forumHostname } = req.body;
    if (apiKey && apiUsername && forumHostname) {
        try {
            const result = await axios_1.default.get(`${forumHostname}/admin/users/list/active.json`, {
                headers: {
                    'Api-Key': apiKey,
                    'Api-Username': apiUsername,
                },
            });
            if (result.status === 200 && result.data && result.data.length > 0) {
                return req.responseHandler.success(req, res, result.data);
            }
        }
        catch (e) {
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
    }
    return req.responseHandler.error(req, res, new common_1.Error400(req.language));
};
//# sourceMappingURL=discourseValidator.js.map