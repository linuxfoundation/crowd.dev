"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /organization/query
 * @summary Query organizations
 * @tag Organizations
 * @security Bearer
 * @description Query organizations. It accepts filters, sorting options and pagination.
 * @bodyContent {OrganizationQuery} application/json
 * @response 200 - Ok
 * @responseContent {OrganizationList} 200.application/json
 * @responseExample {OrganizationList} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationRead);
    const payload = await new organizationService_1.default(req).query(req.body);
    if (((_a = req.body) === null || _a === void 0 ? void 0 : _a.filter) && Object.keys(req.body.filter).length > 0) {
        (0, track_1.default)('Organizations Advanced Filter', { ...req.body }, { ...req });
    }
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationQuery.js.map