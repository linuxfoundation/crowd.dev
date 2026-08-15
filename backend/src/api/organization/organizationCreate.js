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
 * POST /organization
 * @summary Create a organization
 * @tag Organizations
 * @security Bearer
 * @description Create a organization
 * @bodyContent {OrganizationInput} application/json
 * @response 200 - Ok
 * @responseContent {Organization} 200.application/json
 * @responseExample {OrganizationCreate} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationCreate);
    const payload = await new organizationService_1.default(req).createOrUpdate(req.body);
    (0, track_1.default)('Organization Manually Created', { ...req.body }, { ...req });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationCreate.js.map