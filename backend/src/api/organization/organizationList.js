"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const organizationService_1 = __importDefault(require("@/services/organizationService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /organization/list
 * @summary List organizations across all segments
 * @tag Organizations
 * @security Bearer
 * @description List organizations across all segments. It accepts filters, sorting options and pagination.
 * @bodyContent {OrganizationQuery} application/json
 * @response 200 - Ok
 * @responseContent {OrganizationList} 200.application/json
 * @responseExample {OrganizationList} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationRead);
    const orgService = new organizationService_1.default(req);
    const payload = await orgService.listOrganizationsAcrossAllSegments(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationList.js.map