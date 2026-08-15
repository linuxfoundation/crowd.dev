"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /organization/{id}
 * @summary Find an organization
 * @tag Organizations
 * @security Bearer
 * @description Find an organization by ID.
 * @pathParam {string} id - The ID of the organization
 * @response 200 - Ok
 * @responseContent {OrganizationResponse} 200.application/json
 * @responseExample {Organization} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationRead);
    const segmentId = req.query.segmentId;
    const payload = await new organizationService_1.default(req).findById(req.params.id, segmentId);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationFind.js.map