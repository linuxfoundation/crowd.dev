"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PUT /organization/{id}
 * @summary Update an organization
 * @tag Organizations
 * @security Bearer
 * @description Update a organization
 * @pathParam {string} id - The ID of the organization
 * @bodyContent {OrganizationInput} application/json
 * @response 200 - Ok
 * @responseContent {Organization} 200.application/json
 * @responseExample {OrganizationCreate} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationEdit);
    const payload = await new organizationService_1.default(req).update(req.params.id, req.body, true, true, true);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationUpdate.js.map