"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberOrganizationsService_1 = __importDefault(require("@/services/member/memberOrganizationsService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * POST /member/:memberId/organization
 * @summary Create member organization
 * @tag Members
 * @security Bearer
 * @description Create one member organization.
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @responseExample {MemberList} 200.application/json.Organization
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const memberOrganizationsService = new memberOrganizationsService_1.default(req);
    const payload = await memberOrganizationsService.create(req.params.memberId, req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberOrganizationCreate.js.map