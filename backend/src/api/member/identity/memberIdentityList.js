"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberIdentityService_1 = __importDefault(require("@/services/member/memberIdentityService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * GET /member/:memberId/identity
 * @summary Query member identities
 * @tag Members
 * @security Bearer
 * @description Query member identities.
 * @pathParam {string} memberId - member ID
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @responseExample {MemberList} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const memberIdentityService = new memberIdentityService_1.default(req);
    const payload = await memberIdentityService.list(req.params.memberId);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberIdentityList.js.map