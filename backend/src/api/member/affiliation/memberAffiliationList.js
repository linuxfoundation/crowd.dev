"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberAffiliationsService_1 = __importDefault(require("@/services/member/memberAffiliationsService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * GET /member/:memberId/affiliation
 * @summary List member affiliations
 * @tag Members
 * @security Bearer
 * @description Query member affiliations.
 * @pathParam {string} memberId - member ID
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @responseExample {MemberList} 200.application/json.MemberAffiliation
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const memberAffiliationsService = new memberAffiliationsService_1.default(req);
    const payload = await memberAffiliationsService.list(req.params.memberId);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAffiliationList.js.map