"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberAffiliationsService_1 = __importDefault(require("@/services/member/memberAffiliationsService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * PUT /member/:memberId/affiliation
 * @summary Upsert member affiliations
 * @tag Members
 * @security Bearer
 * @description Upsert multiple member affiliations.
 * @pathParam {string} memberId - member ID
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @responseExample {MemberList} 200.application/json.MemberAffiliation
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const memberAffiliationsService = new memberAffiliationsService_1.default(req);
    const payload = await memberAffiliationsService.upsertMultiple(req.params.memberId, req.body.affiliations);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAffiliationUpdateMultiple.js.map