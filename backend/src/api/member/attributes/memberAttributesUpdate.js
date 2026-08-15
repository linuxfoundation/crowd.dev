"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberAttributesService_1 = __importDefault(require("@/services/member/memberAttributesService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * PATCH /member/:memberId/attributes
 * @summary Update member attributes
 * @tag Members
 * @security Bearer
 * @description Update member attributes.
 * @pathParam {string} memberId - member ID
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @responseExample {MemberList} 200.application/json.Attributes
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const memberAttributesService = new memberAttributesService_1.default(req);
    // defaults to true unless query param is 'false'
    const manuallyChanged = req.query.manuallyChanged !== 'false';
    // remove segments from body
    delete req.body.segments;
    const payload = await memberAttributesService.update(req.params.memberId, req.body, manuallyChanged);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAttributesUpdate.js.map