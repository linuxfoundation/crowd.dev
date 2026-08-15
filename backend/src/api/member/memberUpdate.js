"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PUT /member/{id}
 * @summary Update a member
 * @tag Members
 * @security Bearer
 * @description Update a member
 * @pathParam {string} id - The ID of the member
 * @bodyContent {MemberUpdateInput} application/json
 * @response 200 - Ok
 * @responseContent {Member} 200.application/json
 * @responseExample {MemberUpsert} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const { invalidateCache, ...data } = req.body;
    const payload = await new memberService_1.default(req).update(req.params.id, data, {
        syncToOpensearch: true,
        manualChange: true,
        invalidateCache: invalidateCache !== null && invalidateCache !== void 0 ? invalidateCache : false,
    });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberUpdate.js.map