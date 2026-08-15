"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /member
 * @summary Create or update a member
 * @tag Members
 * @security Bearer
 * @description Create or update a member. Existence is checked by platform and username.
 * @bodyContent {MemberUpsertInput} application/json
 * @response 200 - Ok
 * @responseContent {Member} 200.application/json
 * @responseExample {MemberUpsert} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberCreate);
    const payload = await new memberService_1.default(req).upsert(req.body);
    (0, track_1.default)('Member Manually Created', { ...req.body }, { ...req });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberCreate.js.map