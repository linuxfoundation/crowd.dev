"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberService_1 = __importDefault(require("@/services/memberService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /member/bot-suggestions
 * @summary List member bot suggestions
 * @tag Members
 * @security Bearer
 * @description List member bot suggestions with pagination
 * @queryParam {number} [offset] - Skip the first n results. Default 0.
 * @queryParam {number} [limit] - Limit the number of results. Default 20.
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const payload = await new memberService_1.default(req).findMembersWithBotSuggestions(req.query);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberBotSuggestionsList.js.map