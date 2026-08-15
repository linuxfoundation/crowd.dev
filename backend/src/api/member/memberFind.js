"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /member/{id}
 * @summary Find a member
 * @tag Members
 * @security Bearer
 * @description Find a single member by ID.
 * @pathParam {string} id - The ID of the member
 * @response 200 - Ok
 * @responseContent {MemberResponse} 200.application/json
 * @responseExample {MemberFind} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const segmentId = ((_a = req.query.segments) === null || _a === void 0 ? void 0 : _a.length) > 0 ? req.query.segments[0] : null;
    const includeAllAttributes = req.query.includeAllAttributes === 'true' || req.query.includeAllAttributes === true;
    if (!segmentId) {
        await req.responseHandler.error(req, res, {
            code: 400,
            message: 'Segment ID is required',
        });
        return;
    }
    const payload = await new memberService_1.default(req).findById(req.params.id, segmentId, req.query.include, includeAllAttributes);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberFind.js.map