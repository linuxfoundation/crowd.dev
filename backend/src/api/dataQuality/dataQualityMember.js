"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dataQualityService_1 = __importDefault(require("@/services/dataQualityService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /data-quality/member
 * @summary Find a member data issues
 * @tag Data Quality
 * @security Bearer
 * @description Find a data quality issues for members
 * @response 200 - Ok
 * @responseContent {DataQualityResponse} 200.application/json
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const segmentId = ((_a = req.query.segments) === null || _a === void 0 ? void 0 : _a.length) > 0 ? req.query.segments[0] : null;
    if (!segmentId) {
        await req.responseHandler.error(req, res, {
            code: 400,
            message: 'Segment ID is required',
        });
        return;
    }
    const payload = await new dataQualityService_1.default(req).findMemberIssues(req.query, segmentId);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=dataQualityMember.js.map