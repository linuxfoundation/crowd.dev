"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const customViewService_1 = __importDefault(require("../../services/customViewService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /customview/query
 * @summary Query custom views
 * @tag CustomViews
 * @security Bearer
 * @description Query custom views. It accepts filters and sorting options.
 * @queryParam {string[]} placement - The placements to filter by
 * @queryParam {string} visibility - The visibility to filter by
 * @response 200 - Ok
 * @responseContent {CustomViewList} 200.application/json
 * @responseExample {CustomViewList} 200.application/json.CustomView
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.activityRead);
    const payload = await new customViewService_1.default(req).findAll(req.query);
    if (req.query.filter && Object.keys(req.query.filter).length > 0) {
        (0, track_1.default)('Custom views Filter', { ...req.query }, { ...req });
    }
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=customViewQuery.js.map