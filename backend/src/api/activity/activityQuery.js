"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const activityService_1 = __importDefault(require("../../services/activityService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /activity/query
 * @summary Query activities
 * @tag Activities
 * @security Bearer
 * @description Query activities. It accepts filters, sorting options and pagination.
 * @bodyContent {ActivityQuery} application/json
 * @response 200 - Ok
 * @responseContent {ActivityList} 200.application/json
 * @responseExample {ActivityList} 200.application/json.Activity
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.activityRead);
    const service = new activityService_1.default(req);
    const payload = await service.query(req.body);
    if (((_a = req.body) === null || _a === void 0 ? void 0 : _a.filter) && Object.keys(req.body.filter).length > 0) {
        (0, track_1.default)('Activities Advanced Filter', { ...req.body }, { ...req });
    }
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=activityQuery.js.map