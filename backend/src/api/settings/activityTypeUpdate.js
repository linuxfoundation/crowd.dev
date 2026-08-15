"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const segmentService_1 = __importDefault(require("../../services/segmentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PUT /settings/activity/types/{key}
 * @summary Update an activity type
 * @tag Activities
 * @security Bearer
 * @description Update a custom activity type
 * @pathParam {string} key - The key of the activity type
 * @bodyContent {ActivityTypesUpdateInput} application/json
 * @response 200 - Ok
 * @responseContent {ActivityTypes} 200.application/json
 * @responseExample {ActivityTypes} 200.application/json.ActivityTypes
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.segmentEdit);
    const payload = await new segmentService_1.default(req).updateActivityType(req.params.key, req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=activityTypeUpdate.js.map