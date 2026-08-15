"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const segmentService_1 = __importDefault(require("../../services/segmentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /settings/activity/types
 * @summary Create an activity type
 * @tag Activities
 * @security Bearer
 * @description Create a custom activity type
 * @bodyContent {ActivityTypesCreateInput} application/json
 * @response 200 - Ok
 * @responseContent {ActivityTypes} 200.application/json
 * @responseExample {ActivityTypes} 200.application/json.ActivityTypes
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.segmentCreate);
    const payload = await new segmentService_1.default(req).createActivityType(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=activityTypeCreate.js.map