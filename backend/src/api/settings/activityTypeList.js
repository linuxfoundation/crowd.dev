"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const segmentService_1 = __importDefault(require("../../services/segmentService"));
/**
 * GET /settings/activity/types
 * @summary List all activity types
 * @tag Activities
 * @security Bearer
 * @description List all activity types
 * @response 200 - Ok
 * @responseContent {ActivityTypes} 200.application/json
 * @responseExample {ActivityTypes} 200.application/json.ActivityTypes
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    const payload = segmentService_1.default.listActivityTypes(req);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=activityTypeList.js.map