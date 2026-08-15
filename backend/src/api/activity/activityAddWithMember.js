"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const activityService_1 = __importDefault(require("../../services/activityService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /activity/with-member
 * @summary Create or update an activity with a member
 * @tag Activities
 * @security Bearer
 * @description Create or update an activity with a member
 * Activity existence is checked by sourceId
 * Member existence is checked by platform and username
 * @bodyContent {ActivityUpsertWithMemberInput} application/json
 * @response 200 - Ok
 * @responseContent {Activity} 200.application/json
 * @responseExample {ActivityUpsert} 200.application/json.Activity
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.activityCreate);
    const payload = await new activityService_1.default(req).createWithMember(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=activityAddWithMember.js.map