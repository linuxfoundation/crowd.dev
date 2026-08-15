"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberAttributeSettingsService_1 = __importDefault(require("../../services/memberAttributeSettingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PUT /settings/members/attributes/{id}
 * @summary Attribute settings: update
 * @tag Members
 * @security Bearer
 * @description Update a members' attribute setting
 * @pathParam {string} id - The ID of the member attribute settings
 * @bodyContent {MemberAttributeSettingsUpdateInput} application/json
 * @response 200 - Ok
 * @responseContent {MemberAttributeSettings} 200.application/json
 * @responseExample {MemberAttributeSettings} 200.application/json.MemberAttributeSettings
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberAttributesEdit);
    const payload = await new memberAttributeSettingsService_1.default(req).update(req.params.id, req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAttributeUpdate.js.map