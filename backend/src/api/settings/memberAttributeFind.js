"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberAttributeSettingsService_1 = __importDefault(require("../../services/memberAttributeSettingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /settings/members/attributes/{id}
 * @summary Attributes settings: find
 * @tag Members
 * @security Bearer
 * @description Find a single members' attribute setting by ID
 * @pathParam {string} id - The ID of the member attribute's settings
 * @response 200 - Ok
 * @responseContent {MemberAttributeSettings} 200.application/json
 * @responseExample {MemberAttributeSettings} 200.application/json.MemberAttributeSettings
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberAttributesRead);
    const payload = await new memberAttributeSettingsService_1.default(req).findById(req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAttributeFind.js.map