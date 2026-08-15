"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberAttributeSettingsService_1 = __importDefault(require("../../services/memberAttributeSettingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /settings/members/attributes
 * @summary Attributes settings: list
 * @tag Members
 * @security Bearer
 * @description Get a list of members' attribute settings
 * @queryParam {string} [filter[label]] - Filter by label of member attribute settings
 * @queryParam {string} [filter[name]] - Filter by name of member attribute settings
 * @queryParam {string} [filter[type]] - Filter by type of member attribute settings
 * @queryParam {string} [filter[canDelete]] - Filter by canDelete: "true" or "false"
 * @queryParam {string} [filter[show]] - Filter by show: "true" or "false"
 * @queryParam {string} [filter[createdAtRange]] - CreatedAt lower bound. If you want a range, send this parameter twice with [min] and [max]. If you send it once it will be interpreted as a lower bound.
 * @queryParam {MemberAttributeSettingsSort} [orderBy] - Sort the results. Default createdAt_DESC.
 * @queryParam {number} [offset] - Skip the first n results. Default 0.
 * @queryParam {number} [limit] - Limit the number of results. Default 50.
 * @response 200 - Ok
 * @responseContent {MemberAttributeSettingsList} 200.application/json
 * @responseExample {MemberAttributeSettingsList} 200.application/json.MemberAttributeSettings
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberAttributesRead);
    const payload = await new memberAttributeSettingsService_1.default(req).findAndCountAll(req.query);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAttributeList.js.map