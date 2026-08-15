"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * DELETE /member/{id}
 * @summary Delete a member
 * @tag Members
 * @security Bearer
 * @description Delete a member given an ID
 * @pathParam {string} id - The ID of the member
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberDestroy);
    await new memberService_1.default(req).destroyAll(req.query.ids);
    const payload = true;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberDestroy.js.map