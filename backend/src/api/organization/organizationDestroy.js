"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * DELETE /organization/{id}
 * @summary Delete a organization
 * @tag Organizations
 * @security Bearer
 * @description Delete a organization.
 * @pathParam {string} id - The ID of the organization
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationDestroy);
    await new organizationService_1.default(req).destroyAll(req.query.ids);
    const payload = true;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationDestroy.js.map