"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const categoryService_1 = require("@/services/categoryService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * DELETE /category-groups/{id}
 * @summary Delete a category group
 * @tag Category Groups
 * @security Bearer
 * @description Delete a category group by ID
 * @pathParam {string} id - The ID of the category group
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.categoryEdit);
    const service = new categoryService_1.CategoryService(req);
    await service.deleteCategoryGroup(req.params.id);
    await req.responseHandler.success(req, res, true);
};
//# sourceMappingURL=categoryGroupDelete.js.map