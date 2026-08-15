"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const categoryService_1 = require("@/services/categoryService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * DELETE /category/{id}
 * @summary Delete a category
 * @tag Categories
 * @security Bearer
 * @description Delete a category by ID
 * @pathParam {string} id - The ID of the category
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.categoryEdit);
    const service = new categoryService_1.CategoryService(req);
    await service.deleteCategory(req.params.id);
    await req.responseHandler.success(req, res, true);
};
//# sourceMappingURL=categoryDelete.js.map