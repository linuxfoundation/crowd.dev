"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const categoryService_1 = require("@/services/categoryService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * Get /category
 * @summary List Category
 * @tag Category
 * @security Bearer
 * @description Query category with filters
 * @bodyContent {CategoryQuery} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.categoryRead);
    const service = new categoryService_1.CategoryService(req);
    const payload = await service.listCategories(req.query);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=categoryList.js.map