"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const categoryService_1 = require("@/services/categoryService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PATCH /category/:id
 * @summary Update a category
 * @tag Categories
 * @security Bearer
 * @description Update a category
 * @bodyContent {CategoryUpdateInput} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.categoryEdit);
    const service = new categoryService_1.CategoryService(req);
    const payload = await service.updateCategory(req.params.id, req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=categoryUpdate.js.map