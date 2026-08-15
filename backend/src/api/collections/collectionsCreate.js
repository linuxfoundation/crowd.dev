"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const collectionService_1 = require("@/services/collectionService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /collections
 * @summary Create a collection
 * @tag Collections
 * @security Bearer
 * @description Create a new collection
 * @bodyContent {CollectionCreateInput} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.collectionEdit);
    const service = new collectionService_1.CollectionService(req);
    const payload = await service.createCollection(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=collectionsCreate.js.map