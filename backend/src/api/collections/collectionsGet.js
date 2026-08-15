"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const collectionService_1 = require("@/services/collectionService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /collections/{id}
 * @summary Get a collection
 * @tag Collections
 * @security Bearer
 * @description Get a collection by ID
 * @pathParam {string} id - The ID of the collection
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.collectionRead);
    const service = new collectionService_1.CollectionService(req);
    const payload = await service.findById(req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=collectionsGet.js.map