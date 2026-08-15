"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const integrations_1 = require("@crowd/data-access-layer/src/integrations");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /segments/{id}/repositories
 * @summary Get repositories for a segment
 * @tag Segments
 * @security Bearer
 * @description Get repositories for a segment
 * @pathParam {string} id - The ID of the segment
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.collectionRead);
    const qx = sequelizeRepository_1.default.getQueryExecutor(req);
    const payload = await (0, integrations_1.findRepositoriesForSegment)(qx, req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=segmentsRepositoriesGet.js.map