"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const MergeActionsService_1 = __importDefault(require("@/services/MergeActionsService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /mergeAction
 * @summary Query mergeActions
 * @tag MergeActions
 * @security Bearer
 * @description Query mergeActions. It accepts filters and pagination.
 * @queryParam {string} entityId - ID of the entity
 * @queryParam {string} type - type of the entity (e.g., org or member)
 * @queryParam {number} [limit] - number of records to return (optional, default to 20)
 * @queryParam {number} [offset] - number of records to skip (optional, default to 0)
 * @response 200 - Ok
 * @responseContent {MergeActionList} 200.application/json
 * @responseExample {MergeActionList} 200.application/json.MergeAction
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.mergeActionRead);
    const payload = await new MergeActionsService_1.default(req).query(req.query);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=mergeActionQuery.js.map