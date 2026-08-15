"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const collectionService_1 = require("@/services/collectionService");
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * GET /segments/{id}/github-insights
 * @summary Get github insights for a segment
 * @tag Segments
 * @security Bearer
 * @description Get github insights for a segment
 * @pathParam {string} id - The ID of the segment
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.collectionRead);
    const service = new collectionService_1.CollectionService(req);
    const payload = await service.findGithubInsightsForSegment(req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=segmentsGithubInsightsGet.js.map