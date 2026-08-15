"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_services_1 = require("@crowd/common_services");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const { memberId } = req.params;
    const { memberToMerge } = req.body;
    const service = new common_services_1.CommonMemberService((0, sequelizeQueryExecutor_1.optionsQx)(req), req.temporal, req.log);
    const payload = await service.merge(memberId, memberToMerge, req);
    try {
        await (0, common_services_1.invalidateMemberQueryCache)(req.redis, [memberId, memberToMerge]);
    }
    catch (error) {
        req.log.warn({ error }, 'Cache invalidation failed after member merge');
    }
    (0, track_1.default)('Merge members', { memberId, memberToMergeId: memberToMerge }, req);
    return req.responseHandler.success(req, res, payload, (_a = payload.status) !== null && _a !== void 0 ? _a : 200);
};
//# sourceMappingURL=memberMerge.js.map