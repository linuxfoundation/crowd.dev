"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const organizationService_1 = __importDefault(require("@/services/organizationService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationEdit);
    await new organizationService_1.default(req).addToNoMerge(req.params.organizationId, req.body.organizationToNotMerge);
    (0, track_1.default)('Ignore merge organizations', {
        organizationId: req.params.organizationId,
        organizationToNotMergeId: req.body.organizationToNotMerge,
    }, { ...req });
    await req.responseHandler.success(req, res, { status: 200 });
};
//# sourceMappingURL=organizationNotMerge.js.map