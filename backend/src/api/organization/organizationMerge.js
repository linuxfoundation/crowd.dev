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
    const primaryOrgId = req.params.organizationId;
    const secondaryOrgId = req.body.organizationToMerge;
    const segmentId = req.body.segments[0];
    const requestPayload = {
        primary: primaryOrgId,
        secondary: secondaryOrgId,
        segmentId,
    };
    await new organizationService_1.default(req).mergeSync(primaryOrgId, secondaryOrgId, segmentId);
    (0, track_1.default)('Merge organizations', requestPayload, { ...req });
    await req.responseHandler.success(req, res, requestPayload);
};
//# sourceMappingURL=organizationMerge.js.map