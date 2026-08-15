"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberAttributesEdit);
    const payload = await new memberService_1.default(req).addToNoMerge(req.params.memberId, req.body.memberToNotMerge);
    (0, track_1.default)('Ignore merge members', { memberId: req.params.memberId, memberToNotMergeId: req.body.memberToNotMerge }, { ...req });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberNotMerge.js.map