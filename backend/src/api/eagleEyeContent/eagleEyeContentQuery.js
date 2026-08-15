"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const eagleEyeContentService_1 = __importDefault(require("../../services/eagleEyeContentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.eagleEyeContentRead);
    const payload = await new eagleEyeContentService_1.default(req).query(req.body);
    if (((_a = req.body) === null || _a === void 0 ? void 0 : _a.filter) && Object.keys(req.body.filter).length > 0) {
        (0, track_1.default)('EagleEyeContent Advanced Filter', { ...req.body }, { ...req });
    }
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=eagleEyeContentQuery.js.map