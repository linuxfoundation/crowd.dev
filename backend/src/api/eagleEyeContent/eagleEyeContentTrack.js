"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const eagleEyeContentService_1 = __importDefault(require("../../services/eagleEyeContentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.eagleEyeContentRead);
    const event = req.body.event;
    const params = req.body.params;
    switch (event) {
        case 'postClicked':
            eagleEyeContentService_1.default.trackPostClicked(req.body.url, req.body.platform, req);
            break;
        case 'generatedReply':
            (0, track_1.default)('Eagle Eye AI reply generated', {
                title: params.title,
                description: params.description,
                platform: params.platform,
                reply: params.reply,
                url: params.url,
            }, { ...req });
            break;
        case 'generatedReplyFeedback':
            (0, track_1.default)('Eagle Eye AI reply feedback', {
                type: params.type,
                title: params.title,
                description: params.description,
                platform: params.platform,
                reply: params.reply,
                url: params.url,
            }, { ...req });
            break;
        case 'generatedReplyCopied':
            (0, track_1.default)('Eagle Eye AI reply copied', {
                title: params.title,
                description: params.description,
                platform: params.platform,
                url: params.url,
                reply: params.reply,
            }, { ...req });
            break;
        default:
            throw new common_1.Error404('en', 'erros.eagleEye.invlaidEvent');
    }
    const out = {
        Success: true,
    };
    await req.responseHandler.success(req, res, out);
};
//# sourceMappingURL=eagleEyeContentTrack.js.map