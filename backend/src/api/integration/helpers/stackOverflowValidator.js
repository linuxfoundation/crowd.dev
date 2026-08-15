"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
const conf_1 = require("../../../conf");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const track_1 = __importDefault(require("../../../segment/track"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHasAny([
        permissions_1.default.values.integrationCreate,
        permissions_1.default.values.integrationEdit,
    ]);
    if (req.query.tag) {
        try {
            const result = await axios_1.default.get(`https://api.stackexchange.com/2.3/tags/${req.query.tag}/info`, {
                params: {
                    site: 'stackoverflow',
                    key: conf_1.STACKEXCHANGE_CONFIG.key,
                },
            });
            const data = result.data;
            if (result.status === 200 &&
                data.items &&
                data.items.length > 0 &&
                data.items[0].is_moderator_only === false &&
                data.items[0].count > 0) {
                (0, track_1.default)('Stack Overflow: tag input', {
                    tag: req.query.tag,
                    valid: true,
                }, { ...req });
                return req.responseHandler.success(req, res, data);
            }
        }
        catch (e) {
            (0, track_1.default)('Stack Overflow: tag input', {
                tag: req.query.tag,
                valid: false,
            }, { ...req });
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
    }
    (0, track_1.default)('Stack Overflow: tag input', {
        tag: req.query.subreddit,
        valid: false,
    }, { ...req });
    return req.responseHandler.error(req, res, new common_1.Error400(req.language));
};
//# sourceMappingURL=stackOverflowValidator.js.map