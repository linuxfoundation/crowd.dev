"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const eagleEyeActionRepository_1 = __importDefault(require("../database/repositories/eagleEyeActionRepository"));
const eagleEyeContentRepository_1 = __importDefault(require("../database/repositories/eagleEyeContentRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const track_1 = __importDefault(require("../segment/track"));
class EagleEyeActionService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async create(data, contentId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        // find content
        const content = await eagleEyeContentRepository_1.default.findById(contentId, {
            ...this.options,
            transaction,
        });
        if (!content) {
            throw new common_1.Error404(this.options.language, 'errors.eagleEye.contentNotFound');
        }
        // Tracking here so we have access to url and platform
        (0, track_1.default)(`Eagle Eye post ${data.type === types_1.EagleEyeActionType.BOOKMARK ? 'bookmarked' : 'voted'}`, {
            type: data.type,
            url: content.url,
            platform: content.platform,
            action: 'create',
        }, { ...this.options });
        const existingUserActions = content.actions.filter((a) => a.actionById === this.options.currentUser.id);
        const existingUserActionTypes = existingUserActions.map((a) => a.type);
        try {
            // check if already bookmarked - if yes ignore the new action and return existing
            if (data.type === types_1.EagleEyeActionType.BOOKMARK &&
                existingUserActionTypes.includes(types_1.EagleEyeActionType.BOOKMARK)) {
                return existingUserActions.find((a) => a.type === types_1.EagleEyeActionType.BOOKMARK);
            }
            // thumbs up and down should be mutually exclusive
            if (data.type === types_1.EagleEyeActionType.THUMBS_DOWN &&
                existingUserActionTypes.includes(types_1.EagleEyeActionType.THUMBS_UP)) {
                await eagleEyeActionRepository_1.default.removeActionFromContent(types_1.EagleEyeActionType.THUMBS_UP, contentId, {
                    ...this.options,
                    transaction,
                });
            }
            else if (data.type === types_1.EagleEyeActionType.THUMBS_UP &&
                existingUserActionTypes.includes(types_1.EagleEyeActionType.THUMBS_DOWN)) {
                await eagleEyeActionRepository_1.default.removeActionFromContent(types_1.EagleEyeActionType.THUMBS_DOWN, contentId, {
                    ...this.options,
                    transaction,
                });
            }
            // add new action
            const record = await eagleEyeActionRepository_1.default.createActionForContent(data, contentId, {
                ...this.options,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'EagleEyeContent');
            throw error;
        }
    }
    async destroy(id) {
        const action = await eagleEyeActionRepository_1.default.findById(id, this.options);
        const contentId = action.contentId;
        await eagleEyeActionRepository_1.default.destroy(id, this.options);
        // find content
        const content = await eagleEyeContentRepository_1.default.findById(contentId, this.options);
        // if content no longer has any actions attached to it, also delete the content
        if (content.actions.length === 0) {
            await eagleEyeContentRepository_1.default.destroy(contentId, this.options);
        }
        // Tracking here so we have access to url and platform
        (0, track_1.default)(`Eagle Eye post ${action.type === types_1.EagleEyeActionType.BOOKMARK ? 'bookmarked' : 'voted'}`, {
            type: action.type,
            url: content.url,
            platform: content.platform,
            action: 'destroy',
        }, { ...this.options });
    }
}
exports.default = EagleEyeActionService;
//# sourceMappingURL=eagleEyeActionService.js.map