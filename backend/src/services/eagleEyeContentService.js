"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const moment_1 = __importDefault(require("moment"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("../conf");
const eagleEyeContentRepository_1 = __importDefault(require("../database/repositories/eagleEyeContentRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const tenantUserRepository_1 = __importDefault(require("../database/repositories/tenantUserRepository"));
const track_1 = __importDefault(require("../segment/track"));
class EagleEyeContentService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    /**
     * Create an eagle eye shown content record.
     * @param data Data to a new EagleEyeContent record.
     * @param options Repository options.
     * @returns Created EagleEyeContent record.
     */
    async upsert(data) {
        if (!data.url) {
            throw new common_1.Error400(this.options.language, 'errors.eagleEye.urlRequiredWhenUpserting');
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            // find by url
            const existing = await eagleEyeContentRepository_1.default.findByUrl(data.url, {
                ...this.options,
                transaction,
            });
            let record;
            if (existing) {
                record = await eagleEyeContentRepository_1.default.update(existing.id, data, {
                    ...this.options,
                    transaction,
                });
            }
            else {
                record = await eagleEyeContentRepository_1.default.create(data, {
                    ...this.options,
                    transaction,
                });
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async findById(id) {
        return eagleEyeContentRepository_1.default.findById(id, this.options);
    }
    async query(data) {
        const advancedFilter = data.filter;
        const orderBy = data.orderBy;
        const limit = data.limit;
        const offset = data.offset;
        return eagleEyeContentRepository_1.default.findAndCountAll({ advancedFilter, orderBy, limit, offset }, this.options);
    }
    static trackPostClicked(url, platform, req, source = 'app') {
        (0, track_1.default)('Eagle Eye post clicked', {
            url,
            platform,
            source,
        }, { ...req });
    }
    static trackDigestEmailOpened(req) {
        (0, track_1.default)('Eagle Eye digest opened', {}, { ...req });
    }
    /**
     * Convert a relative string date to a Date. For example, 30 days ago -> 2020-01-01
     * @param date String date. Can be one of EagleEyePublishedDates
     * @returns The corresponding Date
     */
    static switchDate(date, offset = 0) {
        let dateMoment;
        switch (date) {
            case types_1.EagleEyePublishedDates.LAST_24_HOURS:
                dateMoment = (0, moment_1.default)().subtract(1, 'days');
                break;
            case types_1.EagleEyePublishedDates.LAST_7_DAYS:
                dateMoment = (0, moment_1.default)().subtract(7, 'days');
                break;
            case types_1.EagleEyePublishedDates.LAST_14_DAYS:
                dateMoment = (0, moment_1.default)().subtract(14, 'days');
                break;
            case types_1.EagleEyePublishedDates.LAST_30_DAYS:
                dateMoment = (0, moment_1.default)().subtract(30, 'days');
                break;
            case types_1.EagleEyePublishedDates.LAST_90_DAYS:
                dateMoment = (0, moment_1.default)().subtract(90, 'days');
                break;
            default:
                return null;
        }
        return dateMoment.subtract(offset, 'days').format('YYYY-MM-DD');
    }
    async search(email = false) {
        const eagleEyeSettings = (await tenantUserRepository_1.default.findByTenantAndUser(this.options.currentTenant.id, this.options.currentUser.id, this.options)).settings.eagleEye;
        if (!eagleEyeSettings.onboarded) {
            throw new common_1.Error400(this.options.language, 'errors.eagleEye.notOnboarded');
        }
        const feedSettings = email ? eagleEyeSettings.emailDigest.feed : eagleEyeSettings.feed;
        const keywords = feedSettings.keywords ? feedSettings.keywords.join(',') : '';
        const exactKeywords = feedSettings.exactKeywords ? feedSettings.exactKeywords.join(',') : '';
        const excludedKeywords = feedSettings.excludedKeywords
            ? feedSettings.excludedKeywords.join(',')
            : '';
        const afterDate = EagleEyeContentService.switchDate(feedSettings.publishedDate);
        const config = {
            method: 'get',
            maxBodyLength: Infinity,
            url: `${conf_1.EAGLE_EYE_CONFIG.url}`,
            params: {
                platforms: feedSettings.platforms.join(','),
                keywords,
                exact_keywords: exactKeywords,
                exclude_keywords: excludedKeywords,
                after_date: afterDate,
            },
            headers: {
                Authorization: `Bearer ${conf_1.EAGLE_EYE_CONFIG.apiKey}`,
            },
        };
        let response;
        try {
            response = await (0, axios_1.default)(config);
        }
        catch (error) {
            this.log.error('Error while fetching eagle eye content', error, config);
            return [];
        }
        const interacted = (await this.query({
            filter: {
                postedAt: { gt: EagleEyeContentService.switchDate(feedSettings.publishedDate, 90) },
            },
        })).rows;
        const interactedMap = {};
        for (const item of interacted) {
            interactedMap[item.url] = item;
        }
        const out = [];
        for (const item of response.data) {
            const post = {
                description: item.description,
                thumbnail: item.thumbnail,
                title: item.title,
            };
            out.push({
                url: item.url,
                postedAt: item.date,
                post,
                platform: item.platform,
                actions: interactedMap[item.url] ? interactedMap[item.url].actions : [],
            });
        }
        return out;
    }
    static async reply(title, description) {
        const config = {
            method: 'get',
            maxBodyLength: Infinity,
            url: `${conf_1.EAGLE_EYE_CONFIG.url}/reply`,
            params: {
                title,
                description,
            },
            headers: {
                Authorization: `Bearer ${conf_1.EAGLE_EYE_CONFIG.apiKey}`,
            },
        };
        const response = await (0, axios_1.default)(config);
        return {
            reply: response.data,
        };
    }
}
exports.default = EagleEyeContentService;
//# sourceMappingURL=eagleEyeContentService.js.map