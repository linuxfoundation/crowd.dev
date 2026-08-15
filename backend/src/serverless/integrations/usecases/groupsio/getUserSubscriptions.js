"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getUserSubscriptions = void 0;
const axios_1 = __importDefault(require("axios"));
const logging_1 = require("@crowd/logging");
const log = (0, logging_1.getServiceChildLogger)('getGroupsIoUserSubscriptions');
const getUserSubscriptions = async (cookie) => {
    let allSubscriptions = [];
    let nextPageToken;
    do {
        const url = 'https://groups.io/api/v1/getsubs';
        const params = {
            limit: 100,
            ...(nextPageToken ? { page_token: nextPageToken } : {}),
        };
        try {
            const response = await axios_1.default.get(url, {
                params,
                headers: {
                    Cookie: cookie,
                },
            });
            allSubscriptions = [...allSubscriptions, ...response.data.data];
            nextPageToken = response.data.next_page_token;
        }
        catch (error) {
            log.error('Error fetching groups.io subscriptions:', error);
            throw error;
        }
    } while (nextPageToken);
    return allSubscriptions;
};
exports.getUserSubscriptions = getUserSubscriptions;
//# sourceMappingURL=getUserSubscriptions.js.map