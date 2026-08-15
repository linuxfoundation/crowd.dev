"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.checkAPIKey = void 0;
const axios_1 = __importDefault(require("axios"));
const checkAPIKey = async (apiKey) => {
    try {
        const response = await axios_1.default.get('https://dev.to/api/articles/me', {
            headers: {
                Accept: 'application/vnd.forem.api-v1+json',
                'api-key': apiKey || '',
            },
        });
        return response.status === 200;
    }
    catch (error) {
        return false;
    }
};
exports.checkAPIKey = checkAPIKey;
//# sourceMappingURL=checkAPIKey.js.map