"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getOrganizations = void 0;
const axios_1 = __importDefault(require("axios"));
const types_1 = require("@crowd/types");
const getToken_1 = __importDefault(require("../nango/getToken"));
const errorHandler_1 = require("./errorHandler");
const getOrganizations = async (nangoId, logger) => {
    const config = {
        method: 'get',
        url: `https://api.linkedin.com/v2/organizationAcls`,
        params: {
            q: 'roleAssignee',
            projection: '(elements*(*,roleAssignee~(localizedFirstName,localizedLastName),organization~(id,localizedName,vanityName,logoV2(original~:playableStreams))))',
        },
        headers: {
            'X-Restli-Protocol-Version': '2.0.0',
        },
    };
    try {
        logger.debug({ nangoId }, 'Fetching organizations from LinkedIn');
        // Get an access token from Nango
        const accessToken = await (0, getToken_1.default)(nangoId, types_1.PlatformType.LINKEDIN, logger);
        config.params.oauth2_access_token = accessToken;
        const response = (await (0, axios_1.default)(config)).data;
        return response.elements.map((e) => {
            var _a, _b, _c;
            let profilePictureUrl;
            if (((_c = (_b = (_a = e['organization~'].logoV2) === null || _a === void 0 ? void 0 : _a['original~']) === null || _b === void 0 ? void 0 : _b.elements) === null || _c === void 0 ? void 0 : _c.length) > 0) {
                const pictures = e['organization~'].logoV2['original~'].elements;
                profilePictureUrl = pictures[pictures.length - 1].identifiers[0].identifier;
            }
            return {
                id: e['organization~'].id,
                name: e['organization~'].localizedName,
                organizationUrn: e.organization,
                vanityName: e['organization~'].vanityName,
                profilePictureUrl,
            };
        });
    }
    catch (err) {
        const newErr = (0, errorHandler_1.handleLinkedinError)(err, config, { nangoId }, logger);
        throw newErr;
    }
};
exports.getOrganizations = getOrganizations;
//# sourceMappingURL=getOrganizations.js.map