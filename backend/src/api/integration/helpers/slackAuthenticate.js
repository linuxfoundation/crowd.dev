"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const passport_1 = __importDefault(require("passport"));
const sequelizeRepository_1 = __importDefault(require("../../../database/repositories/sequelizeRepository"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res, next) => {
    // Checking we have permision to edit the project
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const state = {
        tenantId: req.params.tenantId,
        segmentIds: sequelizeRepository_1.default.getSegmentIds(req),
        redirectUrl: req.query.redirectUrl,
        crowdToken: req.query.crowdToken,
    };
    const authenticator = passport_1.default.authenticate('slack', {
        scope: [
            'users:read',
            'users:read.email',
            'files:read',
            'channels:join',
            'channels:read',
            'channels:history',
            'reactions:read',
            'team:read',
        ],
        state: Buffer.from(JSON.stringify(state)).toString('base64'),
    });
    authenticator(req, res, next);
};
//# sourceMappingURL=slackAuthenticate.js.map