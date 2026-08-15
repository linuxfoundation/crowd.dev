"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const moment_1 = __importDefault(require("moment"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const log = (0, logging_1.getServiceChildLogger)('refreshgroupsioTokenCronJob');
const job = {
    name: 'Refresh Groups IO token',
    // every day
    cronTime: cron_time_generator_1.default.every(1).days(),
    onTrigger: async () => {
        log.info('Checking expiry for current groups io token.');
        const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const expiredGroupsIOTokens = await dbOptions.database.sequelize.query(`select id, settings from integrations 
                where  platform = 'groupsio' 
                and "deletedAt" is null        
                and DATE_PART('day', to_date( settings ->> 'tokenExpiry', 'YYYY-MM-DD') - now() ) < 2`);
        for (const integration of expiredGroupsIOTokens[0]) {
            const thisSetting = integration.settings;
            thisSetting.tokenError = '';
            log.info('Refreshing token for groups: ', thisSetting.groups);
            try {
                const decryptedPassword = (0, common_1.decryptData)(thisSetting.password);
                const config = {
                    method: 'post',
                    url: 'https://groups.io/api/v1/login',
                    params: {
                        email: thisSetting.email,
                        password: decryptedPassword,
                    },
                    headers: {
                        'Content-Type': 'application/json',
                    },
                };
                const response = await (0, axios_1.default)(config);
                // we need to get cookie from the response  and it's expiry
                const cookie = response.headers['set-cookie'][0].split(';')[0];
                const cookieExpiryString = response.headers['set-cookie'][0]
                    .split(';')[3]
                    .split('=')[1];
                const cookieExpiry = (0, moment_1.default)(cookieExpiryString).format('YYYY-MM-DD HH:mm:ss.sss Z');
                thisSetting.token = cookie;
                thisSetting.tokenExpiry = cookieExpiry;
            }
            catch (err) {
                thisSetting.tokenError = err.message;
                log.error(err.message);
            }
            finally {
                await dbOptions.database.integration.update({ settings: thisSetting }, { where: { id: integration.id } });
            }
        }
    },
};
exports.default = job;
//# sourceMappingURL=refreshGroupsioToken.js.map