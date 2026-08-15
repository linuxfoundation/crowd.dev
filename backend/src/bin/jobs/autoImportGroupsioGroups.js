"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const logging_1 = require("@crowd/logging");
const getUserSubscriptions_1 = require("@/serverless/integrations/usecases/groupsio/getUserSubscriptions");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const log = (0, logging_1.getServiceChildLogger)('autoImportGroupsioGroupsCronJob');
const job = {
    name: 'Auto Import Groups IO Groups',
    // every 2 days
    cronTime: cron_time_generator_1.default.every(2).days(),
    onTrigger: async () => {
        log.info('Checking for new groups to auto import.');
        const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const integrations = await dbOptions.database.sequelize.query(`select id, settings from integrations 
                where  platform = 'groupsio' 
                and "deletedAt" is null        
        `);
        log.info(`Found ${integrations[0].length} integrations to check for auto imports.`);
        for (const integration of integrations[0]) {
            const settings = integration.settings;
            if (settings.autoImports) {
                const allGroups = await (0, getUserSubscriptions_1.getUserSubscriptions)(settings.token);
                log.info(`Found ${allGroups.length} available groups in users's account.`);
                const existingGroupIds = new Set(settings.groups.map((group) => group.id));
                for (const autoImport of settings.autoImports) {
                    if (autoImport.isAllowed) {
                        const newGroups = allGroups.filter((group) => !existingGroupIds.has(group.id) &&
                            group.group_name.startsWith(autoImport.mainGroup));
                        for (const newGroup of newGroups) {
                            log.info(`Adding new group ${newGroup.nice_group_name} to auto-import.`);
                            settings.groups.push({
                                id: newGroup.id,
                                name: newGroup.nice_group_name,
                                slug: newGroup.group_name,
                                groupAddedOn: new Date(),
                            });
                        }
                        if (newGroups.length > 0) {
                            log.info(`Added ${newGroups.length} new groups for auto-import in integration ${integration.id}`);
                        }
                        else {
                            log.info(`No new groups found for auto-import in integration ${integration.id}.`);
                        }
                    }
                }
                // Update the integration settings in the database
                await dbOptions.database.integration.update({ settings }, { where: { id: integration.id } });
            }
        }
    },
};
exports.default = job;
//# sourceMappingURL=autoImportGroupsioGroups.js.map