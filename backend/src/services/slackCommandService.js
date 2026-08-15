"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-case-declarations */
const moment_1 = __importDefault(require("moment"));
const slack_block_builder_1 = require("slack-block-builder");
const common_1 = require("@crowd/common");
const conf_1 = require("../conf");
const tenantRepository_1 = __importDefault(require("../database/repositories/tenantRepository"));
const slackTypes_1 = require("../types/slackTypes");
class SlackCommandService {
    constructor(options) {
        this.options = options;
        this.commands = [
            {
                command: slackTypes_1.SlackCommand.HELP,
                description: 'Prints help message',
                executor: this.printHelp.bind(this),
            },
            {
                command: slackTypes_1.SlackCommand.PRINT_TENANT,
                shortVersion: 'pt',
                description: 'Prints tenant data as JSON string',
                parameters: [
                    {
                        name: 'tenantId',
                        short: 't',
                        required: true,
                        description: 'Tenant ID',
                        type: slackTypes_1.SlackCommandParameterType.UUID,
                    },
                ],
                executor: this.printTenant.bind(this),
            },
            {
                command: slackTypes_1.SlackCommand.SET_TENANT_PLAN,
                shortVersion: 'stp',
                description: 'Sets tenant plan to Growth or Essential',
                parameters: [
                    {
                        name: 'tenantId',
                        short: 't',
                        required: true,
                        description: 'Tenant ID',
                        type: slackTypes_1.SlackCommandParameterType.UUID,
                    },
                    {
                        name: 'plan',
                        short: 'p',
                        required: true,
                        description: 'Plan to set',
                        type: slackTypes_1.SlackCommandParameterType.STRING,
                    },
                    {
                        name: 'trialEndsAt',
                        short: 'tea',
                        required: false,
                        description: 'Trial end date in ISO string format - 2023-03-30',
                        type: slackTypes_1.SlackCommandParameterType.DATE,
                    },
                ],
                executor: this.setTenantPlan.bind(this),
            },
        ];
    }
    async setTenantPlan(params) {
        var _a;
        const plan = params.plan;
        const isTrial = params.trialEndsAt !== undefined;
        const trialEndsAt = params.trialEndsAt || null;
        const tenantId = params.tenantId;
        const tenant = await tenantRepository_1.default.findById(tenantId, this.options);
        const sections = [];
        if (!tenant) {
            sections.push((0, slack_block_builder_1.Section)({ text: `*Tenant with ID ${tenantId} not found!*` }));
        }
        else {
            const result = await tenantRepository_1.default.update(tenantId, {
                plan,
                isTrialPlan: isTrial,
                trialEndsAt,
            }, this.options, true);
            sections.push((0, slack_block_builder_1.Section)({
                text: `*Tenant ${tenant.name} (${tenantId}) plan changed to ${result.plan}, trial=${result.isTrialPlan}, trialEndsAt=${(_a = result.trialEndsAt) !== null && _a !== void 0 ? _a : '<unset>'}!*`,
            }));
        }
        return (0, slack_block_builder_1.Message)()
            .blocks(...sections)
            .buildToObject();
    }
    async printHelp() {
        const sections = [];
        sections.push((0, slack_block_builder_1.Section)({ text: `*Available commands:*` }));
        sections.push((0, slack_block_builder_1.Section)({ text: '_Parameters with * are required..._' }));
        sections.push((0, slack_block_builder_1.Divider)());
        for (const command of this.commands) {
            let commandString = `*${command.command}${command.shortVersion ? `/${command.shortVersion}` : ''}*: _${command.description}_`;
            if (command.parameters && command.parameters.length > 0) {
                for (const param of command.parameters) {
                    commandString += `\n\t${SlackCommandService.paramToSlackString(param)}`;
                }
            }
            sections.push((0, slack_block_builder_1.Section)({ text: commandString }));
            sections.push((0, slack_block_builder_1.Divider)());
        }
        return (0, slack_block_builder_1.Message)()
            .blocks(...sections)
            .buildToObject();
    }
    async printTenant(params) {
        const tenantId = params.tenantId;
        const tenant = await tenantRepository_1.default.findById(tenantId, this.options);
        const sections = [];
        if (!tenant) {
            sections.push((0, slack_block_builder_1.Section)({ text: `*Tenant with ID ${tenantId} not found!*` }));
        }
        sections.push((0, slack_block_builder_1.Section)({ text: `*Tenant ${tenant.name} (${tenantId}):*` }), (0, slack_block_builder_1.Divider)(), (0, slack_block_builder_1.Section)({ text: `\`\`\`${JSON.stringify(tenant, null, 2)}\`\`\`` }));
        return (0, slack_block_builder_1.Message)()
            .blocks(...sections)
            .buildToObject();
    }
    async processCommand(command, params, username, userId) {
        if (command === '/crowd-test' && !conf_1.IS_DEV_ENV) {
            this.options.log.error('Received /crowd-test command in non-dev environment! Ignoring!');
            return SlackCommandService.buildSlackMessage((0, slack_block_builder_1.Section)().text('Received /crowd-test command in non-dev environment! Ignoring!'));
        }
        if (command === '/crowd-staging' && !conf_1.IS_STAGING_ENV) {
            this.options.log.error('Received /crowd-staging command in non-staging environment! Ignoring!');
            return SlackCommandService.buildSlackMessage((0, slack_block_builder_1.Section)().text('Received /crowd-staging command in non-staging environment! Ignoring!'));
        }
        if (command === '/crowd' && !conf_1.IS_PROD_ENV) {
            this.options.log.error('Received /crowd command in non-prod environment! Ignoring!');
            return SlackCommandService.buildSlackMessage((0, slack_block_builder_1.Section)().text('Received /crowd command in non-prod environment! Ignoring!'));
        }
        this.options.log.info({ command, params, username, userId }, 'Received slack command!');
        const splitParams = params
            .split(' ')
            .map((s) => s.trim())
            .filter((s) => s.length > 0);
        const serviceCommand = splitParams[0];
        const commandDefinition = this.commands.find((c) => c.command === serviceCommand || c.shortVersion === serviceCommand);
        if (!commandDefinition) {
            return SlackCommandService.buildSlackMessage((0, slack_block_builder_1.Section)().text(`Command not found! Try \`/crowd help\`.`));
        }
        splitParams.shift();
        const parsedParams = SlackCommandService.parseParameters(commandDefinition, splitParams);
        if (parsedParams.error) {
            return parsedParams.error;
        }
        return commandDefinition.executor(parsedParams.params);
    }
    static parseParameters(commandDefinition, params) {
        const missingRequiredParameter = [];
        const invalidParameters = [];
        const result = {
            params: {},
        };
        for (const paramDef of commandDefinition.parameters || []) {
            const value = SlackCommandService.findParamValue(paramDef, params);
            if (value) {
                switch (paramDef.type) {
                    case slackTypes_1.SlackCommandParameterType.UUID:
                        if (!(0, common_1.validateUUID)(value)) {
                            invalidParameters.push(paramDef);
                        }
                        else {
                            result.params[paramDef.name] = value;
                        }
                        break;
                    case slackTypes_1.SlackCommandParameterType.BOOLEAN:
                        if (value !== 'true' && value !== 'false') {
                            invalidParameters.push(paramDef);
                        }
                        else {
                            result.params[paramDef.name] = value === 'true';
                        }
                        break;
                    case slackTypes_1.SlackCommandParameterType.NUMBER:
                        const numericValue = parseInt(value, 10);
                        if (Number.isNaN(numericValue)) {
                            invalidParameters.push(paramDef);
                        }
                        else {
                            result.params[paramDef.name] = numericValue;
                        }
                        break;
                    case slackTypes_1.SlackCommandParameterType.DATE:
                        const dateValue = (0, moment_1.default)(value);
                        if (!dateValue.isValid()) {
                            invalidParameters.push(paramDef);
                        }
                        else {
                            result.params[paramDef.name] = dateValue.toDate();
                        }
                        break;
                    case slackTypes_1.SlackCommandParameterType.STRING:
                        result.params[paramDef.name] = value;
                        break;
                    default:
                        throw new Error(`Unknown parameter type: ${paramDef.type}`);
                }
                if (paramDef.allowedValues && paramDef.allowedValues.indexOf(value) === -1) {
                    invalidParameters.push(paramDef);
                }
            }
            else if (paramDef.required) {
                missingRequiredParameter.push(paramDef);
            }
        }
        const errorBlocks = [];
        if (missingRequiredParameter.length > 0) {
            errorBlocks.push((0, slack_block_builder_1.Section)({ text: '*Missing required parameters!*' }));
            errorBlocks.push((0, slack_block_builder_1.Divider)());
            for (const p of missingRequiredParameter) {
                errorBlocks.push((0, slack_block_builder_1.Section)({ text: SlackCommandService.paramToSlackString(p) }));
            }
        }
        if (invalidParameters.length > 0) {
            if (errorBlocks.length > 0) {
                errorBlocks.push((0, slack_block_builder_1.Divider)());
            }
            errorBlocks.push((0, slack_block_builder_1.Section)({ text: '*Invalid parameters!*' }));
            errorBlocks.push((0, slack_block_builder_1.Divider)());
            for (const p of invalidParameters) {
                errorBlocks.push((0, slack_block_builder_1.Section)({ text: SlackCommandService.paramToSlackString(p) }));
            }
        }
        if (errorBlocks.length > 0) {
            return {
                error: SlackCommandService.buildSlackMessage(...errorBlocks),
            };
        }
        return result;
    }
    static findParamValue(definition, params) {
        let next = false;
        for (const param of params) {
            if (next) {
                return param;
            }
            if (param === `--${definition.name}` || param === `-${definition.short}`) {
                if (definition.type === slackTypes_1.SlackCommandParameterType.BOOLEAN) {
                    return 'true';
                }
                next = true;
            }
        }
        return undefined;
    }
    static buildSlackMessage(...blocks) {
        return (0, slack_block_builder_1.Message)()
            .blocks(...blocks)
            .buildToObject();
    }
    static paramToSlackString(p) {
        return `${p.required ? '*' : ''} _--${p.name}${p.short ? `/-${p.short}` : ''}_ (${p.type}): ${p.description}${p.default ? `, default: ${p.default}` : ''}${p.allowedValues && p.allowedValues.length > 0
            ? `, allowed values: ${p.allowedValues.join(',')}`
            : ''}`;
    }
}
exports.default = SlackCommandService;
//# sourceMappingURL=slackCommandService.js.map