"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const data_issues_1 = require("@crowd/data-access-layer/src/data_issues");
const members_1 = require("@crowd/data-access-layer/src/members");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("@/conf");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class DataIssueService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async createDataIssue(data, entityId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const user = sequelizeRepository_1.default.getCurrentUser(this.options);
        let entityName;
        let reportedBy;
        if (data.entity === types_1.DataIssueEntity.ORGANIZATION) {
            const organization = await (0, data_access_layer_1.findOrgById)(qx, entityId, [
                data_access_layer_1.OrganizationField.ID,
                data_access_layer_1.OrganizationField.DISPLAY_NAME,
            ]);
            entityName = organization.displayName;
        }
        else if (data.entity === types_1.DataIssueEntity.PERSON) {
            const member = await (0, members_1.findMemberById)(qx, entityId, [members_1.MemberField.ID, members_1.MemberField.DISPLAY_NAME]);
            entityName = member.displayName;
        }
        else {
            throw new Error(`Unsupported data issue entity ${data.entity}!1`);
        }
        if (user.fullName) {
            reportedBy = `${user.fullName} - ${user.email}`;
        }
        else {
            reportedBy = `${user.email}`;
        }
        try {
            const result = await axios_1.default.post(`${conf_1.JIRA_ISSUE_REPORTER_CONFIG.apiUrl}/issue`, {
                fields: {
                    project: {
                        key: conf_1.JIRA_ISSUE_REPORTER_CONFIG.projectKey,
                    },
                    summary: `[Data Issue] ${entityName} (${data.entity[0].toUpperCase()}${data.entity
                        .slice(1)
                        .toLowerCase()})`,
                    description: {
                        version: 1,
                        type: 'doc',
                        content: [
                            (0, common_1.createHeading)('Entity'),
                            (0, common_1.createParagraph)(entityName),
                            (0, common_1.createHeading)('Profile'),
                            (0, common_1.createParagraph)(data.profileUrl, true),
                            (0, common_1.createHeading)('Data Issue'),
                            (0, common_1.createParagraph)(data.dataIssue),
                            (0, common_1.createHeading)('Description'),
                            (0, common_1.createParagraph)(data.description),
                            (0, common_1.createHeading)('Reported by'),
                            (0, common_1.createParagraph)(reportedBy),
                        ],
                    },
                    issuetype: {
                        name: 'Task',
                    },
                    labels: ['data-issue'],
                },
            }, {
                headers: {
                    Authorization: `Basic ${Buffer.from(`${conf_1.JIRA_ISSUE_REPORTER_CONFIG.apiTokenEmail}:${conf_1.JIRA_ISSUE_REPORTER_CONFIG.token}`).toString('base64')}`,
                },
            });
            const res = await (0, data_issues_1.createDataIssue)(qx, {
                ...data,
                issueUrl: result.data.self,
                createdById: user.id,
            });
            return res;
        }
        catch (error) {
            this.log.info(error);
            throw new Error('Error during session create!');
        }
    }
}
exports.default = DataIssueService;
//# sourceMappingURL=dataIssueService.js.map