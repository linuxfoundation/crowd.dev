"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupGitlabWebhooks = setupGitlabWebhooks;
const axios_1 = __importDefault(require("axios"));
const conf_1 = require("@/conf");
const webhookBase = `${conf_1.API_CONFIG.url}/webhooks`;
const createWebhookUrl = (integrationId) => `${webhookBase}/gitlab/${integrationId}`;
async function setupGitlabWebhooks(accessToken, projectIds, integrationId) {
    const results = [];
    if (!conf_1.GITLAB_CONFIG.webhookToken) {
        throw new Error('Gitlab webhook token is not set');
    }
    for (const projectId of projectIds) {
        try {
            const response = await axios_1.default.post(`https://gitlab.com/api/v4/projects/${projectId}/hooks`, {
                token: conf_1.GITLAB_CONFIG.webhookToken,
                url: createWebhookUrl(integrationId),
                push_events: false,
                issues_events: true,
                confidential_issues_events: true,
                merge_requests_events: true,
                note_events: true, // This covers discussions
                job_events: false,
                pipeline_events: false,
                wiki_page_events: false,
                enable_ssl_verification: true,
            }, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            if (response.status === 201) {
                results.push({ projectId, success: true, hookId: response.data.id });
            }
            else {
                results.push({
                    projectId,
                    success: false,
                    error: `Unexpected response status: ${response.status}`,
                });
            }
        }
        catch (error) {
            results.push({ projectId, success: false, error: error.message });
        }
    }
    return results;
}
//# sourceMappingURL=setupWebhooks.js.map