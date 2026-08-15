"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.removeGitlabWebhooks = removeGitlabWebhooks;
const axios_1 = __importDefault(require("axios"));
async function removeGitlabWebhooks(accessToken, projectIds, hookIds) {
    const results = [];
    for (const projectId of projectIds) {
        for (const hookId of hookIds) {
            try {
                // Delete the webhook
                const deleteResponse = await axios_1.default.delete(`https://gitlab.com/api/v4/projects/${projectId}/hooks/${hookId}`, {
                    headers: { Authorization: `Bearer ${accessToken}` },
                });
                if (deleteResponse.status === 204) {
                    results.push({ projectId, success: true });
                }
                else {
                    results.push({
                        projectId,
                        success: false,
                        error: `Unexpected response status: ${deleteResponse.status}`,
                    });
                }
            }
            catch (error) {
                results.push({ projectId, success: false, error: error.message });
            }
        }
    }
    return results;
}
//# sourceMappingURL=removeWebhooks.js.map