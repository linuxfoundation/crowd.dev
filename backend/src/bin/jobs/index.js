"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const autoImportGroupsioGroups_1 = __importDefault(require("./autoImportGroupsioGroups"));
const checkStuckIntegrationRuns_1 = __importDefault(require("./checkStuckIntegrationRuns"));
const cleanUp_1 = __importDefault(require("./cleanUp"));
const integrationTicks_1 = __importDefault(require("./integrationTicks"));
const refreshGithubRepoSettings_1 = __importDefault(require("./refreshGithubRepoSettings"));
const refreshGitlabToken_1 = __importDefault(require("./refreshGitlabToken"));
const refreshGroupsioToken_1 = __importDefault(require("./refreshGroupsioToken"));
const jobs = [
    integrationTicks_1.default,
    cleanUp_1.default,
    checkStuckIntegrationRuns_1.default,
    refreshGroupsioToken_1.default,
    refreshGitlabToken_1.default,
    refreshGithubRepoSettings_1.default,
    autoImportGroupsioGroups_1.default,
];
exports.default = jobs;
//# sourceMappingURL=index.js.map