"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fetchAllGitlabGroups = fetchAllGitlabGroups;
exports.fetchGitlabGroupProjects = fetchGitlabGroupProjects;
exports.fetchGitlabUserProjects = fetchGitlabUserProjects;
const axios_1 = __importDefault(require("axios"));
async function fetchAllGitlabGroups(accessToken) {
    const groups = [];
    let page = 1;
    let hasMorePages = true;
    while (hasMorePages) {
        const response = await axios_1.default.get('https://gitlab.com/api/v4/groups', {
            headers: { Authorization: `Bearer ${accessToken}` },
            params: { page, per_page: 100 },
        });
        groups.push(...response.data);
        hasMorePages = response.headers['x-next-page'] !== '';
        page++;
    }
    return groups.map((group) => ({
        id: group.id,
        name: group.name,
        path: group.path,
        avatarUrl: group.avatar_url,
    }));
}
async function fetchProjectsForGroup(accessToken, group) {
    const projects = [];
    let page = 1;
    let hasMorePages = true;
    while (hasMorePages) {
        const response = await axios_1.default.get(`https://gitlab.com/api/v4/groups/${group.id}/projects`, {
            headers: { Authorization: `Bearer ${accessToken}` },
            params: { page, per_page: 100, archived: false },
        });
        projects.push(...response.data);
        hasMorePages = response.headers['x-next-page'] !== '';
        page++;
    }
    return projects.map((project) => {
        var _a;
        return ({
            groupId: group.id,
            groupName: group.name,
            groupPath: group.path,
            id: project.id,
            name: project.name,
            path_with_namespace: project.path_with_namespace,
            enabled: false,
            forkedFrom: ((_a = project === null || project === void 0 ? void 0 : project.forked_from_project) === null || _a === void 0 ? void 0 : _a.web_url) || null,
        });
    });
}
async function fetchGitlabGroupProjects(accessToken, groups) {
    const CONCURRENCY = 10;
    const groupProjects = {};
    for (let i = 0; i < groups.length; i += CONCURRENCY) {
        const batch = groups.slice(i, i + CONCURRENCY);
        const results = await Promise.all(batch.map((group) => fetchProjectsForGroup(accessToken, group)));
        batch.forEach((group, idx) => {
            groupProjects[group.id] = results[idx];
        });
    }
    return groupProjects;
}
async function fetchGitlabUserProjects(accessToken, userId) {
    const projects = [];
    let page = 1;
    let hasMorePages = true;
    while (hasMorePages) {
        const response = await axios_1.default.get(`https://gitlab.com/api/v4/users/${userId}/projects`, {
            headers: { Authorization: `Bearer ${accessToken}` },
            params: { page, per_page: 100, archived: false },
        });
        projects.push(...response.data);
        hasMorePages = response.headers['x-next-page'] !== '';
        page++;
    }
    return projects.map((project) => {
        var _a;
        return ({
            id: project.id,
            name: project.name,
            path_with_namespace: project.path_with_namespace,
            enabled: false,
            forkedFrom: ((_a = project === null || project === void 0 ? void 0 : project.forked_from_project) === null || _a === void 0 ? void 0 : _a.web_url) || null,
        });
    });
}
//# sourceMappingURL=getProjects.js.map