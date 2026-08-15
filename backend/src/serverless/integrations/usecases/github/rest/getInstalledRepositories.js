"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getInstalledRepositories = void 0;
const axios_1 = __importDefault(require("axios"));
const logging_1 = require("@crowd/logging");
const log = (0, logging_1.getServiceChildLogger)('getInstalledRepositories');
/**
 * Normalizes forkedFrom URL for special cases.
 */
const normalizeForkedFrom = (forkedFrom) => {
    if (!forkedFrom) {
        return null;
    }
    // Special case: Linux kernel on GitHub should map to the official kernel.org git repository
    // because that's the one onboarded in our system, not the GitHub mirror.
    if (forkedFrom.endsWith('github.com/torvalds/linux')) {
        return 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux';
    }
    return forkedFrom;
};
const getRepositoriesFromGH = async (page, installToken) => {
    const REPOS_PER_PAGE = 100;
    const requestConfig = {
        method: 'get',
        url: `https://api.github.com/installation/repositories?page=${page}&per_page=${REPOS_PER_PAGE}`,
        headers: {
            Authorization: `Bearer ${installToken}`,
        },
    };
    const response = await (0, axios_1.default)(requestConfig);
    return response.data;
};
const parseRepos = (repositories) => {
    var _a;
    const repos = [];
    for (const repo of repositories) {
        repos.push({
            url: repo.html_url,
            owner: repo.owner.login,
            createdAt: repo.created_at,
            name: repo.name,
            fork: repo.fork,
            private: repo.private,
            cloneUrl: repo.clone_url,
            forkedFrom: normalizeForkedFrom(((_a = repo.parent) === null || _a === void 0 ? void 0 : _a.html_url) || null),
        });
    }
    return repos;
};
const getInstalledRepositories = async (installToken) => {
    try {
        let page = 1;
        let hasMorePages = true;
        const repos = [];
        while (hasMorePages) {
            const data = await getRepositoriesFromGH(page, installToken);
            if (data.repositories) {
                repos.push(...parseRepos(data.repositories));
            }
            hasMorePages = data.total_count && data.total_count > 0 && data.total_count > repos.length;
            page += 1;
        }
        return repos.filter((repo) => !repo.private && !repo.fork);
    }
    catch (err) {
        log.error(err, 'Error fetching installed repositories!');
        throw err;
    }
};
exports.getInstalledRepositories = getInstalledRepositories;
//# sourceMappingURL=getInstalledRepositories.js.map