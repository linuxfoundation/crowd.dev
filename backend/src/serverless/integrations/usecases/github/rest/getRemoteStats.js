"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getGitHubRemoteStats = void 0;
const axios_1 = __importDefault(require("axios"));
const logging_1 = require("@crowd/logging");
const commitsRegExp = /&page=(\d+)>; rel="last"/;
const log = (0, logging_1.getServiceChildLogger)('getRemoteStats');
const checkHeaders = (response, defaultValue = 0) => {
    const link = response.headers.link;
    if (link) {
        const matches = link.match(commitsRegExp);
        if (matches) {
            return parseInt(matches === null || matches === void 0 ? void 0 : matches[1], 10);
        }
        return defaultValue;
    }
    return defaultValue;
};
const getStatsForRepo = async (repoUrl, token) => {
    try {
        const [owner, repo] = repoUrl.split('/').slice(-2);
        const query = `
        query {
        repository(owner: "${owner}", name: "${repo}") {
          starCount: stargazers {
            totalCount
          }
          forkCountDirect: forks {
            totalCount
          }
          forkCount
          issuesOpened: issues(states: OPEN) {
            totalCount
          }
          issuesClosed: issues(states: CLOSED) {
            totalCount
          }
        }
    }`;
        const result = await axios_1.default.post('https://api.github.com/graphql', {
            query,
        }, {
            headers: {
                Authorization: `bearer ${token}`,
            },
        });
        const prsAll = await axios_1.default.get(`https://api.github.com/repos/${owner}/${repo}/pulls?state=all&per_page=1`, {
            headers: {
                Authorization: `bearer ${token}`,
            },
        });
        let out;
        try {
            out = {
                stars: result.data.data.repository.starCount.totalCount,
                forks: result.data.data.repository.forkCountDirect.totalCount,
                totalIssues: result.data.data.repository.issuesOpened.totalCount +
                    result.data.data.repository.issuesClosed.totalCount,
                totalPRs: checkHeaders(prsAll),
            };
        }
        catch (e) {
            log.error('Error getting stats for repo', e);
            throw e;
        }
        return out;
    }
    catch (error) {
        log.error(`Error fetching GitHub stats for repo ${repoUrl}:`, error);
        return {
            stars: 0,
            forks: 0,
            totalIssues: 0,
            totalPRs: 0,
        };
    }
};
const getGitHubRemoteStats = async (installToken, repos) => {
    const stats = {
        stars: 0,
        forks: 0,
        totalIssues: 0,
        totalPRs: 0,
    };
    const statsPromises = repos.map((repo) => getStatsForRepo(repo.url, installToken));
    const allRepoStats = await Promise.all(statsPromises);
    allRepoStats.forEach((repoStats) => {
        stats.stars += repoStats.stars;
        stats.forks += repoStats.forks;
        stats.totalIssues += repoStats.totalIssues;
        stats.totalPRs += repoStats.totalPRs;
    });
    return stats;
};
exports.getGitHubRemoteStats = getGitHubRemoteStats;
//# sourceMappingURL=getRemoteStats.js.map