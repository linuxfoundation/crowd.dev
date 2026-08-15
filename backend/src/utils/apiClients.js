"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getSearchSyncApiClient = void 0;
const opensearch_1 = require("@crowd/opensearch");
const conf_1 = require("../conf");
const config = conf_1.SEARCH_SYNC_API_CONFIG;
let searchSyncApiClient;
const getSearchSyncApiClient = async () => {
    if (!searchSyncApiClient) {
        searchSyncApiClient = new opensearch_1.SearchSyncApiClient(config);
    }
    return searchSyncApiClient;
};
exports.getSearchSyncApiClient = getSearchSyncApiClient;
//# sourceMappingURL=apiClients.js.map