"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_services_1 = require("@crowd/common_services");
const logging_1 = require("@crowd/logging");
const opensearch_1 = require("@crowd/opensearch");
const types_1 = require("@crowd/types");
const conf_1 = require("@/conf");
const queueService_1 = require("@/serverless/utils/queueService");
const apiClients_1 = require("../utils/apiClients");
class SearchSyncService extends logging_1.LoggerBase {
    constructor(options, mode = types_1.SyncMode.ASYNCHRONOUS) {
        super(options.log);
        this.options = options;
        this.mode = mode;
    }
    async getSearchSyncClient() {
        // tests can always use the async emitter
        if (conf_1.IS_TEST_ENV) {
            return (0, queueService_1.getSearchSyncWorkerEmitter)();
        }
        if (this.mode === types_1.SyncMode.SYNCHRONOUS) {
            return (0, apiClients_1.getSearchSyncApiClient)();
        }
        if (this.mode === types_1.SyncMode.ASYNCHRONOUS) {
            return (0, queueService_1.getSearchSyncWorkerEmitter)();
        }
        throw new Error(`Unknown mode ${this.mode} !`);
    }
    async logExecutionTime(process, name) {
        if (this.options.profileSql) {
            return (0, logging_1.logExecutionTimeV2)(process, this.options.log, name);
        }
        return process();
    }
    async triggerMemberSync(memberId) {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient) {
            await this.logExecutionTime(() => client.triggerMemberSync(memberId), `triggerMemberSync: member:${memberId}`);
        }
        else if (client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerMemberSync(memberId, false);
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerOrganizationMembersSync(organizationId) {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient) {
            await this.logExecutionTime(() => client.syncOrganizationMembers(organizationId), `triggerOrganizationMembersSync: organization:${organizationId}`);
        }
        else if (client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerOrganizationMembersSync(organizationId, false);
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerRemoveMember(memberId) {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient) {
            await this.logExecutionTime(() => client.triggerRemoveMember(memberId), `triggerRemoveMember: member:${memberId}`);
        }
        else if (client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerRemoveMember(memberId, false);
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerMemberCleanup() {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient || client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerMemberCleanup();
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerOrganizationSync(organizationId) {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient) {
            await this.logExecutionTime(() => client.triggerOrganizationSync(organizationId), `triggerOrganizationSync: organization:${organizationId}`);
        }
        else if (client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerOrganizationSync(organizationId, false);
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerRemoveOrganization(organizationId) {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient) {
            await this.logExecutionTime(() => client.triggerRemoveOrganization(organizationId), `triggerRemoveOrganization: organization:${organizationId}`);
        }
        else if (client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerRemoveOrganization(organizationId, false);
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
    async triggerOrganizationCleanup() {
        const client = await this.getSearchSyncClient();
        if (client instanceof opensearch_1.SearchSyncApiClient || client instanceof common_services_1.SearchSyncWorkerEmitter) {
            await client.triggerOrganizationCleanup();
        }
        else {
            throw new Error('Unexpected search client type!');
        }
    }
}
exports.default = SearchSyncService;
//# sourceMappingURL=searchSyncService.js.map