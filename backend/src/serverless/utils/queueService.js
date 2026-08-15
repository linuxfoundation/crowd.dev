"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDataSinkWorkerEmitter = exports.getSearchSyncWorkerEmitter = exports.getIntegrationStreamWorkerEmitter = exports.getIntegrationRunWorkerEmitter = exports.QUEUE_CLIENT = void 0;
const common_services_1 = require("@crowd/common_services");
const logging_1 = require("@crowd/logging");
const queue_1 = require("@crowd/queue");
const conf_1 = require("../../conf");
const log = (0, logging_1.getServiceChildLogger)('service.queue');
let queueClient;
const QUEUE_CLIENT = () => {
    if (queueClient)
        return queueClient;
    // TODO: will be bound to an environment variable
    queueClient = queue_1.QueueFactory.createQueueService(conf_1.QUEUE_CONFIG);
    return queueClient;
};
exports.QUEUE_CLIENT = QUEUE_CLIENT;
let runWorkerEmitter;
const getIntegrationRunWorkerEmitter = async () => {
    if (runWorkerEmitter)
        return runWorkerEmitter;
    runWorkerEmitter = new common_services_1.IntegrationRunWorkerEmitter((0, exports.QUEUE_CLIENT)(), log);
    await runWorkerEmitter.init();
    return runWorkerEmitter;
};
exports.getIntegrationRunWorkerEmitter = getIntegrationRunWorkerEmitter;
let streamWorkerEmitter;
const getIntegrationStreamWorkerEmitter = async () => {
    if (streamWorkerEmitter)
        return streamWorkerEmitter;
    streamWorkerEmitter = new common_services_1.IntegrationStreamWorkerEmitter((0, exports.QUEUE_CLIENT)(), log);
    await streamWorkerEmitter.init();
    return streamWorkerEmitter;
};
exports.getIntegrationStreamWorkerEmitter = getIntegrationStreamWorkerEmitter;
let searchSyncWorkerEmitter;
const getSearchSyncWorkerEmitter = async () => {
    if (searchSyncWorkerEmitter)
        return searchSyncWorkerEmitter;
    searchSyncWorkerEmitter = new common_services_1.SearchSyncWorkerEmitter((0, exports.QUEUE_CLIENT)(), log);
    await searchSyncWorkerEmitter.init();
    return searchSyncWorkerEmitter;
};
exports.getSearchSyncWorkerEmitter = getSearchSyncWorkerEmitter;
let dataSinkWorkerEmitter;
const getDataSinkWorkerEmitter = async () => {
    if (dataSinkWorkerEmitter)
        return dataSinkWorkerEmitter;
    dataSinkWorkerEmitter = new common_services_1.DataSinkWorkerEmitter((0, exports.QUEUE_CLIENT)(), log);
    await dataSinkWorkerEmitter.init();
    return dataSinkWorkerEmitter;
};
exports.getDataSinkWorkerEmitter = getDataSinkWorkerEmitter;
//# sourceMappingURL=queueService.js.map