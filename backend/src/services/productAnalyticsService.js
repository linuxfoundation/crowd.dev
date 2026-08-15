"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const productAnalytics_1 = require("@crowd/data-access-layer/src/productAnalytics");
const queryExecutor_1 = require("@crowd/data-access-layer/src/queryExecutor");
const logging_1 = require("@crowd/logging");
class ProductAnalyticsService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
        this.qx = new queryExecutor_1.PgPromiseQueryExecutor(this.options.productDb);
    }
    async createSession(data) {
        try {
            return await (0, productAnalytics_1.createSession)(this.qx, data);
        }
        catch (error) {
            throw new Error('Error during session create!');
        }
    }
    async updateSession(id, data) {
        try {
            return await (0, productAnalytics_1.updateSession)(this.qx, id, data);
        }
        catch (error) {
            throw new Error('Error during session update!');
        }
    }
    async createEvent(data) {
        try {
            return await (0, productAnalytics_1.createEvent)(this.qx, data);
        }
        catch (error) {
            throw new Error('Error during event create!');
        }
    }
}
exports.default = ProductAnalyticsService;
//# sourceMappingURL=productAnalyticsService.js.map