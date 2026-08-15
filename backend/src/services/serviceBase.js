"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ServiceBase = void 0;
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
class ServiceBase {
    constructor(options) {
        this.options = options;
    }
    destroy(id) {
        return this.destroyAll([id]);
    }
    async getTxRepositoryOptions() {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const options = { ...this.options };
        options.transaction = transaction;
        return options;
    }
}
exports.ServiceBase = ServiceBase;
//# sourceMappingURL=serviceBase.js.map