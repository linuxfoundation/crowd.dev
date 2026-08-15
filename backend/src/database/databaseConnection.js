"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.databaseInit = databaseInit;
exports.databaseClose = databaseClose;
const models_1 = __importDefault(require("./models"));
let cached;
/**
 * Initializes the connection to the Database
 */
async function databaseInit(queryTimeoutMilliseconds = 60000, forceNewInstance = false, databaseHostnameOverride = null) {
    if (forceNewInstance) {
        return (0, models_1.default)(queryTimeoutMilliseconds, databaseHostnameOverride);
    }
    if (!cached) {
        cached = (0, models_1.default)(queryTimeoutMilliseconds, databaseHostnameOverride);
    }
    return cached;
}
async function databaseClose(database) {
    await database.sequelize.close();
}
//# sourceMappingURL=databaseConnection.js.map