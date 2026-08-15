"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.databaseMiddleware = databaseMiddleware;
const logging_1 = require("@crowd/logging");
const databaseConnection_1 = require("../database/databaseConnection");
const log = (0, logging_1.getServiceLogger)();
async function databaseMiddleware(req, res, next) {
    try {
        const database = await (0, databaseConnection_1.databaseInit)();
        req.database = database;
    }
    catch (error) {
        log.error(error, 'Database connection error!');
    }
    finally {
        next();
    }
}
//# sourceMappingURL=databaseMiddleware.js.map