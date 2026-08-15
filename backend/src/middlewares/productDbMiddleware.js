"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.productDatabaseMiddleware = productDatabaseMiddleware;
function productDatabaseMiddleware(db) {
    return async (req, res, next) => {
        req.productDb = db;
        next();
    };
}
//# sourceMappingURL=productDbMiddleware.js.map