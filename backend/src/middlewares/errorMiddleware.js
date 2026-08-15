"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.safeWrap = void 0;
exports.errorMiddleware = errorMiddleware;
/* eslint-disable @typescript-eslint/no-unused-vars */
async function errorMiddleware(error, req, res, next) {
    await req.responseHandler.error(req, res, error);
}
const safeWrap = (handler) => async (req, res, next) => {
    try {
        await handler(req, res, next);
    }
    catch (err) {
        next(err);
    }
};
exports.safeWrap = safeWrap;
//# sourceMappingURL=errorMiddleware.js.map