"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.responseHandlerMiddleware = responseHandlerMiddleware;
const apiResponseHandler_1 = __importDefault(require("../api/apiResponseHandler"));
async function responseHandlerMiddleware(req, res, next) {
    req.responseHandler = new apiResponseHandler_1.default(req);
    next();
}
//# sourceMappingURL=responseHandlerMiddleware.js.map