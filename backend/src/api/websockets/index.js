"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const socket_io_1 = require("socket.io");
const logging_1 = require("@crowd/logging");
const namespace_1 = __importDefault(require("./namespace"));
class WebSockets {
    constructor(server) {
        this.log = (0, logging_1.getServiceChildLogger)('websockets');
        this.socketIo = new socket_io_1.Server(server);
        this.log.info('Socket.IO server initialized!');
    }
    authenticatedNamespace(name) {
        return new namespace_1.default(this.socketIo, name, true);
    }
    static async initialize(server) {
        const websockets = new WebSockets(server);
        return websockets.authenticatedNamespace('/user');
    }
}
exports.default = WebSockets;
//# sourceMappingURL=index.js.map