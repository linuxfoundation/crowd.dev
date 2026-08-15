"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const logging_1 = require("@crowd/logging");
const databaseConnection_1 = require("../../database/databaseConnection");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const tenantUserRepository_1 = __importDefault(require("../../database/repositories/tenantUserRepository"));
const authService_1 = __importDefault(require("../../services/auth/authService"));
const logger = (0, logging_1.getServiceChildLogger)('websockets/namespaces');
class WebSocketNamespace {
    constructor(socketIoServer, namespace, authenticated) {
        this.namespace = namespace;
        this.authenticated = authenticated;
        this.log = logger.child({ namespace }, true);
        this.socketIoNamespace = socketIoServer.of(namespace);
        // database middleware
        this.socketIoNamespace.use(async (socket, next) => {
            try {
                socket.database = await (0, databaseConnection_1.databaseInit)();
                next();
            }
            catch (err) {
                this.log.error(err, 'Database connection error!');
                next(err);
            }
        });
        if (authenticated) {
            // auth middleware
            this.socketIoNamespace.use(async (socket, next) => {
                try {
                    if (socket.handshake.query && socket.handshake.query.token) {
                        socket.user = await authService_1.default.findByToken(socket.handshake.query.token, socket);
                        next();
                    }
                    else {
                        next(new Error('Authentication error'));
                    }
                }
                catch (err) {
                    this.log.error(err, 'WebSockets authentication error!');
                    next(err);
                }
            });
        }
        // handle connect
        this.socketIoNamespace.on('connection', (socket) => {
            if (authenticated) {
                this.log.info({ userId: socket.user.id }, 'Authenticated user connected!');
                // add to user room if we need to send a notification to this user only
                socket.join(`user-${socket.user.id}`);
            }
            else {
                this.log.info('User connected!');
            }
            socket.emit('connected');
            // handle disconnect
            socket.on('disconnect', () => {
                if (authenticated) {
                    this.log.info({ userId: socket.user.id }, 'Authenticated user disconnected!');
                }
                else {
                    this.log.info('User disconnected!');
                }
                ;
                socket.leaveAll();
            });
        });
        this.log.info('WebSockets namespace initialized!');
    }
    on(event, handler) {
        this.socketIoNamespace.on(event, handler);
    }
    emit(event, data) {
        this.socketIoNamespace.emit(event, data);
    }
    emitToRoom(room, event, data) {
        this.socketIoNamespace.to(room).emit(event, data);
    }
    emitToUserRoom(userId, event, data) {
        this.emitToRoom(`user-${userId}`, event, data);
    }
    async emitForTenant(tenantId, event, data) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const tenantUsers = await tenantUserRepository_1.default.findByTenant(tenantId, options);
        for (const user of tenantUsers) {
            this.emitToUserRoom(user.userId, event, data);
        }
    }
}
exports.default = WebSocketNamespace;
//# sourceMappingURL=namespace.js.map