"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const body_parser_1 = __importDefault(require("body-parser"));
const bunyan_middleware_1 = __importDefault(require("bunyan-middleware"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const helmet_1 = __importDefault(require("helmet"));
const http = __importStar(require("http"));
const os_1 = __importDefault(require("os"));
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const database_1 = require("@crowd/data-access-layer/src/database");
const logging_1 = require("@crowd/logging");
const opensearch_1 = require("@crowd/opensearch");
const redis_1 = require("@crowd/redis");
const telemetry_1 = require("@crowd/telemetry");
const temporal_1 = require("@crowd/temporal");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const productDbMiddleware_1 = require("@/middlewares/productDbMiddleware");
const conf_1 = require("../conf");
const authMiddleware_1 = require("../middlewares/authMiddleware");
const databaseMiddleware_1 = require("../middlewares/databaseMiddleware");
const errorMiddleware_1 = require("../middlewares/errorMiddleware");
const languageMiddleware_1 = require("../middlewares/languageMiddleware");
const opensearchMiddleware_1 = require("../middlewares/opensearchMiddleware");
const passportStrategyMiddleware_1 = require("../middlewares/passportStrategyMiddleware");
const redisMiddleware_1 = require("../middlewares/redisMiddleware");
const responseHandlerMiddleware_1 = require("../middlewares/responseHandlerMiddleware");
const segmentMiddleware_1 = require("../middlewares/segmentMiddleware");
const tenantMiddleware_1 = require("../middlewares/tenantMiddleware");
const apiRateLimiter_1 = require("./apiRateLimiter");
const authSocial_1 = __importDefault(require("./auth/authSocial"));
const public_1 = require("./public");
const websockets_1 = __importDefault(require("./websockets"));
const serviceLogger = (0, logging_1.getServiceLogger)();
const app = (0, express_1.default)();
const server = http.createServer(app);
setImmediate(async () => {
    const redis = await (0, redis_1.getRedisClient)(conf_1.REDIS_CONFIG, true);
    const opensearch = await (0, opensearch_1.getOpensearchClient)(conf_1.OPENSEARCH_CONFIG);
    const redisPubSubPair = await (0, redis_1.getRedisPubSubPair)(conf_1.REDIS_CONFIG);
    const userNamespace = await websockets_1.default.initialize(server);
    const pubSubReceiver = new redis_1.RedisPubSubReceiver('api-pubsub', redisPubSubPair.subClient, (err) => {
        serviceLogger.error(err, 'Error while listening to Redis Pub/Sub api-ws channel!');
        process.exit(1);
    }, serviceLogger);
    pubSubReceiver.subscribe('user', async (message) => {
        const data = message;
        if (data.tenantId) {
            await userNamespace.emitForTenant(data.tenantId, data.event, data.data);
        }
        else if (data.userId) {
            userNamespace.emitToUserRoom(data.userId, data.event, data.data);
        }
        else {
            serviceLogger.error({ type: data.type }, 'Received invalid websocket message!');
        }
    });
    app.use((0, telemetry_1.telemetryExpressMiddleware)('api.request.duration'));
    // Enables CORS
    app.use((0, cors_1.default)({ origin: true }));
    // Logging middleware
    app.use((0, bunyan_middleware_1.default)({
        headerName: 'x-request-id',
        propertyName: 'requestId',
        logName: `requestId`,
        logger: serviceLogger,
        level: 'trace',
    }));
    app.use((req, res, next) => {
        // @ts-ignore
        req.profileSql = req.headers['x-profile-sql'] === 'true';
        next();
    });
    app.use((req, res, next) => {
        res.setHeader('X-Hostname', os_1.default.hostname());
        next();
    });
    app.use((req, res, next) => {
        // this middleware fixes the issue with logging and datadog
        // explained in detail here: https://github.com/CrowdDotDev/crowd.dev/pull/2144
        // in short: the hostname field in logs breaks how datadog assigns k8s cluster info
        if (req.log.fields.hostname) {
            delete req.log.fields.hostname;
        }
        next();
    });
    // Initializes and adds the database middleware.
    app.use(databaseMiddleware_1.databaseMiddleware);
    // Bind redis to request
    app.use((0, redisMiddleware_1.redisMiddleware)(redis));
    // bind opensearch
    app.use((0, opensearchMiddleware_1.opensearchMiddleware)(opensearch));
    // temp check for production
    if (conf_1.TEMPORAL_CONFIG.serverUrl) {
        // Bind temporal to request
        const temporal = await (0, temporal_1.getTemporalClient)(conf_1.TEMPORAL_CONFIG);
        app.use((req, res, next) => {
            req.temporal = temporal;
            next();
        });
    }
    // Enables Helmet, a set of tools to
    // increase security.
    app.use((0, helmet_1.default)());
    const defaultRateLimiter = (0, apiRateLimiter_1.createRateLimiter)({
        max: 200,
        windowMs: 60 * 1000,
        skip: (req) => req.method === 'POST' && req.originalUrl.split('?')[0] === '/v1/members/resolve',
    });
    app.use(defaultRateLimiter);
    app.use(body_parser_1.default.json({
        limit: '5mb',
    }));
    app.use(body_parser_1.default.urlencoded({ limit: '5mb', extended: true }));
    app.use((err, req, res, next) => {
        if (err.type === 'entity.parse.failed') {
            next(new common_1.BadRequestError('Invalid JSON body'));
            return;
        }
        next(err);
    });
    app.use((req, res, next) => {
        // @ts-ignore
        req.userData = {
            ip: req.ip,
            userAgent: req.headers ? req.headers['user-agent'] : null,
        };
        next();
    });
    // Public API uses its own OAuth2 auth and error flow
    // Must be mounted before internal endpoints.
    app.use('/', (0, public_1.publicRouter)());
    // initialize passport strategies
    app.use(passportStrategyMiddleware_1.passportStrategyMiddleware);
    // Sets the current language of the request
    app.use(languageMiddleware_1.languageMiddleware);
    // adds our ApiResponseHandler instance to the req object as responseHandler
    app.use(responseHandlerMiddleware_1.responseHandlerMiddleware);
    // Configures the authentication middleware
    // to set the currentUser to the requests
    app.use(authMiddleware_1.authMiddleware);
    app.use('/health', async (req, res) => {
        try {
            const seq = sequelizeRepository_1.default.getSequelize(req);
            const [osPingRes, redisPingRes, dbPingRes, temporalPingRes] = await Promise.all([
                // ping opensearch
                opensearch.ping().then((res) => res.body),
                // ping redis,
                redis.ping().then((res) => res === 'PONG'),
                // ping database
                seq.query('select 1', { type: sequelize_1.QueryTypes.SELECT }).then((rows) => rows.length === 1),
                // ping temporal
                req.temporal
                    ? req.temporal.workflowService.getSystemInfo({}).then(() => true)
                    : Promise.resolve(true),
            ]);
            if (osPingRes && redisPingRes && dbPingRes && temporalPingRes) {
                res.sendStatus(200);
            }
            else {
                res.status(500).json({
                    opensearch: osPingRes,
                    redis: redisPingRes,
                    database: dbPingRes,
                    temporal: temporalPingRes,
                });
            }
        }
        catch (err) {
            res.status(500).json({ error: err.message, stack: err.stack });
        }
    });
    // Configure the Entity routes
    const routes = express_1.default.Router();
    // Enable Passport for Social Sign-in
    (0, authSocial_1.default)(app, routes);
    // Enable product db only if it's configured
    if (conf_1.PRODUCT_DB_CONFIG) {
        const productDbClient = await (0, database_1.getDbConnection)(conf_1.PRODUCT_DB_CONFIG);
        app.use((0, productDbMiddleware_1.productDatabaseMiddleware)(productDbClient));
        require('./product').default(routes);
    }
    require('./auth').default(routes);
    app.use(tenantMiddleware_1.tenantMiddleware);
    app.use(segmentMiddleware_1.segmentMiddleware);
    require('./auditLog').default(routes);
    require('./merge-suggestions').default(routes);
    require('./user').default(routes);
    require('./settings').default(routes);
    require('./member').default(routes);
    require('./activity').default(routes);
    require('./integration').default(routes);
    require('./eagleEyeContent').default(routes);
    require('./organization').default(routes);
    require('./slack').default(routes);
    require('./segment').default(routes);
    require('./systemStatus').default(routes);
    require('./customViews').default(routes);
    require('./dashboard').default(routes);
    require('./mergeAction').default(routes);
    require('./dataQuality').default(routes);
    require('./collections').default(routes);
    require('./categories').default(routes);
    await require('./nango').default(routes);
    app.use('/', routes);
    app.use(errorMiddleware_1.errorMiddleware);
});
exports.default = server;
//# sourceMappingURL=index.js.map