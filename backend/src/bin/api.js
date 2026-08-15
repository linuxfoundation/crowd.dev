"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const api_1 = __importDefault(require("../api"));
const conf_1 = require("../conf");
const PORT = conf_1.API_CONFIG.port || 8080;
const log = (0, logging_1.getServiceLogger)();
api_1.default.listen(PORT, () => {
    log.info(`Listening on port ${PORT}`);
});
process.on('SIGTERM', async () => {
    log.warn('Detected SIGTERM signal, started exiting!');
    await new Promise((resolve) => {
        api_1.default.close((err) => {
            if (err) {
                log.error(err, 'Error while closing server!');
                resolve();
            }
            else {
                log.info('Server closed successfully!');
                resolve();
            }
        });
    });
    log.info('Exiting in 5 seconds...');
    await (0, common_1.timeout)(5000);
    process.exit(0);
});
//# sourceMappingURL=api.js.map