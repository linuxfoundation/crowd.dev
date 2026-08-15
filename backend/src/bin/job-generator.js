"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const cron_1 = require("cron");
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const sequelize_1 = require("sequelize");
const logging_1 = require("@crowd/logging");
const redis_1 = require("@crowd/redis");
const databaseConnection_1 = require("@/database/databaseConnection");
const conf_1 = require("../conf");
const jobs_1 = __importDefault(require("./jobs"));
const log = (0, logging_1.getServiceLogger)();
for (const job of jobs_1.default) {
    const cronJob = new cron_1.CronJob(job.cronTime, async () => {
        log.info({ job: job.name }, 'Triggering job.');
        try {
            await job.onTrigger(log);
        }
        catch (err) {
            log.error(err, { job: job.name }, 'Error while executing a job!');
        }
    }, null, true, 'Europe/Berlin');
    if (cronJob.running) {
        log.info({ job: job.name }, 'Scheduled a job.');
    }
}
const liveFilePath = path_1.default.join(__dirname, 'tmp/job-generator-live.tmp');
const readyFilePath = path_1.default.join(__dirname, 'tmp/job-generator-ready.tmp');
let seq;
let redis;
const initRedisSeq = async () => {
    if (!seq) {
        seq = (await (0, databaseConnection_1.databaseInit)()).sequelize;
    }
    if (!redis) {
        redis = await (0, redis_1.getRedisClient)(conf_1.REDIS_CONFIG, true);
    }
};
setInterval(async () => {
    try {
        await initRedisSeq();
        log.debug('Checking liveness and readiness for job generator.');
        const [redisPingRes, dbPingRes] = await Promise.all([
            // ping redis,
            redis.ping().then((res) => res === 'PONG'),
            // ping database
            seq.query('select 1', { type: sequelize_1.QueryTypes.SELECT }).then((rows) => rows.length === 1),
        ]);
        if (redisPingRes && dbPingRes) {
            await Promise.all([
                fs_1.default.promises.open(liveFilePath, 'a').then((file) => file.close()),
                fs_1.default.promises.open(readyFilePath, 'a').then((file) => file.close()),
            ]);
        }
    }
    catch (err) {
        log.error(`Error checking liveness and readiness for job generator: ${err}`);
    }
}, 5000);
//# sourceMappingURL=job-generator.js.map