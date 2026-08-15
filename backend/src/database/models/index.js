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
const pg_1 = __importDefault(require("pg"));
const sequelize_1 = __importStar(require("sequelize"));
const common_1 = require("@crowd/common");
/**
 * This module creates the Sequelize to the database and
 * exports all the models.
 */
const logging_1 = require("@crowd/logging");
const conf_1 = require("../../conf");
const configTypes = __importStar(require("../../conf/configTypes"));
const { highlight } = require('cli-highlight');
const log = (0, logging_1.getServiceChildLogger)('Database');
pg_1.default.usingSequelize = true;
function getCredentials() {
    if (conf_1.DB_CONFIG.username) {
        return {
            username: conf_1.DB_CONFIG.username,
            password: conf_1.DB_CONFIG.password,
        };
    }
    switch (conf_1.SERVICE) {
        case configTypes.ServiceType.API:
            return {
                username: conf_1.DB_CONFIG.apiUsername,
                password: conf_1.DB_CONFIG.apiPassword,
            };
        case configTypes.ServiceType.JOB_GENERATOR:
            return {
                username: conf_1.DB_CONFIG.jobGeneratorUsername,
                password: conf_1.DB_CONFIG.jobGeneratorPassword,
            };
        default:
            throw new Error('Incorrectly configured database connection settings!');
    }
}
async function models(queryTimeoutMilliseconds, databaseHostnameOverride = null) {
    log.info('Initializing sequelize database connection!');
    const database = {};
    let readHost = conf_1.SERVICE === configTypes.ServiceType.API ? conf_1.DB_CONFIG.readHost : conf_1.DB_CONFIG.writeHost;
    let writeHost = conf_1.DB_CONFIG.writeHost;
    if (databaseHostnameOverride) {
        readHost = databaseHostnameOverride;
        writeHost = databaseHostnameOverride;
    }
    const credentials = getCredentials();
    const sequelize = new sequelize_1.default(conf_1.DB_CONFIG.database, credentials.username, credentials.password, {
        dialect: conf_1.DB_CONFIG.dialect,
        dialectOptions: {
            application_name: conf_1.SERVICE ? `${conf_1.SERVICE}-seq` : 'unknown-app-seq',
            connectionTimeoutMillis: 15000,
            query_timeout: queryTimeoutMilliseconds,
            idle_in_transaction_session_timeout: 20000,
            ssl: common_1.IS_CLOUD_ENV ? { rejectUnauthorized: false } : false,
        },
        port: conf_1.DB_CONFIG.port,
        replication: {
            read: [
                {
                    host: readHost,
                },
            ],
            write: { host: writeHost },
        },
        pool: {
            max: conf_1.SERVICE === configTypes.ServiceType.API ? 20 : 10,
            min: 1,
            acquire: 50000,
            idle: 10000,
        },
        logging: conf_1.DB_CONFIG.logging
            ? (dbLog) => log.info(highlight(dbLog, {
                language: 'sql',
                ignoreIllegals: true,
            }), 'DB LOG')
            : false,
    });
    // if (profileQueries) {
    //   const oldQuery = sequelize.query
    //   sequelize.query = async (query, options) => {
    //     const { replacements } = options || {}
    //     const result = await logExecutionTimeV2(
    //       () => oldQuery.apply(sequelize, [query, options]),
    //       log,
    //       `DB Query:\n${query}\n${replacements ? `Params: ${JSON.stringify(replacements)}` : ''}`,
    //     )
    //     return result
    //   }
    // }
    const modelClasses = [
        require('./member').default,
        require('./memberIdentity').default,
        require('./file').default,
        require('./integration').default,
        require('./settings').default,
        require('./tenant').default,
        require('./tenantUser').default,
        require('./user').default,
        require('./eagleEyeContent').default,
        require('./eagleEyeAction').default,
        require('./organization').default,
        require('./memberAttributeSettings').default,
        require('./segment').default,
        require('./customView').default,
        require('./customViewOrder').default,
    ];
    for (const notInitmodel of modelClasses) {
        const model = notInitmodel(sequelize, sequelize_1.DataTypes);
        database[model.name] = model;
    }
    Object.keys(database).forEach((modelName) => {
        if (database[modelName].associate) {
            database[modelName].associate(database);
        }
    });
    database.sequelize = sequelize;
    database.Sequelize = sequelize_1.default;
    await sequelize.authenticate();
    log.info('Sequelize database connection has been established successfully!');
    return database;
}
exports.default = models;
//# sourceMappingURL=index.js.map