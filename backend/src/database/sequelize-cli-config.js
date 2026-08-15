const IS_TEST_ENV = process.env.NODE_ENV === 'test';
const IS_STAGING_ENV = process.env.NODE_ENV === 'staging';
const IS_PROD_ENV = process.env.NODE_ENV === 'production';
const dbEnvVars = {
    username: process.env.CROWD_DB_USERNAME,
    password: process.env.CROWD_DB_PASSWORD,
    database: process.env.CROWD_DB_DATABASE,
    host: process.env.CROWD_DB_WRITE_HOST,
    port: process.env.CROWD_DB_PORT,
    dialect: 'postgres',
};
let currentEnvironmentVariables = {};
if (IS_TEST_ENV) {
    currentEnvironmentVariables = {
        test: { ...dbEnvVars },
    };
}
else if (IS_STAGING_ENV) {
    currentEnvironmentVariables = {
        staging: { ...dbEnvVars },
    };
}
else if (IS_PROD_ENV) {
    currentEnvironmentVariables = {
        production: { ...dbEnvVars },
    };
}
else {
    currentEnvironmentVariables = {
        development: { ...dbEnvVars },
    };
}
module.exports = currentEnvironmentVariables;
//# sourceMappingURL=sequelize-cli-config.js.map