"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPackagesQx = getPackagesQx;
const database_1 = require("@crowd/data-access-layer/src/database");
const queryExecutor_1 = require("@crowd/data-access-layer/src/queryExecutor");
const conf_1 = require("@/conf");
let _init;
function getPackagesQx() {
    if (!_init) {
        if (!conf_1.PACKAGES_DB_CONFIG) {
            throw new Error('Packages DB is not configured — set CROWD_PACKAGES_DB_* environment variables');
        }
        _init = (0, database_1.getDbConnection)(conf_1.PACKAGES_DB_CONFIG)
            .then(queryExecutor_1.pgpQx)
            .catch((err) => {
            _init = undefined;
            throw err;
        });
    }
    return _init;
}
//# sourceMappingURL=packagesDb.js.map