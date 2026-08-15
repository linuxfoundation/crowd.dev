"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = getStage;
const conf_1 = require("../../conf");
function getStage() {
    if (conf_1.IS_PROD_ENV)
        return 'prod';
    if (conf_1.IS_STAGING_ENV)
        return 'staging';
    return 'local';
}
//# sourceMappingURL=getStage.js.map