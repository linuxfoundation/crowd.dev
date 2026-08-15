"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPackagesTemporalClient = getPackagesTemporalClient;
const common_1 = require("@crowd/common");
const temporal_1 = require("@crowd/temporal");
const conf_1 = require("@/conf");
let _init;
// Separate connection from the API's default req.temporal client — packages_worker
// (npm/maven/pypi/osv/security-contacts/...) polls task queues in its own Temporal
// namespace (CROWD_PACKAGES_TEMPORAL_NAMESPACE), not the API's default namespace.
function getPackagesTemporalClient() {
    if (!_init) {
        if (!(conf_1.PACKAGES_TEMPORAL_CONFIG === null || conf_1.PACKAGES_TEMPORAL_CONFIG === void 0 ? void 0 : conf_1.PACKAGES_TEMPORAL_CONFIG.serverUrl)) {
            throw new Error('Packages Temporal is not configured — set CROWD_PACKAGES_TEMPORAL_NAMESPACE');
        }
        const cfg = conf_1.PACKAGES_TEMPORAL_CONFIG;
        _init = temporal_1.Connection.connect({
            address: cfg.serverUrl,
            tls: cfg.certificate && cfg.privateKey
                ? {
                    clientCertPair: {
                        crt: Buffer.from(cfg.certificate, 'base64'),
                        key: Buffer.from(cfg.privateKey, 'base64'),
                    },
                }
                : undefined,
        })
            .then(async (connection) => new temporal_1.Client({
            connection,
            namespace: cfg.namespace,
            identity: common_1.SERVICE,
            dataConverter: common_1.IS_DEV_ENV ? undefined : await (0, temporal_1.getDataConverter)(),
        }))
            .catch((err) => {
            _init = undefined;
            throw err;
        });
    }
    return _init;
}
//# sourceMappingURL=packagesTemporal.js.map