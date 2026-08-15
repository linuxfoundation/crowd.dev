"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HEALTH_BAND_SET = exports.LIFECYCLE_VALUES = exports.STEWARDSHIP_STATUS_VALUES = exports.HEALTH_BAND_VALUES = void 0;
const data_access_layer_1 = require("@crowd/data-access-layer");
Object.defineProperty(exports, "HEALTH_BAND_VALUES", { enumerable: true, get: function () { return data_access_layer_1.HEALTH_BAND_VALUES; } });
exports.STEWARDSHIP_STATUS_VALUES = [
    'unassigned',
    'open',
    'assessing',
    'active',
    'needs_attention',
    'escalated',
    'blocked',
    'inactive',
];
exports.LIFECYCLE_VALUES = ['active', 'stable', 'declining', 'abandoned', 'archived'];
exports.HEALTH_BAND_SET = new Set(data_access_layer_1.HEALTH_BAND_VALUES);
//# sourceMappingURL=types.js.map