"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getSecondsTillEndOfMonth = void 0;
const moment_1 = __importDefault(require("moment"));
const getSecondsTillEndOfMonth = () => {
    const endTime = (0, moment_1.default)().endOf('month');
    const startTime = (0, moment_1.default)();
    return endTime.diff(startTime, 'seconds');
};
exports.getSecondsTillEndOfMonth = getSecondsTillEndOfMonth;
//# sourceMappingURL=timing.js.map