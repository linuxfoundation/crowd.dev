"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SlackCommandParameterType = exports.SlackCommand = void 0;
var SlackCommand;
(function (SlackCommand) {
    SlackCommand["HELP"] = "help";
    SlackCommand["PRINT_TENANT"] = "print-tenant";
    SlackCommand["SET_TENANT_PLAN"] = "set-tenant-plan";
})(SlackCommand || (exports.SlackCommand = SlackCommand = {}));
var SlackCommandParameterType;
(function (SlackCommandParameterType) {
    SlackCommandParameterType["STRING"] = "string";
    SlackCommandParameterType["NUMBER"] = "number";
    SlackCommandParameterType["BOOLEAN"] = "boolean";
    SlackCommandParameterType["UUID"] = "uuid";
    SlackCommandParameterType["DATE"] = "date";
})(SlackCommandParameterType || (exports.SlackCommandParameterType = SlackCommandParameterType = {}));
//# sourceMappingURL=slackTypes.js.map