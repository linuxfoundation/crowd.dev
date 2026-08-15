"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebhookError = exports.WebhookType = exports.WebhookState = void 0;
const baseError_1 = require("./baseError");
var WebhookState;
(function (WebhookState) {
    WebhookState["PENDING"] = "PENDING";
    WebhookState["PROCESSED"] = "PROCESSED";
    WebhookState["PROCESSING"] = "PROCESSING";
    WebhookState["ERROR"] = "ERROR";
})(WebhookState || (exports.WebhookState = WebhookState = {}));
var WebhookType;
(function (WebhookType) {
    WebhookType["GITHUB"] = "GITHUB";
    WebhookType["DISCOURSE"] = "DISCOURSE";
    WebhookType["GROUPSIO"] = "GROUPSIO";
})(WebhookType || (exports.WebhookType = WebhookType = {}));
class WebhookError extends baseError_1.BaseError {
    constructor(webhookId, message, origError) {
        super(message, origError);
        this.webhookId = webhookId;
    }
}
exports.WebhookError = WebhookError;
//# sourceMappingURL=webhooks.js.map