"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ok = ok;
exports.created = created;
exports.noContent = noContent;
function ok(res, data) {
    return res.status(200).json(data);
}
function created(res, data) {
    return res.status(201).json(data);
}
function noContent(res) {
    return res.status(204).send();
}
//# sourceMappingURL=api.js.map