"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.requireScopes = void 0;
const common_1 = require("@crowd/common");
const requireScopes = (required, mode = 'all') => (req, _res, next) => {
    if (!req.actor) {
        next(new common_1.UnauthorizedError());
        return;
    }
    const granted = new Set(req.actor.scopes);
    const hasAccess = mode === 'all' ? required.every((s) => granted.has(s)) : required.some((s) => granted.has(s));
    if (!hasAccess) {
        next(new common_1.InsufficientScopeError());
        return;
    }
    next();
};
exports.requireScopes = requireScopes;
//# sourceMappingURL=requireScopes.js.map