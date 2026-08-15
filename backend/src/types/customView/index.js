"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaultCustomViews = void 0;
const types_1 = require("@crowd/types");
const member_1 = require("./member");
const organization_1 = require("./organization");
exports.defaultCustomViews = {
    [types_1.CustomViewPlacement.MEMBER]: member_1.memberCustomViews,
    [types_1.CustomViewPlacement.ORGANIZATION]: organization_1.organizationCustomViews,
};
//# sourceMappingURL=index.js.map