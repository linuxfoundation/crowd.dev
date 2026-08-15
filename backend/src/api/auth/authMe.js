"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
exports.default = async (req, res) => {
    if (!req.currentUser || !req.currentUser.id) {
        await req.responseHandler.error(req, res, new common_1.Error403(req.language));
        return;
    }
    const payload = req.currentUser;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=authMe.js.map