"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert_1 = __importDefault(require("assert"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const userRepository_1 = __importDefault(require("../../database/repositories/userRepository"));
class AuthProfileEditor {
    constructor(options) {
        this.options = options;
        this.transaction = null;
    }
    async execute(data) {
        this.data = data;
        await this._validate();
        try {
            this.transaction = await sequelizeRepository_1.default.createTransaction(this.options);
            await userRepository_1.default.updateProfile(this.options.currentUser.id, this.data, {
                ...this.options,
                bypassPermissionValidation: true,
            });
            await sequelizeRepository_1.default.commitTransaction(this.transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(this.transaction);
            throw error;
        }
    }
    async _validate() {
        (0, assert_1.default)(this.options.currentUser, 'currentUser is required');
        (0, assert_1.default)(this.options.currentUser.id, 'currentUser.id is required');
        (0, assert_1.default)(this.options.currentUser.email, 'currentUser.email is required');
        (0, assert_1.default)(this.data, 'profile is required');
    }
}
exports.default = AuthProfileEditor;
//# sourceMappingURL=authProfileEditor.js.map