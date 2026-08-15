"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class GithubInstallationsRepository {
    static async getInstallations(options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        return seq.query(`
       select * from "githubInstallations"
      `, {
            transaction,
            type: sequelize_1.QueryTypes.SELECT,
        });
    }
}
exports.default = GithubInstallationsRepository;
//# sourceMappingURL=githubInstallationsRepository.js.map