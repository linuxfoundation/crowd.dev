"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class MemberOrganizationRepository {
    static async findRolesBelongingToBothEntities(primaryId, secondaryId, entityIdField, intersectBasedOnField, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const sequelize = sequelizeRepository_1.default.getSequelize(options);
        const results = await sequelize.query(`
      SELECT  mo.*
      FROM "memberOrganizations" AS mo
      WHERE mo."deletedAt" is null and
         mo."${intersectBasedOnField}" IN (
          SELECT "${intersectBasedOnField}"
          FROM "memberOrganizations"
          WHERE "${entityIdField}" = :primaryId
      )
      AND mo."${intersectBasedOnField}" IN (
          SELECT "${intersectBasedOnField}"
          FROM "memberOrganizations"
          WHERE "${entityIdField}" = :secondaryId)
      AND mo."${entityIdField}" IN (:primaryId, :secondaryId);

    `, {
            replacements: {
                primaryId,
                secondaryId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    static async findNonIntersectingRoles(primaryId, secondaryId, entityIdField, intersectBasedOnField, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const remainingRoles = (await seq.query(`
        SELECT *
        FROM "memberOrganizations"
        WHERE "${entityIdField}" = :secondaryId
        AND "deletedAt" IS NULL
        AND "${intersectBasedOnField}" NOT IN (
            SELECT "${intersectBasedOnField}"
            FROM "memberOrganizations"
            WHERE "${entityIdField}" = :primaryId
            AND "deletedAt" IS NULL
        );
      `, {
            replacements: {
                primaryId,
                secondaryId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        }));
        return remainingRoles;
    }
    static async findRolesInOrganization(organizationId, options) {
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const rolesInOrganization = (await seq.query(`
        SELECT *
        FROM "memberOrganizations"
        WHERE "organizationId" = :organizationId
        AND "deletedAt" IS NULL;
      `, {
            replacements: {
                organizationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        }));
        return rolesInOrganization;
    }
}
exports.default = MemberOrganizationRepository;
//# sourceMappingURL=memberOrganizationRepository.js.map