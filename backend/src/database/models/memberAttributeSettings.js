"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const types_1 = require("@crowd/types");
exports.default = (sequelize) => {
    const memberAttributeSettings = sequelize.define('memberAttributeSettings', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        type: {
            type: sequelize_1.DataTypes.ENUM,
            values: Object.values(types_1.MemberAttributeType),
            allowNull: false,
        },
        canDelete: {
            type: sequelize_1.DataTypes.BOOLEAN,
            allowNull: false,
            defaultValue: true,
        },
        show: {
            type: sequelize_1.DataTypes.BOOLEAN,
            allowNull: false,
            defaultValue: true,
        },
        label: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: false,
            validate: {
                notEmpty: true,
            },
        },
        name: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: false,
            validate: {
                notEmpty: true,
            },
        },
        options: {
            type: sequelize_1.DataTypes.ARRAY(sequelize_1.DataTypes.TEXT),
            allowNull: false,
            defaultValue: [],
        },
    }, {
        indexes: [
            {
                unique: true,
                fields: ['name', 'tenantId'],
            },
        ],
        timestamps: true,
    });
    memberAttributeSettings.associate = (models) => {
        models.memberAttributeSettings.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.memberAttributeSettings.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.memberAttributeSettings.belongsTo(models.user, {
            as: 'updatedBy',
        });
    };
    return memberAttributeSettings;
};
//# sourceMappingURL=memberAttributeSettings.js.map