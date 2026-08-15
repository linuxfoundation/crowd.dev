"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
exports.default = (sequelize) => {
    const integration = sequelize.define('integration', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        platform: {
            type: sequelize_1.DataTypes.TEXT,
        },
        status: {
            type: sequelize_1.DataTypes.TEXT,
        },
        token: {
            type: sequelize_1.DataTypes.TEXT,
        },
        refreshToken: {
            type: sequelize_1.DataTypes.TEXT,
        },
        settings: {
            type: sequelize_1.DataTypes.JSONB,
            defaultValue: {},
        },
        integrationIdentifier: {
            type: sequelize_1.DataTypes.TEXT,
        },
    }, {
        indexes: [
            {
                unique: false,
                fields: ['integrationIdentifier'],
            },
        ],
        timestamps: true,
        paranoid: true,
    });
    integration.associate = (models) => {
        models.integration.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.integration.belongsTo(models.segment, {
            as: 'segment',
            foreignKey: {
                allowNull: false,
            },
        });
        models.integration.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.integration.belongsTo(models.user, {
            as: 'updatedBy',
        });
    };
    return integration;
};
//# sourceMappingURL=integration.js.map