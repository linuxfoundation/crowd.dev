"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const types_1 = require("@crowd/types");
exports.default = (sequelize) => {
    const customView = sequelize.define('customView', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        name: {
            type: sequelize_1.DataTypes.STRING,
            allowNull: false,
            validate: {
                notEmpty: true,
            },
        },
        visibility: {
            type: sequelize_1.DataTypes.ENUM,
            values: Object.values(types_1.CustomViewVisibility),
            allowNull: false,
        },
        config: {
            type: sequelize_1.DataTypes.JSONB,
            defaultValue: {},
        },
        placement: {
            type: sequelize_1.DataTypes.TEXT,
            isIn: [['member', 'organization', 'activity']],
            allowNull: false,
        },
        tenantId: {
            type: sequelize_1.DataTypes.UUID,
            allowNull: false,
        },
    }, {
        indexes: [
            {
                unqiue: true,
                fields: ['id', 'tenantId'],
                where: {
                    deletedAt: null,
                },
            },
        ],
        timestamps: true,
        paranoid: true,
    });
    customView.associate = (models) => {
        models.customView.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.customView.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.customView.belongsTo(models.user, {
            as: 'updatedBy',
        });
        models.customView.hasMany(models.customViewOrder, {
            as: 'customViewOrders',
            foreignKey: 'customViewId',
        });
    };
    return customView;
};
//# sourceMappingURL=customView.js.map