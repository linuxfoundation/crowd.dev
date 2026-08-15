"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
exports.default = (sequelize) => {
    const customViewOrder = sequelize.define('customViewOrder', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        order: {
            type: sequelize_1.DataTypes.INTEGER,
            allowNull: true,
            validate: {
                min: 0,
            },
        },
    }, {
        indexes: [
            {
                fields: ['id', 'userId', 'customViewId'],
                where: {
                    deletedAt: null,
                },
            },
            {
                name: 'customViewOrder_unique',
                unique: true,
                fields: ['userId', 'customViewId'],
            },
        ],
        paranoid: true,
    });
    customViewOrder.associate = (models) => {
        customViewOrder.belongsTo(models.customView, {
            as: 'customView',
            onDelete: 'CASCADE',
            foreignKey: {
                allowNull: false,
            },
        });
        customViewOrder.belongsTo(models.user, {
            as: 'user',
            onDelete: 'CASCADE',
            foreignKey: {
                allowNull: false,
            },
        });
    };
    return customViewOrder;
};
//# sourceMappingURL=customViewOrder.js.map