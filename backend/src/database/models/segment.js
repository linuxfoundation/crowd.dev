"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
exports.default = (sequelize) => {
    const segment = sequelize.define('segment', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        url: {
            type: sequelize_1.DataTypes.TEXT,
        },
        name: {
            type: sequelize_1.DataTypes.TEXT,
        },
        parentName: {
            type: sequelize_1.DataTypes.TEXT,
        },
        grandparentName: {
            type: sequelize_1.DataTypes.TEXT,
        },
        slug: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: false,
        },
        parentSlug: {
            type: sequelize_1.DataTypes.TEXT,
        },
        grandparentSlug: {
            type: sequelize_1.DataTypes.TEXT,
        },
        status: {
            type: sequelize_1.DataTypes.TEXT,
            validate: {
                isIn: [['active', 'archived', 'formation', 'prospect']],
            },
        },
        description: {
            type: sequelize_1.DataTypes.TEXT,
        },
        sourceId: {
            type: sequelize_1.DataTypes.TEXT,
        },
        sourceParentId: {
            type: sequelize_1.DataTypes.TEXT,
        },
    }, {
        indexes: [
            {
                unique: true,
                fields: ['slug', 'parentSlug', 'grandparentSlug', 'tenantId'],
            },
        ],
        timestamps: false,
        paranoid: false,
    });
    segment.associate = (models) => {
        models.segment.belongsTo(models.tenant, {
            foreignKey: {
                allowNull: false,
            },
        });
    };
    return segment;
};
//# sourceMappingURL=segment.js.map