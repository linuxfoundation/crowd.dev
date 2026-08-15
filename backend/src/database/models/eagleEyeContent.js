"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.eagleEyeContentModel = void 0;
const sequelize_1 = require("sequelize");
const eagleEyeContentModel = {
    id: {
        type: sequelize_1.DataTypes.UUID,
        defaultValue: sequelize_1.DataTypes.UUIDV4,
        primaryKey: true,
    },
    platform: {
        type: sequelize_1.DataTypes.TEXT,
        allowNull: false,
        validate: {
            notEmpty: true,
        },
    },
    post: {
        type: sequelize_1.DataTypes.JSONB,
        allowNull: false,
    },
    url: {
        type: sequelize_1.DataTypes.TEXT,
        allowNull: false,
        validate: {
            notEmpty: true,
        },
    },
    postedAt: {
        type: sequelize_1.DataTypes.DATE,
        allowNull: false,
    },
};
exports.eagleEyeContentModel = eagleEyeContentModel;
exports.default = (sequelize) => {
    const eagleEyeContent = sequelize.define('eagleEyeContent', eagleEyeContentModel, {
        timestamps: true,
        paranoid: false,
    });
    eagleEyeContent.associate = (models) => {
        models.eagleEyeContent.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.eagleEyeContent.hasMany(models.eagleEyeAction, {
            as: 'actions',
            foreignKey: 'contentId',
        });
    };
    return eagleEyeContent;
};
//# sourceMappingURL=eagleEyeContent.js.map