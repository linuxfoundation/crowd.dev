"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.eagleEyeActionModel = void 0;
const sequelize_1 = require("sequelize");
const eagleEyeActionModel = {
    id: {
        type: sequelize_1.DataTypes.UUID,
        defaultValue: sequelize_1.DataTypes.UUIDV4,
        primaryKey: true,
    },
    type: {
        type: sequelize_1.DataTypes.TEXT,
        validate: {
            isIn: [['thumbs-up', 'thumbs-down', 'bookmark']],
        },
        defaultValue: null,
    },
    timestamp: {
        type: sequelize_1.DataTypes.DATE,
        allowNull: false,
    },
};
exports.eagleEyeActionModel = eagleEyeActionModel;
exports.default = (sequelize) => {
    const eagleEyeAction = sequelize.define('eagleEyeAction', eagleEyeActionModel, {
        timestamps: true,
        paranoid: false,
    });
    eagleEyeAction.associate = (models) => {
        models.eagleEyeAction.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.eagleEyeAction.belongsTo(models.user, {
            as: 'actionBy',
        });
        models.eagleEyeAction.belongsTo(models.eagleEyeContent, {
            as: 'content',
        });
    };
    return eagleEyeAction;
};
//# sourceMappingURL=eagleEyeAction.js.map