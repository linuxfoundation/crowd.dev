"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const types_1 = require("@crowd/types");
exports.default = (sequelize, DataTypes) => {
    const settings = sequelize.define('settings', {
        id: {
            type: DataTypes.STRING,
            defaultValue: 'default',
            primaryKey: true,
        },
        website: {
            type: DataTypes.STRING(255),
        },
        backgroundImageUrl: {
            type: DataTypes.STRING(1024),
        },
        logoUrl: {
            type: DataTypes.STRING(1024),
        },
        slackWebHook: {
            type: DataTypes.STRING(1024),
        },
        organizationsViewed: {
            type: DataTypes.BOOLEAN(),
        },
        contactsViewed: {
            type: DataTypes.BOOLEAN(),
        },
        attributeSettings: {
            type: DataTypes.JSONB,
            allowNull: false,
            defaultValue: {
                priorities: [
                    'custom',
                    types_1.PlatformType.TWITTER,
                    types_1.PlatformType.GITHUB,
                    types_1.PlatformType.LINKEDIN,
                    types_1.PlatformType.REDDIT,
                    types_1.PlatformType.DEVTO,
                    types_1.PlatformType.HACKERNEWS,
                    types_1.PlatformType.SLACK,
                    types_1.PlatformType.DISCORD,
                    types_1.PlatformType.ENRICHMENT,
                    types_1.PlatformType.CROWD,
                ],
            },
        },
    }, {
        timestamps: true,
        paranoid: true,
    });
    settings.associate = (models) => {
        models.settings.hasMany(models.file, {
            as: 'logos',
            foreignKey: 'belongsToId',
            constraints: false,
            scope: {
                belongsTo: models.settings.getTableName(),
                belongsToColumn: 'logos',
            },
        });
        models.settings.hasMany(models.file, {
            as: 'backgroundImages',
            foreignKey: 'belongsToId',
            // constraints: false,
            scope: {
                belongsTo: models.settings.getTableName(),
                belongsToColumn: 'backgroundImages',
            },
        });
        models.settings.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.settings.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.settings.belongsTo(models.user, {
            as: 'updatedBy',
        });
    };
    return settings;
};
//# sourceMappingURL=settings.js.map