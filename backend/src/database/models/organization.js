"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
exports.default = (sequelize) => {
    const organization = sequelize.define('organization', {
        id: {
            type: sequelize_1.DataTypes.UUID,
            defaultValue: sequelize_1.DataTypes.UUIDV4,
            primaryKey: true,
        },
        importHash: {
            type: sequelize_1.DataTypes.STRING(255),
            allowNull: true,
            validate: {
                len: [0, 255],
            },
        },
        isTeamOrganization: {
            type: sequelize_1.DataTypes.BOOLEAN,
            defaultValue: false,
            allowNull: false,
        },
        isAffiliationBlocked: {
            type: sequelize_1.DataTypes.BOOLEAN,
            defaultValue: false,
            allowNull: false,
        },
        lastEnrichedAt: {
            type: sequelize_1.DataTypes.DATE,
            allowNull: true,
        },
        manuallyCreated: {
            type: sequelize_1.DataTypes.BOOLEAN,
            allowNull: false,
            defaultValue: false,
        },
        displayName: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
        },
        location: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
        },
        country: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
        },
        description: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
            comment: 'A detailed description of the company',
        },
        logo: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
        },
        tags: {
            type: sequelize_1.DataTypes.ARRAY(sequelize_1.DataTypes.TEXT),
            allowNull: true,
            default: [],
        },
        employees: {
            type: sequelize_1.DataTypes.INTEGER,
            allowNull: true,
            comment: 'total employee count of the company',
        },
        revenueRange: {
            type: sequelize_1.DataTypes.JSONB,
            allowNull: true,
            comment: 'inferred revenue range of the company',
        },
        founded: {
            type: sequelize_1.DataTypes.INTEGER,
            allowNull: true,
        },
        industry: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
        },
        size: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
            comment: 'A range representing the size of the company.',
        },
        headline: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
            comment: 'A brief description of the company',
        },
        type: {
            type: sequelize_1.DataTypes.TEXT,
            allowNull: true,
            comment: "The company's type. For example NGO",
        },
        employeeChurnRate: {
            type: sequelize_1.DataTypes.JSONB,
            allowNull: true,
        },
        employeeGrowthRate: {
            type: sequelize_1.DataTypes.JSONB,
            allowNull: true,
        },
    }, {
        indexes: [
            {
                unique: true,
                fields: ['importHash', 'tenantId'],
                where: {
                    deletedAt: null,
                },
            },
            {
                fields: ['url', 'tenantId'],
                unique: true,
                where: {
                    deletedAt: null,
                    url: { [sequelize_1.Op.ne]: null },
                },
            },
            {
                fields: ['name', 'tenantId'],
                unique: true,
                where: {
                    deletedAt: null,
                },
            },
        ],
        timestamps: true,
        paranoid: true,
    });
    organization.associate = (models) => {
        models.organization.belongsToMany(models.member, {
            as: 'members',
            through: 'memberOrganizations',
            foreignKey: 'organizationId',
        });
        models.organization.belongsToMany(models.segment, {
            as: 'segments',
            through: 'organizationSegments',
            timestamps: false,
        });
        models.organization.belongsTo(models.tenant, {
            as: 'tenant',
            foreignKey: {
                allowNull: false,
            },
        });
        models.organization.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.organization.belongsTo(models.user, {
            as: 'updatedBy',
        });
    };
    return organization;
};
//# sourceMappingURL=organization.js.map