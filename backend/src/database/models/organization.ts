import { DataTypes, Op } from 'sequelize'

export default (sequelize) => {
  const organization = sequelize.define(
    'organization',
    {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      importHash: {
        type: DataTypes.STRING(255),
        allowNull: true,
        validate: {
          len: [0, 255],
        },
      },
      isTeamOrganization: {
        type: DataTypes.BOOLEAN,
        defaultValue: false,
        allowNull: false,
      },
      isAffiliationBlocked: {
        type: DataTypes.BOOLEAN,
        defaultValue: false,
        allowNull: false,
      },

      lastEnrichedAt: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      manuallyCreated: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: false,
      },

      displayName: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      location: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
        comment: 'A detailed description of the company',
      },
      logo: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      tags: {
        type: DataTypes.ARRAY(DataTypes.TEXT),
        allowNull: true,
        default: [],
      },
      employees: {
        type: DataTypes.INTEGER,
        allowNull: true,
        comment: 'total employee count of the company',
      },
      revenueRange: {
        type: DataTypes.JSONB,
        allowNull: true,
        comment: 'inferred revenue range of the company',
      },
      founded: {
        type: DataTypes.INTEGER,
        allowNull: true,
      },
      industry: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      size: {
        type: DataTypes.TEXT,
        allowNull: true,
        comment: 'A range representing the size of the company.',
      },
      headline: {
        type: DataTypes.TEXT,
        allowNull: true,
        comment: 'A brief description of the company',
      },
      type: {
        type: DataTypes.TEXT,
        allowNull: true,
        comment: "The company's type. For example NGO",
      },
      employeeChurnRate: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      employeeGrowthRate: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
    },
    {
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
            url: { [Op.ne]: null },
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
    },
  )

  organization.associate = (models) => {
    models.organization.belongsToMany(models.member, {
      as: 'members',
      through: 'memberOrganizations',
      foreignKey: 'organizationId',
    })

    models.organization.belongsToMany(models.segment, {
      as: 'segments',
      through: 'organizationSegments',
      timestamps: false,
    })

    models.organization.belongsTo(models.tenant, {
      as: 'tenant',
      foreignKey: {
        allowNull: false,
      },
    })

    models.organization.belongsTo(models.user, {
      as: 'createdBy',
    })

    models.organization.belongsTo(models.user, {
      as: 'updatedBy',
    })
  }

  return organization
}
