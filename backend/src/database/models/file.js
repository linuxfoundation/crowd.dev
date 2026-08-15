"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = (sequelize, DataTypes) => {
    const file = sequelize.define('file', {
        id: {
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4,
            primaryKey: true,
        },
        belongsTo: {
            type: DataTypes.STRING(255),
            allowNull: false,
            validate: {
                notEmpty: true,
                len: [0, 255],
            },
        },
        belongsToId: {
            type: DataTypes.STRING(255),
            allowNull: false,
            validate: {
                notEmpty: true,
                len: [0, 255],
            },
        },
        belongsToColumn: {
            type: DataTypes.STRING(255),
            allowNull: false,
            validate: {
                notEmpty: true,
                len: [0, 255],
            },
        },
        name: {
            type: DataTypes.STRING(2083),
            allowNull: false,
            validate: {
                notEmpty: true,
                len: [0, 2083],
            },
        },
        sizeInBytes: {
            type: DataTypes.INTEGER,
            allowNull: true,
        },
        privateUrl: {
            type: DataTypes.STRING(2083),
            allowNull: true,
            validate: {
                len: [0, 2083],
            },
        },
        publicUrl: {
            type: DataTypes.STRING(2083),
            allowNull: true,
            validate: {
                len: [0, 2083],
            },
        },
    }, {
        timestamps: true,
        paranoid: true,
    });
    file.associate = (models) => {
        models.file.belongsTo(models.tenant, {
            as: 'tenant',
        });
        models.file.belongsTo(models.user, {
            as: 'createdBy',
        });
        models.file.belongsTo(models.user, {
            as: 'updatedBy',
        });
    };
    return file;
};
//# sourceMappingURL=file.js.map