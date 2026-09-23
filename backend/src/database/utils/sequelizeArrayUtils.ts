import Sequelize, { DataTypes, WhereOptions } from 'sequelize'

import { DB_CONFIG } from '../../conf'

export default class SequelizeArrayUtils {
  // MySQL doesn't have Array Field
  static get DataType() {
    return DB_CONFIG.dialect === 'mysql' ? DataTypes.JSON : DataTypes.ARRAY(DataTypes.TEXT)
  }

  static filter(tableName, fieldName, filterValue): WhereOptions {
    const filterValueAsArray = Array.isArray(filterValue) ? filterValue : [filterValue]

    if (DB_CONFIG.dialect === 'mysql') {
      return {
        [Sequelize.Op.and]: filterValueAsArray.map((filterValue) =>
          arrayContainsForMySQL(tableName, fieldName, filterValue),
        ),
      }
    }
    return {
      [fieldName]: {
        [Sequelize.Op.contains]: filterValueAsArray,
      },
    }
  }
}

function arrayContainsForMySQL(model, column, value) {
  return Sequelize.fn('JSON_CONTAINS', Sequelize.col(`${model}.${column}`), `"${value}"`)
}
