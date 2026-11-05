/* eslint-disable @typescript-eslint/no-explicit-any */
import { MemberAttributeType } from '@crowd/types'
import { JsonColumnInfo, Operator, ParsedJsonColumn } from '@crowd/types'

import { singleOrDefault } from './array'

// ph for placeholder
function ph(field: string, { pgPromiseFormat }: { pgPromiseFormat: boolean }): string {
  return pgPromiseFormat ? `$(${field})` : `:${field}`
}

export interface IRawQueryParserOptions {
  pgPromiseFormat: boolean
  keepOnly?: string[]
}

export class RawQueryParser {
  public static parseFilters(
    filters: any,
    columnMap: Map<string, string>,
    jsonColumnInfos: JsonColumnInfo[],
    params: any,
    options: IRawQueryParserOptions = {
      pgPromiseFormat: false,
      keepOnly: [],
    },
  ): string {
    const keys = Object.keys(filters)
    if (keys.length === 0) {
      return '(1=1)'
    }

    const results = []

    for (const key of keys) {
      if (this.isOperator(key)) {
        const operands = []
        for (const operand of filters[key]) {
          if (!operand) {
            continue
          }
          operands.push(this.parseFilters(operand, columnMap, jsonColumnInfos, params, options))
        }

        const condition = operands.join(` ${key} `)
        results.push(`(${condition})`)
      } else if (key === Operator.NOT) {
        const condition = this.parseFilters(
          filters[key],
          columnMap,
          jsonColumnInfos,
          params,
          options,
        )
        results.push(`(not ${condition})`)
      } else {
        const jsonColumnInfo = this.getJsonColumnInfo(key, jsonColumnInfos)

        if (jsonColumnInfo === undefined && !columnMap.has(key)) {
          throw new Error(`Unknown filter key: ${key}!`)
        }

        if (jsonColumnInfo) {
          results.push(this.parseJsonColumnCondition(jsonColumnInfo, filters[key], params, options))
        } else {
          // handle column maps without quotes/alias to handle postgres camelCase columns
          // no double quotes, add for support of camelCase columns
          let column = columnMap.get(key)
          if (column.match(/^[a-zA-Z0-9_]+$/)) {
            column = `"${column}"`
          }

          if (
            options.keepOnly === undefined ||
            options.keepOnly.length === 0 ||
            options.keepOnly.includes(key)
          ) {
            results.push(this.parseColumnCondition(key, column, filters[key], params, options))
          }
        }
      }
    }

    return results.join(' and ')
  }

  private static parseJsonColumnCondition(
    property: ParsedJsonColumn,
    filters: any,
    params: any,
    options: IRawQueryParserOptions,
  ): string {
    const parts = property.parts.slice(1)

    let jsonColumn: string
    if (parts.length > 0) {
      const attribute = parts[0]
      const attributeInfo = singleOrDefault(
        property.info.attributeInfos,
        (a) => a.name === attribute,
      )
      if (attributeInfo === undefined) {
        throw new Error(`Unknown ${property.info.property} attribute: ${attribute}!`)
      }

      const isText = this.isJsonPropertyText(attributeInfo.type)

      let nestedProperty = ''
      for (let i = 0; i < parts.length; i++) {
        const part = parts[i]

        if (isText && i === parts.length - 1) {
          nestedProperty += ` ->> '${part}'`
        } else {
          nestedProperty += ` -> '${part}'`
        }
      }
      jsonColumn = `(${property.info.column}${nestedProperty})`

      if (attributeInfo.type === MemberAttributeType.BOOLEAN) {
        jsonColumn = `${jsonColumn}::boolean`
      } else if (attributeInfo.type === MemberAttributeType.NUMBER) {
        jsonColumn = `${jsonColumn}::integer`
      } else if (attributeInfo.type === MemberAttributeType.DATE) {
        jsonColumn = `${jsonColumn}::timestamptz`
      }
    } else {
      jsonColumn = `(${property.info.column})`
    }

    let value
    let operator: Operator

    if (Array.isArray(filters)) {
      operator = Operator.CONTAINS
      value = filters
    } else {
      const conditionKeys = Object.keys(filters)
      if (conditionKeys.length !== 1) {
        throw new Error(`Invalid condition! ${JSON.stringify(filters, undefined, 2)}`)
      }

      operator = conditionKeys[0] as Operator
      value = filters[operator]
    }

    const actualOperator = this.getOperator(operator, true)

    if (operator === Operator.BETWEEN || operator === Operator.NOT_BETWEEN) {
      // we need two values
      const firstParamName = this.getJsonParamName(property.info.property, parts, params)
      params[firstParamName] = value[0]

      const secondParamName = this.getJsonParamName(property.info.property, parts, params)
      params[secondParamName] = value[1]

      return [
        '(',
        jsonColumn,
        ' ',
        actualOperator,
        ' ',
        ph(firstParamName, options),
        ' and ',
        ph(secondParamName, options),
        ')',
      ].join('')
    }
    if (operator === Operator.CONTAINS || operator === Operator.OVERLAP) {
      // we need an array of values
      const paramNames: string[] = []

      for (const val of value) {
        const paramName = this.getJsonParamName(property.info.property, parts, params)
        params[paramName] = val
        paramNames.push(ph(paramName, options))
      }

      const paramNamesString = paramNames.join(', ')
      return `(${jsonColumn} ${actualOperator} array[${paramNamesString}])`
    }
    if (operator === Operator.IN || operator === Operator.NOT_IN) {
      // we need a list of values
      const paramNames: string[] = []

      for (const val of value) {
        const paramName = this.getJsonParamName(property.info.property, parts, params)
        params[paramName] = val
        paramNames.push(ph(paramName, options))
      }

      const paramNamesString = paramNames.join(', ')
      return `(${jsonColumn} ${actualOperator} (${paramNamesString}))`
    }
    const paramName = this.getJsonParamName(property.info.property, parts, params)
    if (operator === Operator.LIKE || operator === Operator.NOT_LIKE) {
      params[paramName] = `%${value}%`
    } else {
      params[paramName] = value
    }

    return `(${jsonColumn} ${actualOperator} ${ph(paramName, options)})`
  }

  private static parseColumnCondition(
    key: string,
    column: string,
    filters: any,
    params: any,
    options: IRawQueryParserOptions,
  ): string {
    const conditionKeys = Object.keys(filters)
    if (conditionKeys.length !== 1) {
      throw new Error(`Invalid condition! ${JSON.stringify(filters, undefined, 2)}`)
    }

    const operator = conditionKeys[0] as Operator
    const actualOperator = this.getOperator(operator)
    const value = filters[operator]

    if (operator === Operator.BETWEEN || operator === Operator.NOT_BETWEEN) {
      // we need two values
      const firstParamName = this.getParamName(key, params)
      params[firstParamName] = value[0]

      const secondParamName = this.getParamName(key, params)
      params[secondParamName] = value[1]

      return [
        '(',
        column,
        ' ',
        actualOperator,
        ' ',
        ph(firstParamName, options),
        ' and ',
        ph(secondParamName, options),
        ')',
      ].join('')
    }
    if (operator === Operator.CONTAINS || operator === Operator.OVERLAP) {
      // we need an array of values
      const paramNames: string[] = []

      for (const val of value) {
        const paramName = this.getParamName(key, params)
        params[paramName] = val
        paramNames.push(ph(paramName, options))
      }

      const paramNamesString = paramNames.join(', ')
      return `(${column} ${actualOperator} array[${paramNamesString}])`
    }
    if (operator === Operator.IN || operator === Operator.NOT_IN) {
      // we need a list of values
      const paramNames: string[] = []

      for (const val of value) {
        const paramName = this.getParamName(key, params)
        params[paramName] = val
        paramNames.push(ph(paramName, options))
      }

      const paramNamesString = paramNames.join(', ')
      return `(${column} ${actualOperator} (${paramNamesString}))`
    }

    const paramName = this.getParamName(key, params)

    if (
      operator === Operator.EQUAL &&
      (value === null || (typeof value === 'string' && value.toLowerCase() === 'null'))
    ) {
      params[paramName] = null
      return `(${column} is ${ph(paramName, options)})`
    }

    if (
      operator === Operator.NOT_EQUAL &&
      (value === null || (typeof value === 'string' && value.toLowerCase() === 'null'))
    ) {
      params[paramName] = null
      return `(${column} is not ${ph(paramName, options)})`
    }

    if (
      operator === Operator.LIKE ||
      operator === Operator.NOT_LIKE ||
      operator === Operator.TEXT_CONTAINS ||
      operator === Operator.NOT_TEXT_CONTAINS
    ) {
      params[paramName] = `%${value}%`
    } else {
      params[paramName] = value
    }

    if (operator === Operator.MATCH_PHRASE_PREFIX) {
      params[paramName] = `${value}%`
      return `(${column} ilike ${ph(paramName, options)})`
    }

    return `(${column} ${actualOperator} ${ph(paramName, options)})`
  }

  private static getJsonColumnInfo(
    column: string,
    jsonColumnInfos: JsonColumnInfo[],
  ): ParsedJsonColumn | undefined {
    const parts = column.split('.')

    const actualProperty = parts[0]
    const info = singleOrDefault(
      jsonColumnInfos,
      (jsonColumnInfo) => jsonColumnInfo.property === actualProperty,
    )

    if (info) {
      return {
        info,
        parts,
      }
    }

    return undefined
  }

  private static getOperator(operator: Operator, json = false): string {
    switch (operator) {
      case Operator.GREATER_THAN:
        return '>'
      case Operator.GREATER_THAN_OR_EQUAL:
        return '>='
      case Operator.LESS_THAN:
        return '<'
      case Operator.LESS_THAN_OR_EQUAL:
        return '<='
      case Operator.NOT_EQUAL:
      case Operator.NOT:
        return '<>'
      case Operator.EQUAL:
        return '='
      case Operator.LIKE:
      case Operator.TEXT_CONTAINS:
        return 'ilike'
      case Operator.NOT_LIKE:
      case Operator.NOT_TEXT_CONTAINS:
        return 'not ilike'
      case Operator.AND:
        return 'and'
      case Operator.OR:
        return 'or'
      case Operator.IN:
        return 'in'
      case Operator.NOT_IN:
        return 'not in'
      case Operator.BETWEEN:
        return 'between'
      case Operator.NOT_BETWEEN:
        return 'not between'
      case Operator.MATCH_PHRASE_PREFIX:
        return 'matchPhrasePrefix'
      case Operator.OVERLAP:
        if (json) {
          return '?|'
        }
        return '&&'
      case Operator.CONTAINS:
        if (json) {
          return '?&'
        }
        return '@>'

      default:
        throw new Error(`Unknown operator: ${operator}!`)
    }
  }

  private static isJsonPropertyText(type: MemberAttributeType): boolean {
    return (
      type === MemberAttributeType.STRING ||
      type === MemberAttributeType.EMAIL ||
      type === MemberAttributeType.URL
    )
  }

  // A name is taken once it's a key in params, even if its value is undefined.
  private static getParamName(column: string, params: any): string {
    let index = 1

    while (`${column}_${index}` in params) {
      index++
    }

    return `${column}_${index}`
  }

  private static getJsonParamName(column: string, parts: string[], params: any): string {
    let index = 1

    let key: string
    if (parts.length > 0) {
      key = `${column}_${parts.join('_')}`
    } else {
      key = column
    }

    while (`${key}_${index}` in params) {
      index++
    }

    return `${key}_${index}`
  }

  private static isOperator(opOrCondition: any): boolean {
    const lower = opOrCondition.toLowerCase().trim()
    return lower === 'and' || lower === 'or'
  }
}
