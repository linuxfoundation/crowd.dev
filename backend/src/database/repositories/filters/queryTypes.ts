import { MemberAttributeType } from '@crowd/types'

export interface QueryInput {
  filter: any
  orderBy: any
  limit?: string | number
  offset?: string | number
  fields?: false | string | string[]
  include?: any
}

export interface QueryOutput {
  where?: any
  having?: any
  limit: number
  order?: Array<string[]>
  offset: number
  attributes?: string[]
}

export interface ManyToManyType {
  [key: string]: {
    table: string
    model?: string
    overrideJoinField?: string
    relationTable: {
      name: string
      from: string
      to: string
    }
  }
}

export enum Operator {
  GREATER_THAN = 'gt',
  GREATER_THAN_OR_EQUAL = 'gte',
  LESS_THAN = 'lt',
  LESS_THAN_OR_EQUAL = 'lte',
  NOT_EQUAL = 'ne',
  EQUAL = 'eq',

  // case insensitive ilike
  LIKE = 'like',
  NOT_LIKE = 'notLike',

  TEXT_CONTAINS = 'textContains',
  NOT_TEXT_CONTAINS = 'notContains',

  REGEX = 'regexp',
  NOT_REGEX = 'notRegexp',

  AND = 'and',
  OR = 'or',

  IN = 'in',
  NOT_IN = 'notIn',

  BETWEEN = 'between',
  NOT_BETWEEN = 'notBetween',

  OVERLAP = 'overlap',
  CONTAINS = 'contains',

  NOT = 'not',
}

export interface ParsedJsonColumn {
  parts: string[]
  info: JsonColumnInfo
}

export interface AttributeInfo {
  name: string
  type: MemberAttributeType
}

export interface JsonColumnInfo {
  property: string
  column: string
  attributeInfos: AttributeInfo[]
}
