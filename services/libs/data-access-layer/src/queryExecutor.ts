import { QueryTypes, Sequelize, Transaction } from 'sequelize'

import { type QueryExecutor, formatQuery } from '@crowd/database'

export { PgPromiseQueryExecutor, dbStoreQx, formatQuery, pgpQx, repoQx } from '@crowd/database'
export type { QueryExecutor } from '@crowd/database'

/* eslint-disable @typescript-eslint/no-explicit-any */

/** Sequelize-backed QueryExecutor for legacy backend repositories. */
export class SequelizeQueryExecutor implements QueryExecutor {
  constructor(private readonly sequelize: Sequelize) {}

  protected prepareOptions(options: any): any {
    return options
  }

  select(query: string, params?: object): Promise<any> {
    return this.sequelize.query(
      formatQuery(query, params),
      this.prepareOptions({
        type: QueryTypes.SELECT,
      }),
    )
  }
  async selectNone(query: string, params?: object): Promise<void> {
    const result = await this.sequelize.query(
      formatQuery(query, params),
      this.prepareOptions({
        type: QueryTypes.SELECT,
      }),
    )
    if (result.length > 0) {
      throw new Error('Expected no rows')
    }
  }
  async selectOneOrNone(query: string, params?: object): Promise<any> {
    const result = await this.sequelize.query(
      formatQuery(query, params),
      this.prepareOptions({
        type: QueryTypes.SELECT,
      }),
    )
    if (result.length > 1) {
      throw new Error('Expected at most one row')
    }

    return result[0]
  }
  async selectOne(query: string, params?: object): Promise<any> {
    const result: any = await this.sequelize.query(
      formatQuery(query, params),
      this.prepareOptions({
        type: QueryTypes.SELECT,
      }),
    )
    if (result.length !== 1) {
      throw new Error('Expected exactly one row')
    }

    return result[0]
  }
  async result(query: string, params?: object): Promise<number> {
    const [, result] = await this.sequelize.query(
      formatQuery(query, params),
      this.prepareOptions({}),
    )
    if (typeof result === 'number') {
      return result
    }

    if (typeof result === 'object' && 'rowCount' in result) {
      return (result as any).rowCount
    }

    return result
  }

  async tx<T>(fn: (tx: QueryExecutor) => Promise<T>): Promise<T> {
    const transaction = await this.sequelize.transaction()

    try {
      const res = await fn(new TransactionalSequelizeQueryExecutor(this.sequelize, transaction))
      await transaction.commit()
      return res
    } catch (err) {
      await transaction.rollback()
      throw err
    }
  }
}

export class TransactionalSequelizeQueryExecutor extends SequelizeQueryExecutor {
  constructor(
    sequelize: Sequelize,
    private readonly transaction: Transaction,
  ) {
    super(sequelize)
  }

  protected prepareOptions(options: any): any {
    return {
      ...super.prepareOptions(options),
      transaction: this.transaction,
    }
  }
}

export function optionsQx(options: any): QueryExecutor {
  const seq = options.database.sequelize
  if (options.transaction) {
    return new TransactionalSequelizeQueryExecutor(seq, options.transaction)
  }

  return new SequelizeQueryExecutor(seq)
}
