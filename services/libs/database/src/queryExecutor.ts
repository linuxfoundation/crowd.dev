import pgp from 'pg-promise'

import { DbStore } from './dbStore'
import { RepositoryBase } from './repoBase'
import type { DbConnOrTx, DbConnection, DbTransaction } from './types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export interface QueryExecutor {
  select(query: string, params?: object): Promise<any>
  selectNone(query: string, params?: object): Promise<void>
  selectOneOrNone(query: string, params?: object): Promise<any>
  selectOne(query: string, params?: object): Promise<any>
  result(query: string, params?: object): Promise<number>

  tx<T>(fn: (tx: QueryExecutor) => Promise<T>): Promise<T>
}

export function formatQuery(query: string, params?: object): string {
  return pgp.as.format(query, params)
}

export class PgPromiseQueryExecutor implements QueryExecutor {
  constructor(private readonly db: DbConnection | DbTransaction) {}

  select(query: string, params?: object): Promise<any> {
    return this.db.query(formatQuery(query, params))
  }

  selectNone(query: string, params?: object): Promise<void> {
    return this.db.none(formatQuery(query, params))
  }

  selectOneOrNone(query: string, params?: object): Promise<any> {
    return this.db.oneOrNone(formatQuery(query, params))
  }

  selectOne(query: string, params?: object): Promise<any> {
    return this.db.one(formatQuery(query, params))
  }

  async result(query: string, params?: object): Promise<number> {
    const result = await this.db.result(formatQuery(query, params))
    return result.rowCount
  }

  tx<T>(fn: (tx: QueryExecutor) => Promise<T>): Promise<T> {
    return this.db.tx((tx) => fn(new PgPromiseQueryExecutor(tx)))
  }
}

export function pgpQx(db: DbConnOrTx): QueryExecutor {
  return new PgPromiseQueryExecutor(db)
}

export function dbStoreQx(dbStore: DbStore): QueryExecutor {
  return pgpQx(dbStore.connection())
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function repoQx(repo: RepositoryBase<any>): QueryExecutor {
  return pgpQx(repo.db())
}
