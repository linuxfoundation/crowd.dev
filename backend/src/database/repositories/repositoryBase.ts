/* eslint-disable class-methods-use-this,@typescript-eslint/no-unused-vars */
import { Sequelize } from 'sequelize'

import {
  QueryExecutor,
  SequelizeQueryExecutor,
  TransactionalSequelizeQueryExecutor,
  optionsQx,
} from '@crowd/data-access-layer/src/queryExecutor'
import { PageData, SearchCriteria } from '@crowd/types'

import { IRepositoryOptions } from './IRepositoryOptions'
import SequelizeRepository from './sequelizeRepository'

export abstract class RepositoryBase<
  TData,
  TId,
  TCreate,
  TUpdate,
  TCriteria extends SearchCriteria,
> {
  protected constructor(
    public readonly options: IRepositoryOptions,
    public readonly log: boolean = false,
  ) {}

  protected get currentUser(): any {
    return SequelizeRepository.getCurrentUser(this.options)
  }

  protected get currentTenant(): any {
    return SequelizeRepository.getCurrentTenant(this.options)
  }

  protected get transaction(): any {
    return SequelizeRepository.getTransaction(this.options)
  }

  protected get seq(): Sequelize {
    return SequelizeRepository.getSequelize(this.options)
  }

  protected get queryExecutor(): QueryExecutor {
    return optionsQx(this.options)
  }

  protected get database(): any {
    return this.options.database
  }

  async create(data: TCreate): Promise<TData> {
    throw new Error('Method not implemented.')
  }

  async update(id: TId, data: TUpdate): Promise<TData> {
    throw new Error('Method not implemented.')
  }

  async destroy(id: TId): Promise<void> {
    return this.destroyAll([id])
  }

  async destroyAll(ids: TId[]): Promise<void> {
    throw new Error('Method not implemented.')
  }

  async findById(id: TId): Promise<TData> {
    throw new Error('Method not implemented.')
  }

  async findAndCountAll(criteria: TCriteria): Promise<PageData<TData>> {
    throw new Error('Method not implemented.')
  }

  async findAll(criteria: TCriteria): Promise<TData[]> {
    const copy = { ...criteria }

    // let's initially load just the first row in the db to see how many elements there are in total
    copy.offset = undefined
    copy.limit = undefined
    const page = await this.findAndCountAll(criteria)

    return page.rows
  }

  protected async populateRelationsForRows(rows: any[]): Promise<any[]> {
    return Promise.all(rows.map((r) => this.populateRelations(r)))
  }

  protected async populateRelations(record: any): Promise<any> {
    if (!record) return record

    return record.get({ plain: true })
  }

  protected isPaginationValid(criteria: SearchCriteria): boolean {
    if (criteria.limit !== undefined && criteria.offset !== undefined) {
      return criteria.limit > 0 && criteria.offset >= 0
    }

    return false
  }

  protected getPaginationString(criteria: SearchCriteria): string {
    if (this.isPaginationValid(criteria)) {
      return `limit ${criteria.limit} offset ${criteria.offset}`
    }

    return ''
  }
}
