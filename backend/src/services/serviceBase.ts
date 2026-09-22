/* eslint-disable class-methods-use-this,@typescript-eslint/no-unused-vars */
import { PageData, SearchCriteria } from '@crowd/types'

import { IRepositoryOptions } from '../database/repositories/IRepositoryOptions'
import SequelizeRepository from '../database/repositories/sequelizeRepository'
import { IServiceOptions } from './IServiceOptions'

export abstract class ServiceBase<TData, TId, TCreate, TUpdate, TCriteria extends SearchCriteria> {
  protected constructor(public readonly options: IServiceOptions) {}

  abstract create(data: TCreate): Promise<TData>

  abstract update(id: TId, data: TUpdate): Promise<TData>

  destroy(id: TId): Promise<void> {
    return this.destroyAll([id])
  }

  abstract destroyAll(ids: TId[]): Promise<void>

  abstract findById(id: TId): Promise<TData>

  abstract findAndCountAll(criteria: TCriteria): Promise<PageData<TData>>

  protected async getTxRepositoryOptions(): Promise<IRepositoryOptions> {
    const transaction = await SequelizeRepository.createTransaction(this.options)
    const options: IRepositoryOptions = { ...this.options }
    options.transaction = transaction

    return options
  }
}
