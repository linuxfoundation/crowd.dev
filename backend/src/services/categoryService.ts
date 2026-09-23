import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { getCleanString } from '@crowd/common'
import {
  ICategoryFilters,
  ICategoryGroupsFilters,
  ICreateCategory,
  ICreateCategoryGroupWithCategories,
  createCategory,
  createCategoryGroup,
  deleteCategories,
  deleteCategory,
  deleteCategoryGroup,
  getCategoryById,
  getCategoryGroupById,
  listCategories,
  listCategoriesBySlug,
  listCategoryGroups,
  listCategoryGroupsBySlug,
  listCategoryGroupsCount,
  listGroupListCategories,
  updateCategory,
  updateCategoryGroup,
} from '@crowd/data-access-layer/src/categories'
import { LoggerBase } from '@crowd/logging'

import { IServiceOptions } from './IServiceOptions'

export class CategoryService extends LoggerBase {
  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super()
    this.options = options
  }

  /**
   * Creates a new category group with optional associated categories.
   *
   * @param {ICreateCategoryGroupWithCategories} categoryGroup - An object representing the category group to be created, which may include associated categories.
   * @return {Promise<boolean>} A promise that resolves to true if the category group is successfully created.
   */
  async createCategoryGroup(categoryGroup: ICreateCategoryGroupWithCategories) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      let slug = getCleanString(categoryGroup.name).replace(/\s+/g, '-')

      const categoryGroupsWithSameSlug = await listCategoryGroupsBySlug(qx, slug)

      if (categoryGroupsWithSameSlug.length > 0) {
        slug = `${slug}-${categoryGroupsWithSameSlug.length}`
      }

      const createdCategoryGroup = await createCategoryGroup(qx, {
        ...categoryGroup,
        slug,
      })

      if (categoryGroup.categories) {
        for (const category of categoryGroup.categories) {
          let slug = getCleanString(category.name).replace(/\s+/g, '-')

          const categoriesWithSameSlug = await listCategoriesBySlug(qx, slug)

          if (categoriesWithSameSlug.length > 0) {
            slug = `${slug}-${categoriesWithSameSlug.length}`
          }

          await createCategory(qx, {
            ...category,
            slug,
            categoryGroupId: createdCategoryGroup.id,
          })
        }
      }
      return true
    })
  }

  /**
   * Updates an existing category group with the specified data.
   *
   * @param {string} categoryGroupId - The unique identifier of the category group to update.
   * @param {ICreateCategoryGroup} data - The data to update the category group with.
   * @return {Promise<object>} A promise that resolves to the updated category group object.
   */
  async updateCategoryGroup(categoryGroupId: string, data: ICreateCategoryGroupWithCategories) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      const currentCategoryGroup = await getCategoryGroupById(qx, categoryGroupId)

      let slug = currentCategoryGroup.slug

      if (currentCategoryGroup.name !== data.name) {
        slug = getCleanString(data.name).replace(/\s+/g, '-')

        const categoryGroupsWithSameSlug = await listCategoryGroupsBySlug(qx, slug)

        if (categoryGroupsWithSameSlug.length > 0) {
          slug = `${slug}-${categoryGroupsWithSameSlug.length + 1}`
        }
      }

      await updateCategoryGroup(qx, categoryGroupId, {
        ...data,
        slug,
      })

      if (data.categories) {
        const existingCategories = await listGroupListCategories(qx, [categoryGroupId])
        const existingCategoryIds = existingCategories.map((category) => category.id)
        const newCategoryIds = data.categories.map((category) => category.id)
        const categoriesToDelete = existingCategoryIds.filter((id) => !newCategoryIds.includes(id))
        const categoriesToCreate = data.categories.filter((category) => !category.id)
        const categoriesToUpdate = data.categories.filter((category) =>
          existingCategoryIds.includes(category.id),
        )

        if (categoriesToDelete.length > 0) {
          await deleteCategories(qx, categoriesToDelete)
        }
        for (const category of categoriesToCreate) {
          await this.createCategory({
            ...category,
            categoryGroupId,
          })
        }
        for (const category of categoriesToUpdate) {
          await this.updateCategory(category.id, {
            ...category,
            categoryGroupId,
          })
        }
      }

      return true
    })
  }

  /**
   * Deletes a category group by its unique identifier.
   *
   * @param {string} categoryGroupId - The unique identifier of the category group to delete.
   * @return {Promise<any>} A promise that resolves when the category group is successfully deleted.
   */
  async deleteCategoryGroup(categoryGroupId: string) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      return deleteCategoryGroup(qx, categoryGroupId)
    })
  }

  /**
   * Retrieves a list of category groups based on the provided filters.
   *
   * @param {ICategoryGroupsFilters} filters - The filters used to query category groups. Includes options like limit, offset, and other criteria.
   * @return {Promise<{rows: Array<Object>, count: number, limit: number, offset: number}>} A promise that resolves to an object containing:
   *         - rows: An array of category group objects with their associated categories.
   *         - count: The total number of category groups matching the filters.
   *         - limit: The number of category groups returned in the current batch.
   *         - offset: The starting point for the current batch of category groups.
   */
  async listCategoryGroups(filters: ICategoryGroupsFilters) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    let rows = await listCategoryGroups(qx, filters)
    const count = await listCategoryGroupsCount(qx, filters)

    const categoryGroupIds = rows.map((categoryGroup) => categoryGroup.id)

    const categories = await listGroupListCategories(qx, categoryGroupIds)

    rows = rows.map((row) => ({
      ...row,
      categories: categories.filter((category) => category.categoryGroupId === row.id),
    }))

    return {
      rows,
      count,
      limit: +filters.limit || 20,
      offset: +filters.offset || 0,
    }
  }

  /**
   * Creates a new category with a unique slug. If a category with the same slug already exists,
   * appends a number to the slug to ensure uniqueness.
   *
   * @param {ICreateCategory} category - The category object containing the details for the new category.
   * @return {Promise<object>} A promise that resolves to the created category object.
   */
  async createCategory(category: ICreateCategory) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      let slug = getCleanString(category.name).replace(/\s+/g, '-')

      const categoriesWithSameSlug = await listCategoriesBySlug(qx, slug)

      if (categoriesWithSameSlug.length > 0) {
        slug = `${slug}-${categoriesWithSameSlug.length}`
      }

      return createCategory(qx, {
        ...category,
        slug,
      })
    })
  }

  /**
   * Updates the details of an existing category, including its name and slug.
   * If the category name has been modified, it generates a new unique slug.
   *
   * @param {string} categoryId - The ID of the category to be updated.
   * @param {ICreateCategory} data - The updated data for the category, including name and other properties.
   * @return {Promise<Object>} A promise that resolves to the updated category details.
   */
  async updateCategory(categoryId: string, data: ICreateCategory) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      const currentCategory = await getCategoryById(qx, categoryId)

      let slug = currentCategory.slug

      if (currentCategory.name !== data.name) {
        slug = getCleanString(data.name).replace(/\s+/g, '-')

        const categoriesWithSameSlug = await listCategoriesBySlug(qx, slug)

        if (categoriesWithSameSlug.length > 0) {
          slug = `${slug}-${categoriesWithSameSlug.length + 1}`
        }
      }

      return updateCategory(qx, categoryId, {
        ...data,
        slug,
      })
    })
  }

  /**
   * Deletes a category based on the provided category ID.
   *
   * @param {string} categoryId - The unique identifier of the category to be deleted.
   * @return {Promise<any>} A promise that resolves when the category is successfully deleted.
   */
  async deleteCategory(categoryId: string) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      return deleteCategory(qx, categoryId)
    })
  }

  /**
   * Deletes categories based on the provided list of IDs.
   *
   * @param {string[]} ids - An array of category IDs to be deleted.
   * @return {Promise<any>} A promise that resolves with the result of the deletion operation.
   */
  async deleteCategories(ids: string[]) {
    return SequelizeRepository.withTx(this.options, async (tx) => {
      const qx = SequelizeRepository.getQueryExecutor({ ...this.options, transaction: tx })

      return deleteCategories(qx, ids)
    })
  }

  async listCategories(filters: ICategoryFilters) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    const rows = await listCategories(qx, filters)

    const groupedCategories = rows.reduce((acc, row) => {
      if (!acc[row.categoryGroupId]) {
        acc[row.categoryGroupId] = {
          id: row.categoryGroupId,
          name: row.categoryGroupName,
          categories: [],
        }
      }
      acc[row.categoryGroupId].categories.push({
        id: row.id,
        name: row.name,
      })
      return acc
    }, {})

    return {
      rows: Object.values(groupedCategories),
      limit: +filters.limit || 20,
      offset: +filters.offset || 0,
    }
  }
}
