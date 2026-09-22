import { QueryExecutor } from '../queryExecutor'

export enum CategoryGroupType {
  HORIZONTAL = 'horizontal',
  VERTICAL = 'vertical',
}

export interface ICreateCategoryGroup {
  name: string
  type: CategoryGroupType
  slug: string
}

export interface ICategoryGroup {
  id: string
  name: string
  type: CategoryGroupType
  slug: string
  createdAt: string
  updatedAt: string
  categories?: ICategory[]
}

export interface ICategory {
  id: string
  name: string
  slug: string
  categoryGroupId: string
  createdAt: string
  updatedAt: string
}

export interface ICreateCategory {
  name: string
  slug: string
  categoryGroupId?: string
}

export interface ICreateCategoryGroupWithCategories extends ICreateCategoryGroup {
  categories?: ICategory[]
}

export interface ICategoryGroupsFilters {
  type?: CategoryGroupType
  query?: string
  limit: number
  offset: number
}

export interface ICategoryFilters {
  groupType?: CategoryGroupType
  query?: string
  limit: number
  offset: number
}

/**
 * Creates a new category group in the database.
 *
 * @param {QueryExecutor} qx - The query executor used to interact with the database.
 * @param {ICreateCategoryGroup} categoryGroup - The category group data to be inserted, including name, type, and slug.
 * @return {Promise<ICategoryGroup>} A promise that resolves to the created category group object.
 */
export async function createCategoryGroup(
  qx: QueryExecutor,
  categoryGroup: ICreateCategoryGroup,
): Promise<ICategoryGroup> {
  return qx.selectOne(
    `
            INSERT INTO "categoryGroups" (name, type, slug)
            VALUES ($(name), $(type), $(slug))
            RETURNING *
        `,
    categoryGroup,
  )
}

/**
 * Associates multiple categories with a specified category group.
 *
 * @param {QueryExecutor} qx - The query executor instance used to run the database queries.
 * @param {string} categoryGroupId - The ID of the category group to which the categories will be linked.
 * @param {string[]} categoryIds - An array of category IDs that will be linked to the category group.
 * @return {Promise<void>} A promise that resolves when the operation is complete.
 */
export async function connectCategoriesToCategoryGroup(
  qx: QueryExecutor,
  categoryGroupId: string,
  categoryIds: string[],
): Promise<void> {
  await qx.result(
    `
      UPDATE "categories"
      SET "categoryGroupId" = $(categoryGroupId)
      WHERE id = ANY($(categoryIds)::uuid[])
    `,
    {
      categoryGroupId,
      categoryIds,
    },
  )
}

/**
 * Retrieves a list of category groups based on the provided filters.
 *
 * @param {QueryExecutor} qx - The query executor used to run database queries.
 * @param {ICategoryGroupsFilters} filters - The filters to apply to the query. Includes:
 *   - type: Optional filter by the type of category group.
 *   - query: Optional search query to match category group names (case-insensitive).
 *   - limit: Optional maximum number of results to return (default: 20).
 *   - offset: Optional number of results to skip for pagination (default: 0).
 * @return {Promise<ICategoryGroup[]>} A promise that resolves to an array of category groups matching the filters.
 */
export async function listCategoryGroups(
  qx: QueryExecutor,
  filters: ICategoryGroupsFilters,
): Promise<ICategoryGroup[]> {
  return qx.select(
    `
            SELECT id, name, type
            FROM "categoryGroups"
            WHERE COALESCE($(type), type) = type
              AND ($(query) = '' OR name ILIKE $(searchPattern))
              AND "deletedAt" IS NULL
            ORDER BY name
            LIMIT $(limit) OFFSET $(offset)
        `,
    {
      type: filters.type || null,
      query: filters.query || '',
      searchPattern: `%${filters.query || ''}%`,
      limit: filters.limit || 20,
      offset: filters.offset || 0,
    },
  )
}

/**
 * Retrieves a list of category details based on the provided category group IDs.
 *
 * @param {QueryExecutor} qx - The query executor used to interact with the database.
 * @param {string[]} categoryGroupIds - An array of category group IDs to filter categories.
 * @return {Promise<ICategory[]>} A promise that resolves to an array of category objects containing id, name, slug, and categoryGroupId.
 */
export async function listGroupListCategories(
  qx: QueryExecutor,
  categoryGroupIds: string[],
): Promise<ICategory[]> {
  return qx.select(
    `
            SELECT id, name, slug, "categoryGroupId"
            FROM "categories"
            WHERE "categoryGroupId" = ANY($(categoryGroupIds)::uuid[])
              AND "deletedAt" IS NULL
            ORDER BY "categoryGroupId"
        `,
    {
      categoryGroupIds,
    },
  )
}

/**
 * Retrieves the count of category groups based on the provided filters.
 *
 * @param {QueryExecutor} qx - The query executor used to interact with the database.
 * @param {ICategoryGroupsFilters} filters - Filters for querying category groups. Contains properties such as `type` and `query`.
 * @return {Promise<number>} A promise that resolves to the count of category groups matching the given filters.
 */
export async function listCategoryGroupsCount(
  qx: QueryExecutor,
  filters: ICategoryGroupsFilters,
): Promise<number> {
  const result = await qx.selectOne(
    `
            SELECT COUNT(*) as count
            FROM "categoryGroups"
            WHERE COALESCE($(type), type) = type
              AND ($(query) = '' OR name ILIKE $(searchPattern))
              AND "deletedAt" IS NULL
        `,
    {
      type: filters.type || null,
      query: filters.query || '',
      searchPattern: `%${filters.query || ''}%`,
    },
  )
  return parseInt(result.count, 10)
}

/**
 * Fetches a list of category groups based on the provided slug.
 * The method retrieves category groups where the slug matches exactly
 * or follows a specific pattern defined by the slug.
 *
 * @param {QueryExecutor} qx - The query executor to interact with the database.
 * @param {string} slug - The slug used to filter the category groups.
 * @return {Promise<ICategoryGroup[]>} A promise that resolves to an array of category groups.
 */
export async function listCategoryGroupsBySlug(
  qx: QueryExecutor,
  slug: string,
): Promise<ICategoryGroup[]> {
  return qx.select(
    `
          SELECT slug
          FROM "categoryGroups"
          WHERE 
          "deletedAt" IS NULL
          AND (slug = $(slug)
             OR slug ~ $(slugRegex))

      `,
    {
      slug,
      slugRegex: `^${slug}-[0-9]+$`,
    },
  )
}

/**
 * Fetches a category group by its unique identifier.
 *
 * @param {QueryExecutor} qx - The query executor used to execute database queries.
 * @param {string} id - The unique identifier of the category group to retrieve.
 * @return {Promise<ICategoryGroup>} A promise that resolves to the category group object, including its id, slug, and name.
 */
export async function getCategoryGroupById(qx: QueryExecutor, id: string): Promise<ICategoryGroup> {
  return qx.selectOne(
    `
          SELECT id, slug, name
          FROM "categoryGroups"
          WHERE id = $(id)
          AND "deletedAt" IS NULL
      `,
    {
      id,
    },
  )
}

/**
 * Updates an existing category group in the database with the provided data.
 *
 * @param {QueryExecutor} qx - The query executor instance used to interact with the database.
 * @param {string} categoryGroupId - The unique identifier of the category group to update.
 * @param {Partial<ICreateCategoryGroup>} data - The updated data for the category group, which may include name, type, and slug.
 * @return {Promise<ICategoryGroup>} A promise that resolves to the updated category group object.
 */
export async function updateCategoryGroup(
  qx: QueryExecutor,
  categoryGroupId: string,
  data: Partial<ICreateCategoryGroup>,
): Promise<ICategoryGroup> {
  return qx.selectOne(
    `
            UPDATE "categoryGroups"
            SET
                name = COALESCE($(name), name),
                type = COALESCE($(type), type),
                slug = COALESCE($(slug), slug),
                "updatedAt" = NOW()
            WHERE id = $(categoryGroupId)
            RETURNING *
        `,
    {
      categoryGroupId,
      name: data.name,
      type: data.type,
      slug: data.slug,
    },
  )
}

/**
 * Deletes a category group by its ID and returns the deleted category group object.
 *
 * @param {QueryExecutor} qx - The query executor used to perform database operations.
 * @param {string} categoryGroupId - The ID of the category group to be deleted.
 * @return {Promise<ICategoryGroup>} A promise that resolves to the deleted category group object.
 */
export async function deleteCategoryGroup(
  qx: QueryExecutor,
  categoryGroupId: string,
): Promise<ICategoryGroup> {
  return qx.selectOne(
    `
        UPDATE "categoryGroups"
        SET "deletedAt" = NOW(),
            "updatedAt" = NOW()
        WHERE id = $(categoryGroupId)
        RETURNING *
    `,
    {
      categoryGroupId,
    },
  )
}

/**
 * Retrieves a category by its unique identifier from the database.
 *
 * @param {QueryExecutor} qx - The query executor used to run the database query.
 * @param {string} id - The unique identifier of the category to retrieve.
 * @return {Promise<ICategory>} A promise that resolves to the category object with the specified ID.
 */
export async function getCategoryById(qx: QueryExecutor, id: string): Promise<ICategory> {
  return qx.selectOne(
    `
          SELECT id, slug, name
          FROM "categories"
          WHERE id = $(id)
          AND "deletedAt" IS NULL
      `,
    {
      id,
    },
  )
}

/**
 * Retrieves a list of category slugs that match a given slug or a slug pattern.
 *
 * @param {QueryExecutor} qx - The query executor to interact with the database.
 * @param {string} slug - The slug to match against or use as the base for matching slug patterns.
 * @return {Promise<ICategory[]>} A promise that resolves to an array of matching category slugs.
 */
export async function listCategoriesBySlug(qx: QueryExecutor, slug: string): Promise<ICategory[]> {
  return qx.select(
    `
          SELECT slug
          FROM "categories"
          WHERE 
          "deletedAt" IS NULL
          AND (slug = $(slug)
             OR slug ~ $(slugRegex))
      `,
    {
      slug,
      slugRegex: `^${slug}-[0-9]+$`,
    },
  )
}

/**
 * Creates a new category in the database.
 *
 * @param {QueryExecutor} qx - The query executor used to interact with the database.
 * @param {ICreateCategory} category - The category data to be inserted, including name, slug, and optional category group ID.
 * @return {Promise<ICategory>} A promise that resolves to the newly created category object.
 */
export async function createCategory(
  qx: QueryExecutor,
  category: ICreateCategory,
): Promise<ICategory> {
  return qx.selectOne(
    `
            INSERT INTO "categories" (name, slug, "categoryGroupId")
            VALUES ($(name), $(slug), $(categoryGroupId))
            RETURNING *
        `,
    {
      name: category.name,
      slug: category.slug,
      categoryGroupId: category.categoryGroupId || null,
    },
  )
}

/**
 * Updates the details of a category with the specified categoryId. Allows partial updates
 * for fields such as name, slug, and categoryGroupId. Updates the "updatedAt" timestamp
 * to the current date and time.
 *
 * @param {QueryExecutor} qx - The query executor responsible for interacting with the database.
 * @param {string} categoryId - The unique identifier of the category to be updated.
 * @param {Partial<ICreateCategory>} data - An object containing the category fields to be updated.
 *                                          Supported fields include name, slug, and categoryGroupId.
 * @return {Promise<ICategoryGroup>} A promise that resolves to the updated category details.
 */
export async function updateCategory(
  qx: QueryExecutor,
  categoryId: string,
  data: Partial<ICreateCategory>,
): Promise<ICategoryGroup> {
  return qx.selectOne(
    `
            UPDATE "categories"
            SET
                name = COALESCE($(name), name),
                slug = COALESCE($(slug), slug),
                "categoryGroupId" = COALESCE($(categoryGroupId), "categoryGroupId"),
                "updatedAt" = NOW()
            WHERE id = $(categoryId)
            RETURNING *
        `,
    {
      categoryId,
      name: data.name,
      slug: data.slug,
      categoryGroupId: data.categoryGroupId,
    },
  )
}

/**
 * Deletes a category from the database with the specified category ID and returns the deleted category.
 *
 * @param {QueryExecutor} qx - The query executor used to perform database operations.
 * @param {string} categoryId - The unique identifier of the category to be deleted.
 * @return {Promise<ICategory>} A promise that resolves to the deleted category object.
 */
export async function deleteCategory(qx: QueryExecutor, categoryId: string): Promise<ICategory> {
  return qx.selectOne(
    `
      UPDATE "categories"
      SET "deletedAt" = NOW(),
          "updatedAt" = NOW()
      WHERE id = $(categoryId)
      RETURNING *
    `,
    {
      categoryId,
    },
  )
}

/**
 * Deletes categories from the database based on the provided category IDs.
 *
 * @param {QueryExecutor} qx - The query executor used to interact with the database.
 * @param {string[]} ids - An array of category IDs to be deleted.
 * @return {Promise<ICategory[]>} A promise that resolves to an array of the deleted categories.
 */
export async function deleteCategories(qx: QueryExecutor, ids: string[]): Promise<ICategory[]> {
  return qx.select(
    `
      UPDATE "categories"
      SET "deletedAt" = NOW(),
          "updatedAt" = NOW()
      WHERE id = ANY($(ids)::uuid[])
      RETURNING *
    `,
    {
      ids,
    },
  )
}

/**
 * Retrieves a list of categories based on the provided filters.
 *
 * @param {QueryExecutor} qx - The query executor used to perform the database query.
 * @param {ICategoryFilters} filters - An object containing filtering and pagination options.
 * Filters include:
 * - query: A string to search for categories by name.
 * - limit: The maximum number of categories to retrieve.
 * - offset: The number of categories to skip before starting retrieval.
 * - groupType: A filter to include only categories belonging to a specific group type.
 *
 * @return {Promise<Object[]>} A promise that resolves to an array of category objects,
 * each containing:
 * - id: The unique identifier of the category.
 * - name: The name of the category.
 * - categoryGroupId: The unique identifier of the category group.
 * - categoryGroupName: The name of the category group.
 */
export async function listCategories(
  qx: QueryExecutor,
  filters: ICategoryFilters,
): Promise<
  {
    id: string
    name: string
    categoryGroupId: string
    categoryGroupName: string
  }[]
> {
  return qx.select(
    `          SELECT c.id, c.name, cg.id as "categoryGroupId", cg.name as "categoryGroupName", cg.type as "categoryGroupType"
                   FROM "categories" c
                            JOIN "categoryGroups" cg ON c."categoryGroupId" = cg.id
                   WHERE 
                   c."deletedAt" IS NULL 
                   AND cg."deletedAt" IS NULL
                   AND c.name ILIKE $(query)
                     AND COALESCE($(groupType), cg.type) = cg.type
                   ORDER BY cg.name
                    LIMIT $(limit)
          OFFSET $(offset)
        `,
    {
      query: `%${filters.query || ''}%`,
      limit: filters.limit || 20,
      offset: filters.offset || 0,
      groupType: filters.groupType || null,
    },
  )
}

/**
 * Fetches a list of categories based on their IDs.
 *
 * @param {QueryExecutor} qx - The query executor for performing database operations.
 * @param {string[]} ids - Array of category IDs to fetch.
 * @return {Promise<ICategory[]>} A promise that resolves to an array of category objects.
 */
export async function listCategoriesByIds(qx: QueryExecutor, ids: string[]): Promise<ICategory[]> {
  return qx.select(
    `         
    SELECT c.id, c.name, c.slug, c."categoryGroupId", cg.name as "categoryGroupName", cg.type as "categoryGroupType"
    FROM categories c
             JOIN "categoryGroups" cg ON c."categoryGroupId" = cg.id
    WHERE c."deletedAt" IS NULL AND c.id = ANY($(ids)::uuid[])
        `,
    {
      ids,
    },
  )
}
