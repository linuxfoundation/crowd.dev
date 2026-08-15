"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CategoryService = void 0;
const common_1 = require("@crowd/common");
const categories_1 = require("@crowd/data-access-layer/src/categories");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class CategoryService extends logging_1.LoggerBase {
    constructor(options) {
        super();
        this.options = options;
    }
    /**
     * Creates a new category group with optional associated categories.
     *
     * @param {ICreateCategoryGroupWithCategories} categoryGroup - An object representing the category group to be created, which may include associated categories.
     * @return {Promise<boolean>} A promise that resolves to true if the category group is successfully created.
     */
    async createCategoryGroup(categoryGroup) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            let slug = (0, common_1.getCleanString)(categoryGroup.name).replace(/\s+/g, '-');
            const categoryGroupsWithSameSlug = await (0, categories_1.listCategoryGroupsBySlug)(qx, slug);
            if (categoryGroupsWithSameSlug.length > 0) {
                slug = `${slug}-${categoryGroupsWithSameSlug.length}`;
            }
            const createdCategoryGroup = await (0, categories_1.createCategoryGroup)(qx, {
                ...categoryGroup,
                slug,
            });
            if (categoryGroup.categories) {
                for (const category of categoryGroup.categories) {
                    let slug = (0, common_1.getCleanString)(category.name).replace(/\s+/g, '-');
                    const categoriesWithSameSlug = await (0, categories_1.listCategoriesBySlug)(qx, slug);
                    if (categoriesWithSameSlug.length > 0) {
                        slug = `${slug}-${categoriesWithSameSlug.length}`;
                    }
                    await (0, categories_1.createCategory)(qx, {
                        ...category,
                        slug,
                        categoryGroupId: createdCategoryGroup.id,
                    });
                }
            }
            return true;
        });
    }
    /**
     * Updates an existing category group with the specified data.
     *
     * @param {string} categoryGroupId - The unique identifier of the category group to update.
     * @param {ICreateCategoryGroup} data - The data to update the category group with.
     * @return {Promise<object>} A promise that resolves to the updated category group object.
     */
    async updateCategoryGroup(categoryGroupId, data) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const currentCategoryGroup = await (0, categories_1.getCategoryGroupById)(qx, categoryGroupId);
            let slug = currentCategoryGroup.slug;
            if (currentCategoryGroup.name !== data.name) {
                slug = (0, common_1.getCleanString)(data.name).replace(/\s+/g, '-');
                const categoryGroupsWithSameSlug = await (0, categories_1.listCategoryGroupsBySlug)(qx, slug);
                if (categoryGroupsWithSameSlug.length > 0) {
                    slug = `${slug}-${categoryGroupsWithSameSlug.length + 1}`;
                }
            }
            await (0, categories_1.updateCategoryGroup)(qx, categoryGroupId, {
                ...data,
                slug,
            });
            if (data.categories) {
                const existingCategories = await (0, categories_1.listGroupListCategories)(qx, [categoryGroupId]);
                const existingCategoryIds = existingCategories.map((category) => category.id);
                const newCategoryIds = data.categories.map((category) => category.id);
                const categoriesToDelete = existingCategoryIds.filter((id) => !newCategoryIds.includes(id));
                const categoriesToCreate = data.categories.filter((category) => !category.id);
                const categoriesToUpdate = data.categories.filter((category) => existingCategoryIds.includes(category.id));
                if (categoriesToDelete.length > 0) {
                    await (0, categories_1.deleteCategories)(qx, categoriesToDelete);
                }
                for (const category of categoriesToCreate) {
                    await this.createCategory({
                        ...category,
                        categoryGroupId,
                    });
                }
                for (const category of categoriesToUpdate) {
                    await this.updateCategory(category.id, {
                        ...category,
                        categoryGroupId,
                    });
                }
            }
            return true;
        });
    }
    /**
     * Deletes a category group by its unique identifier.
     *
     * @param {string} categoryGroupId - The unique identifier of the category group to delete.
     * @return {Promise<any>} A promise that resolves when the category group is successfully deleted.
     */
    async deleteCategoryGroup(categoryGroupId) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            return (0, categories_1.deleteCategoryGroup)(qx, categoryGroupId);
        });
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
    async listCategoryGroups(filters) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        let rows = await (0, categories_1.listCategoryGroups)(qx, filters);
        const count = await (0, categories_1.listCategoryGroupsCount)(qx, filters);
        const categoryGroupIds = rows.map((categoryGroup) => categoryGroup.id);
        const categories = await (0, categories_1.listGroupListCategories)(qx, categoryGroupIds);
        rows = rows.map((row) => ({
            ...row,
            categories: categories.filter((category) => category.categoryGroupId === row.id),
        }));
        return {
            rows,
            count,
            limit: +filters.limit || 20,
            offset: +filters.offset || 0,
        };
    }
    /**
     * Creates a new category with a unique slug. If a category with the same slug already exists,
     * appends a number to the slug to ensure uniqueness.
     *
     * @param {ICreateCategory} category - The category object containing the details for the new category.
     * @return {Promise<object>} A promise that resolves to the created category object.
     */
    async createCategory(category) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            let slug = (0, common_1.getCleanString)(category.name).replace(/\s+/g, '-');
            const categoriesWithSameSlug = await (0, categories_1.listCategoriesBySlug)(qx, slug);
            if (categoriesWithSameSlug.length > 0) {
                slug = `${slug}-${categoriesWithSameSlug.length}`;
            }
            return (0, categories_1.createCategory)(qx, {
                ...category,
                slug,
            });
        });
    }
    /**
     * Updates the details of an existing category, including its name and slug.
     * If the category name has been modified, it generates a new unique slug.
     *
     * @param {string} categoryId - The ID of the category to be updated.
     * @param {ICreateCategory} data - The updated data for the category, including name and other properties.
     * @return {Promise<Object>} A promise that resolves to the updated category details.
     */
    async updateCategory(categoryId, data) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const currentCategory = await (0, categories_1.getCategoryById)(qx, categoryId);
            let slug = currentCategory.slug;
            if (currentCategory.name !== data.name) {
                slug = (0, common_1.getCleanString)(data.name).replace(/\s+/g, '-');
                const categoriesWithSameSlug = await (0, categories_1.listCategoriesBySlug)(qx, slug);
                if (categoriesWithSameSlug.length > 0) {
                    slug = `${slug}-${categoriesWithSameSlug.length + 1}`;
                }
            }
            return (0, categories_1.updateCategory)(qx, categoryId, {
                ...data,
                slug,
            });
        });
    }
    /**
     * Deletes a category based on the provided category ID.
     *
     * @param {string} categoryId - The unique identifier of the category to be deleted.
     * @return {Promise<any>} A promise that resolves when the category is successfully deleted.
     */
    async deleteCategory(categoryId) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            return (0, categories_1.deleteCategory)(qx, categoryId);
        });
    }
    /**
     * Deletes categories based on the provided list of IDs.
     *
     * @param {string[]} ids - An array of category IDs to be deleted.
     * @return {Promise<any>} A promise that resolves with the result of the deletion operation.
     */
    async deleteCategories(ids) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            return (0, categories_1.deleteCategories)(qx, ids);
        });
    }
    async listCategories(filters) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const rows = await (0, categories_1.listCategories)(qx, filters);
        const groupedCategories = rows.reduce((acc, row) => {
            if (!acc[row.categoryGroupId]) {
                acc[row.categoryGroupId] = {
                    id: row.categoryGroupId,
                    name: row.categoryGroupName,
                    categories: [],
                };
            }
            acc[row.categoryGroupId].categories.push({
                id: row.id,
                name: row.name,
            });
            return acc;
        }, {});
        return {
            rows: Object.values(groupedCategories),
            limit: +filters.limit || 20,
            offset: +filters.offset || 0,
        };
    }
}
exports.CategoryService = CategoryService;
//# sourceMappingURL=categoryService.js.map