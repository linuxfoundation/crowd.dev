import authAxios from '@/shared/axios/auth-axios';
import type {
  CategoryGroup,
  CategoryGroupListFilters,
  CreateCategoryGroup,
} from '@/modules/admin/modules/categories/types/CategoryGroup';
import type { Pagination } from '@/shared/types/Pagination';

export class CategoryGroupService {
  static async create(categoryGroup: CreateCategoryGroup): Promise<boolean> {
    const response = await authAxios.post(
      '/category-group',
      categoryGroup,
    );
    return response.data;
  }

  static async list(filters: CategoryGroupListFilters): Promise<Pagination<CategoryGroup>> {
    const response = await authAxios.get(
      '/category-group',
      {
        params: filters,
      },
    );
    return response.data;
  }

  static async update(categoryGroupId: string, data: Partial<CreateCategoryGroup>): Promise<boolean> {
    const response = await authAxios.patch(
      `/category-group/${categoryGroupId}`,
      data,
    );
    return response.data;
  }

  static async delete(categoryGroupId: string): Promise<boolean> {
    const response = await authAxios.delete(
      `/category-group/${categoryGroupId}`,
    );
    return response.data;
  }
}
