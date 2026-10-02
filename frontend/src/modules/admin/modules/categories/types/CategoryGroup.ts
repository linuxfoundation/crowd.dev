import type { Category } from '@/modules/admin/modules/categories/types/Category';

export enum CategoryGroupType {
  HORIZONTAL = 'horizontal',
    VERTICAL = 'vertical',
}

export interface CategoryGroup {
  id: string;
  name: string;
  slug: string;
  type: CategoryGroupType;
  categories: Category[];
}

export interface CategoryGroupListFilters {
  type?: string;
  query?: string;
  offset?: number;
  limit?: number;
}

export interface CreateCategoryGroup {
  name: string;
  type: CategoryGroupType;
  categories?: Category[];
}
