import authAxios from '@/shared/axios/auth-axios';
import type { SavedViewCreate } from '@/shared/modules/saved-views/types/SavedViewsConfig';

export class SavedViewsService {
  static query(params: any) {
    return authAxios.get(
      '/customview',
      {
        params,
      },
    )
      .then((res) => res.data);
  }

  static create(view: SavedViewCreate) {
    return authAxios.post(
      '/customview',
      view,
    )
      .then((res) => res.data);
  }

  static update(id: string, view: Partial<SavedViewCreate>) {
    return authAxios.put(
      `/customview/${id}`,
      view,
    )
      .then((res) => res.data);
  }

  static updateBulk(views: Partial<SavedViewCreate>[]) {
    return authAxios.patch(
      '/customview',
      views,
    )
      .then((res) => res.data);
  }

  static delete(id: string) {
    return authAxios.delete(
      '/customview',
      {
        params: {
          ids: [id],
        },
      },
    )
      .then((res) => res.data);
  }
}
