// Filter DSL object sent to the API, e.g. { platform: { in: [...] } }
export type ApiFilter = Record<string, unknown>;

export interface FilterQuery {
  search: string;
  filter: ApiFilter;
  body: ApiFilter;
  orderBy: string;
}
