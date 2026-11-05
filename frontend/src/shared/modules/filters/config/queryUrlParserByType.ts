import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { booleanQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/boolean.parser';
import { numberQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/number.parser';
import { dateQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/date.parser';
import { selectQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/select.parser';
import { multiSelectQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/multiselect.parser';
import { stringQueryUrlParser } from '@/shared/modules/filters/config/queryUrlParser/string.parser';

export const queryUrlParserByType = {
  [FilterConfigType.BOOLEAN]: booleanQueryUrlParser,
  [FilterConfigType.NUMBER]: numberQueryUrlParser,
  [FilterConfigType.DATE]: dateQueryUrlParser,
  [FilterConfigType.SELECT]: selectQueryUrlParser,
  [FilterConfigType.SELECT_ASYNC]: selectQueryUrlParser,
  [FilterConfigType.MULTISELECT]: multiSelectQueryUrlParser,
  [FilterConfigType.MULTISELECT_ASYNC]: multiSelectQueryUrlParser,
  [FilterConfigType.STRING]: stringQueryUrlParser,
  [FilterConfigType.CUSTOM]: () => null,
} satisfies Record<FilterConfigType, unknown>;
