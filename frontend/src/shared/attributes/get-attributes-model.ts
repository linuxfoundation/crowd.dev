import type { ContributorAttribute } from '@/modules/contributor/types/Contributor';

/**
 * Util to transform extract the default property from attributes and create a new object
 */
export default (
  record: { attributes?: Record<string, ContributorAttribute> } | null | undefined,
): Record<string, unknown> => Object.entries(record?.attributes || {}).reduce(
  (obj: Record<string, unknown>, [key, val]: [string, ContributorAttribute]): Record<string, unknown> => {
    if (
      val.default === undefined
        || val.default === null
        || key === 'emails'
    ) {
      return obj;
    }

    return {
      ...obj,
      [key]: val.default,
    };
  },
  {},
);
