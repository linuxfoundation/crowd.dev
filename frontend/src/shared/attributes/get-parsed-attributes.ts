import type { ContributorAttribute } from '@/modules/contributor/types/Contributor';
import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';

interface AttributesFormModel {
  attributes: Record<string, ContributorAttribute>;
  [key: string]: unknown;
}

type ParsedAttributes = Record<string, ContributorAttribute | undefined>;

/**
 * Util to transform the attributes form model into the object model for the request
 */
export default (
  attributes: Pick<FilterCustomAttribute, 'name'>[],
  model: AttributesFormModel,
): ParsedAttributes => attributes.reduce(
  (obj: ParsedAttributes, attribute: Pick<FilterCustomAttribute, 'name'>): ParsedAttributes => {
    if (model[attribute.name]) {
      return {
        ...obj,
        [attribute.name]: {
          ...model.attributes[attribute.name],
          ...(
            model[attribute.name] !== model.attributes[attribute.name]?.default
            && {
              custom: model[attribute.name],
              default: model[attribute.name],
            }),
        },
      };
    }

    return {
      ...obj,
      ...model.attributes[attribute.name] && { [attribute.name]: undefined },
    };
  },
  {
    ...model.attributes,
  },
);
