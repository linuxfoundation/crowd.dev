<template>
  <section v-bind="$attrs">
    <div class="flex items-center justify-between pb-4">
      <h6>
        Organization details
      </h6>
    </div>
    <div>
      <!-- Enriched attributes -->
      <article
        v-for="attribute in [...visibleAttributes, ...nonConfigAttributes]"
        :key="attribute.name"
        class="border-b border-gray-100 flex py-4"
      >
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            {{ attribute.label }}
          </p>
          <lf-organization-attribute-source :values="props.organization.attributes[attribute.name]" />
        </div>
        <div class="w-7/12 pr-1">
          <component
            :is="attribute.component"
            v-if="attribute.component"
            :data="getValue(attribute)"
            v-bind="attribute.attributes"
          />
          <lf-organization-attribute-array
            v-else-if="attribute.type === AttributeType.ARRAY"
            :data="getValue(attribute)"
            v-bind="attribute.attributes"
          />
          <lf-organization-attribute-json
            v-else-if="attribute.type === AttributeType.JSON"
            :data="getValue(attribute)"
            v-bind="attribute.attributes"
          />
          <lf-organization-attribute-string
            v-else
            :data="`${getValue(attribute)}`"
          />
        </div>
      </article>
      <article
        v-if="affiliated.length > 0"
        class="border-b border-gray-100 flex py-4"
      >
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Affiliated profiles
          </p>
        </div>
        <div class="w-7/12 pr-1">
          <lf-organization-attribute-array
            :data="affiliated.map((a) => a.value)"
          />
        </div>
      </article>

      <div v-if="Object.keys(visibleAttributes).length === 0" class="pt-2 flex flex-col items-center w-full">
        <lf-icon name="rectangle-list" :size="80" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No organization details yet
        </p>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Organization } from '@/modules/organization/types/Organization';
import LfOrganizationAttributeString
  from '@/modules/organization/components/details/overview/attributes/organization-attribute-string.vue';
import type { OrganizationEnrichmentConfig } from '@/modules/organization/config/enrichment';
import enrichmentAttributes from '@/modules/organization/config/enrichment';
import { AttributeType } from '@/modules/organization/types/Attributes';
import LfOrganizationAttributeArray
  from '@/modules/organization/components/details/overview/attributes/organization-attribute-array.vue';
import LfOrganizationAttributeJson
  from '@/modules/organization/components/details/overview/attributes/organization-attribute-json.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfOrganizationAttributeSource
  from '@/modules/organization/components/details/overview/attributes/organization-attribute-source.vue';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';

const props = defineProps<{
  organization: Organization,
}>();

const ignoreAttributes = [
  'logo',
  'name',
  'email',
  'phoneNumber',
];

const { affiliatedProfiles } = useOrganizationHelpers();

const camelCaseToLabel = (attribute: string) => attribute
  ?.replace(/([A-Z])/g, ' $1')
  ?.replace(/^./, (str) => str.toUpperCase()) || attribute;

const getAttributeType = (attribute: Record<string, any>) => {
  if (attribute.default?.constructor === Array || (!attribute.default && Object.values(attribute)?.[0].constructor === Array)) {
    return AttributeType.ARRAY;
  }

  // Check if its a json object
  if (attribute.default.charAt(0) === '{' && attribute.default.charAt(-1) === '}') {
    return AttributeType.JSON;
  }

  return AttributeType.STRING;
};

const getAttributeValue = (attribute: Record<string, any>, type: AttributeType) => {
  if (type === AttributeType.ARRAY) {
    return attribute.default || [...new Set(Object.values(attribute).reduce((arr, val) => [...arr, ...val], []))];
  }

  return attribute.default;
};

const visibleAttributes = computed(() => enrichmentAttributes
  .filter((a) => ((props.organization.attributes[a.name]?.default && a.type !== AttributeType.ARRAY && a.type !== AttributeType.JSON)
          || (a.type === AttributeType.ARRAY && props.organization.attributes[a.name]?.default?.length)
          || (a.type === AttributeType.JSON && props.organization.attributes[a.name]?.default
          && Object.keys(props.organization.attributes[a.name]?.default).length)) && a.showInAttributes));

const nonConfigAttributes = computed(() => Object.keys(props.organization.attributes)
  .filter((a: string) => !enrichmentAttributes.find((ea) => ea.name === a) && !ignoreAttributes.includes(a))
  .map((a: string): OrganizationEnrichmentConfig => {
    const type = getAttributeType(props.organization.attributes[a]);
    return {
      name: a,
      label: camelCaseToLabel(a),
      type,
      showInAttributes: true,
      showInForm: false,
      formatValue: () => getAttributeValue(props.organization.attributes[a], type),
    };
  }));

const getValue = (attribute: OrganizationEnrichmentConfig) => {
  let value = props.organization.attributes[attribute.name]?.default;
  if (attribute.type === AttributeType.JSON) {
    value = JSON.parse(value);
  }
  if (attribute.formatValue) {
    return attribute.formatValue(value);
  }
  return value;
};

const affiliated = computed(() => affiliatedProfiles(props.organization));
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsAttributes',
};
</script>
