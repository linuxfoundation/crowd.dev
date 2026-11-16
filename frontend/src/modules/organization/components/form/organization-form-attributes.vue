<template>
  <div class="grid gap-x-12 grid-cols-3 mb-16">
    <div v-if="showHeader">
      <h6>Attributes</h6>
      <p class="text-gray-500 text-2xs leading-normal mt-1">
        Data points to enhance the company profile
      </p>
    </div>
    <div :class="showHeader ? 'col-span-2' : 'col-span-3'">
      <div class="flex gap-3 border-b h-8 items-center">
        <span
          class="uppercase text-gray-400 text-2xs font-semibold tracking-wide w-1/3"
        >Name</span>
        <span
          class="uppercase text-gray-400 text-2xs font-semibold tracking-wide grow"
        >Value</span>
      </div>
      <div
        class="custom-attributes-form flex mt-4 mb-2 flex-col gap-4"
      >
        <app-organization-form-item
          v-for="attribute in visibleAttributes"
          :key="attribute.name"
          :type="attribute.type"
          :label="attribute.label"
          is-enrichment-field
        >
          <app-autocomplete-many-input
            v-if="attribute.type === AttributeType.ARRAY"
            v-model="model[attribute.name]"
            disabled
            input-class="w-full multi-select-field"
            placeholder=" "
            collapse-tags
          />
          <app-organization-form-json
            v-else-if="attribute.type === AttributeType.JSON"
            v-model="model[attribute.name]"
            :type="attribute.type"
          />
          <el-input
            v-else
            v-model="model[attribute.name]"
            :type="attribute.type"
            disabled
            clearable
          /><template #error>
            <div class="el-form-item__error">
              Value is required
            </div>
          </template>
        </app-organization-form-item>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { AttributeType } from '@/modules/organization/types/Attributes';
import enrichmentAttributes from '@/modules/organization/config/enrichment';
import type { Organization } from '../../types/Organization';
import AppOrganizationFormItem from './organization-form-item.vue';
import AppOrganizationFormJson from './organization-form-json.vue';

defineOptions({
  name: 'AppOrganizationFormAttributes',
});

const props = withDefaults(defineProps<{
  showHeader?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- shortcut: untyped enrichment values. revisit: per-attribute value types
  modelValue: Record<string, any>;
  organization: Organization;
}>(), {
  showHeader: true,
});

const model = computed(() => props.modelValue);
const visibleAttributes = computed(() => enrichmentAttributes.filter((a) => a.showInForm));
</script>
