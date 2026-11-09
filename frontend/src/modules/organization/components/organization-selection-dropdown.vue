<template>
  <div class="pt-16">
    <div class="flex justify-center">
      <lf-icon name="house-building" class="text-gray-200 account-icon text-center h-16 flex items-center" />
    </div>
    <div class="text-gray-600 text-sm text-center py-4">
      Select the company you want to merge with
    </div>
    <div class="flex justify-center">
      <div class="flex w-4/5">
        <app-autocomplete-one-input
          id="searchOrganizations"
          v-model="computedOrganizationToMerge"
          :fetch-fn="fetchFn"
          :disable-option="disableOption"
          placeholder="Type to search companies"
          input-class="w-full"
        >
          <template #option="{ item }">
            <div class="flex items-center">
              <app-avatar
                :entity="{
                  ...item,
                  displayName: item.label,
                  avatar: item.logo,
                }"
                entity-name="organization"
                size="xxs"
                class="mr-3"
              />
              <div class="flex flex-col justify-center">
                <p class="text-xs leading-4.5" v-html="$sanitize(item.label as string)" />
              </div>
            </div>
          </template>
        </app-autocomplete-one-input>
      </div>
    </div>
    <div class="flex justify-center mt-4 text-gray-500 text-2xs">
      <span class="font-semibold pr-1">Note:</span>Active member companies of the Linux Foundation can't be merged into other companies.
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  computed,
  ref,
  onMounted,
} from 'vue';
import AppAutocompleteOneInput from '@/shared/form/autocomplete-one-input.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import { OrganizationService } from '@/modules/organization/services/organization.api.service';
import { useRoute } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '@/modules/organization/types/Organization';

defineOptions({ name: 'AppOrganizationSelectionDropdown' });

const props = defineProps<{
  modelValue: Organization | null;
  primaryOrganization: Organization | null;
  id: string | undefined;
}>();
const emit = defineEmits<{(e: 'update:modelValue', value: Organization): void}>();
const route = useRoute();

const segments = ref<string[]>([]);
const loadingOrganizationToMerge = ref<boolean>();
const computedOrganizationToMerge = computed<Organization | null>({
  get() {
    return props.modelValue;
  },
  async set(value) {
    loadingOrganizationToMerge.value = true;

    const response = await OrganizationService.find(value!.id, segments.value);

    emit('update:modelValue', response);
    loadingOrganizationToMerge.value = false;
  },
});

const fetchFn = async ({ query, limit }: { query: string; limit: number }) => {
  if (!/[\p{L}\p{N}]{3}/u.test(query ?? '')) {
    return [];
  }

  const options = await OrganizationService.listOrganizationsAutocomplete({
    query,
    limit,
    excludeLfMember: true,
    excludeSegments: true,
  });

  // Remove primary organization from organizations that can be merged with
  const filteredOptions: Partial<Organization>[] = options.filter((m: Organization) => m.id !== props.id);

  // If the primary organization was removed, add an empty object in replacement
  if (options.length !== filteredOptions.length) {
    filteredOptions.push({});
  }

  return filteredOptions;
};

onMounted(() => {
  const segmentId = route.query.segmentId || route.query.projectGroup;
  segments.value = segmentId ? [segmentId as string] : [];
});

const disableOption = (option: Record<string, unknown>) => !!option.lfxMembership && !!props.primaryOrganization?.lfxMembership;
</script>

<style lang="scss" scoped>
.account-icon {
  font-size: 64px;
}
#searchOrganizations .el-select-dropdown__item {
  height: auto !important;
}
</style>
