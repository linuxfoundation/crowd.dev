<template>
  <lf-dropdown v-bind="$attrs" width="232px">
    <template #trigger>
      <slot />
    </template>
    <div class="max-h-60 overflow-y-scroll -m-2 p-2 pt-0">
      <div class="-mx-2 border-b border-gray-100 mb-2 sticky top-0" @click.stop>
        <lf-input
          v-model="search"
          class="!border-0 !shadow-none h-12"
          placeholder="Search..."
        >
          <template #suffix>
            <lf-icon
              v-if="search.length"
              name="circle-xmark"
              type="regular"
              :size="16"
              class="text-gray-400 cursor-pointer"
              @click="search = ''"
            />
          </template>
        </lf-input>
      </div>
      <lf-dropdown-item
        v-for="platform in platforms"
        :key="platform.key"
        @click="emit('add', {
          platform: platform.key,
        })"
      >
        <div class="w-full flex items-center gap-2">
          <img :src="platform.image" :alt="platform.key" class="h-4 min-w-4 object-contain" /> {{ platform.name || platform.key }}
        </div>
      </lf-dropdown-item>
      <template v-if="showEmail">
        <lf-dropdown-separator v-if="platforms.length" />
        <lf-dropdown-item
          @click="emit('add', {
            platform: 'custom',
            type: 'email',
          })"
        >
          <div class="w-full flex items-center gap-2">
            <lf-icon name="envelope" type="regular" :size="16" /> Email
          </div>
        </lf-dropdown-item>
      </template>
      <div v-if="!platforms.length && !showEmail" class="p-2 text-sm italic text-gray-400">
        No platforms found
      </div>
    </div>
  </lf-dropdown>
</template>

<script setup lang="ts">
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import LfInput from '@/ui-kit/input/Input.vue';
import { computed, ref } from 'vue';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';

const emit = defineEmits<{(e: 'add', value: Partial<ContributorIdentity>): void}>();

const { memberIdentities } = useIdentitiesHelpers();

const search = ref<string>('');

const platforms = computed(() => memberIdentities.filter((i) => {
  if (!search.value) return true;
  return i.name.toLowerCase().includes(search.value.toLowerCase()) || i.key.toLowerCase().includes(search.value.toLowerCase());
}));

const showEmail = computed(() => 'email'.includes(search.value.toLowerCase()));
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsIdentityAddDropdown',
};
</script>
