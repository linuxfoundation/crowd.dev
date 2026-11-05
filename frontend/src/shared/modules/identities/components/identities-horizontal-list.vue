<template>
  <el-popover
    v-if="hasIdentities"
    v-bind="$attrs"
    trigger="hover"
    placement="top"
    width="20rem"
    popper-class="!px-0 !py-4 !shadow !rounded-lg"
  >
    <template #reference>
      <slot name="badge">
        <div
          class="flex gap-1.5 items-center bg-white border border-gray-200
        hover:bg-gray-50 rounded-md h-6 px-1.5 relative cursor-auto group whitespace-nowrap"
        >
          <div class="text-xs text-black">
            {{ pluralize("identity", parsedIdentities.identitiesLength, true) }}
          </div>
          <div
            v-for="[platform, value] of Object.entries(parsedIdentities.visible)"
            :key="platform"
          >
            <app-platform
              v-if="value.length"
              :platform="platform"
              :identities="value"
              :as-svg="asSvg"
            />
          </div>
          <div
            v-if="
              parsedIdentities.platformsLength
                >= (limit || parsedIdentities.platformsLength)
            "
            class="absolute right-2.5 w-10 h-5 bg-gradient-to-r from-transparent to-white group-hover:to-gray-50"
          />
        </div>
      </slot>
    </template>

    <div class="overflow-y-auto overflow-x-hidden max-h-100">
      <div class="text-xs text-gray-400 font-semibold mb-3 px-5">
        Identities
      </div>
      <app-identities-vertical-list
        :identities="identities"
        :x-padding="5"
      />
    </div>
  </el-popover>
  <div v-else class="text-gray-500">
    -
  </div>
</template>

<script setup lang="ts">
import { defineProps, computed } from 'vue';
import pluralize from 'pluralize';
import AppPlatform from '@/shared/modules/platform/components/platform.vue';
import AppIdentitiesVerticalList from '@/shared/modules/identities/components/identities-vertical-list.vue';

const props = defineProps<{
  identities: {
    [key: string]: {
      handle: string;
      link: string | null;
      verified: boolean;
    }[];
  };
  limit?: number;
  asSvg?: boolean;
}>();

const parsedIdentities = computed(() => {
  const platformKeys = Object.keys(props.identities);
  const visibleKeys = Object.keys(props.identities).slice(
    0,
    props.limit ?? platformKeys.length,
  );

  const visible = visibleKeys.reduce(
    (
      acc: {
        [key: string]: {
          handle: string;
          link: string | null;
          verified: boolean;
        }[];
      },
      k,
    ) => {
      acc[k] = props.identities[k];

      return acc;
    },
    {},
  );

  return {
    visible,
    platformsLength: platformKeys.length,
    identitiesLength: platformKeys.reduce(
      (acc, p) => acc + props.identities[p].length,
      0,
    ),
  };
});

const hasIdentities = computed(
  () => Object.keys(props.identities).length,
);
</script>

<script lang="ts">
export default {
  name: 'AppIdentitiesHorizontalList',
};
</script>
