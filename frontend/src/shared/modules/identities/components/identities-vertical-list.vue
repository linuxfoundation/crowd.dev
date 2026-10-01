<template>
  <div class="flex flex-col gap-5">
    <div
      v-for="[platform, value] of Object.entries(slicedIdentities)"
      :key="platform"
    >
      <div v-if="value.length">
        <div
          class="flex gap-3 items-start relative min-h-4"
          :class="{
            [`px-${xPadding}`]: !!xPadding,
          }"
        >
          <app-platform
            :platform="platform"
            :as-link="false"
            size="large"
            class="mt-1"
            show-platform-tooltip
          />

          <div class="flex flex-wrap items-center gap-2">
            <div class="flex flex-wrap items-center">
              <template v-for="({ handle, link, verified }, vi) of value" :key="handle">
                <div class="flex items-center">
                  <div
                    v-if="platform === 'linkedin' && handle.includes('private-')"
                    class="text-gray-900 text-xs"
                  >
                    <span>*********</span>
                    <el-tooltip placement="top" content="Private profile">
                      <lf-icon name="lock-keyhole" class="text-gray-400" />
                    </el-tooltip>
                  </div>

                  <component
                    :is="link ? 'a' : 'span'"
                    v-else
                    :href="link"
                    class="text-gray-900 text-xs font-medium leading-5 items-center w-auto break-all"
                    :class="{
                      'underline decoration-dashed decoration-gray-400 underline-offset-4 ':
                        link,
                      'hover:decoration-gray-900 hover:cursor-pointer hover:!text-gray-900':
                        link,
                    }"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {{ handle }}
                  </component>

                  <lf-verified-identity-badge v-if="verified" />
                </div>

                <span
                  v-if="vi !== value.length - 1"
                  class="font-medium"
                >・</span>
              </template>
            </div>
            <span
              v-if="isCustomPlatform(platform)"
              class="text-xs text-gray-400"
            >{{ platform }}</span>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="displayShowMore && Object.keys(identities).length > 5"
      class="underline cursor-pointer text-gray-500 hover:text-primary-500 text-xs underline-offset-4"
      :class="{
        [`px-${xPadding}`]: !!xPadding,
      }"
      @click="displayMore = !displayMore"
    >
      Show {{ displayMore ? "less" : "more" }}
    </div>
  </div>
</template>

<script setup lang="ts">
import AppPlatform from '@/shared/modules/platform/components/platform.vue';
import { computed, ref } from 'vue';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfVerifiedIdentityBadge from '@/shared/modules/identities/components/verified-identity-badge.vue';
import { lfIdentities } from '@/config/identities';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  identities: {
    [key: string]: {
      handle: string;
      link: string | null;
      verified: boolean;
    }[];
  };
  xPadding?: number;
  displayShowMore?: boolean;
}>();

const displayMore = ref(false);

const slicedIdentities = computed(() => {
  if (!displayMore.value && props.displayShowMore) {
    return Object.fromEntries(Object.entries(props.identities).filter(([, v]) => v.length).slice(0, 5));
  }

  return Object.fromEntries(Object.entries(props.identities).filter(([, v]) => v.length));
});

const isCustomPlatform = (platform: string) => platform !== Platform.EMAILS
  && platform !== Platform.PHONE_NUMBERS
  && platform !== 'domains'
  && platform !== 'email'
  && !lfIdentities[platform]?.name;
</script>

<script lang="ts">
export default {
  name: 'AppIdentitiesVerticalList',
};
</script>
