<template>
  <div class="relative flex items-center justify-center">
    <el-tooltip
      :content="platformName"
      placement="top"
      :disabled="!platformName || !showPlatformTooltip"
      popper-class="-mb-2"
      :hide-after="0"
    >
      <component
        :is="asLink ? 'a' : 'span'"
        :href="asLink ? identities[0].link ?? null : null"
        target="_blank"
        rel="noopener noreferrer"
        class="btn flex items-center justify-center"
        :class="{
          'hover:cursor-auto': !asLink,
          'hover:cursor-pointer': asLink,
          'min-h-4 min-w-[16px] h-4 w-4': size === 'small',
          'min-h-5 min-w-[20px] h-5 w-5': size !== 'small',
        }"
        @click.stop
      >
        <app-platform-svg v-if="asSvg" :platform="platform" :size="size" />
        <app-platform-img
          v-else-if="defaultPlatformConfig"
          :platform="platform"
          :size="size"
        />
        <app-platform-icon v-else :platform="platform" :size="size" />
      </component>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { defineProps, computed } from 'vue';
import AppPlatformIcon from '@/shared/modules/platform/components/platform-icon.vue';
import AppPlatformSvg from '@/shared/modules/platform/components/platform-svg.vue';
import AppPlatformImg from '@/shared/modules/platform/components/platform-img.vue';
import { lfIdentities } from '@/config/identities';

const props = withDefaults(
  defineProps<{
    platform: string;
    size?: string;
    identities?: {
      handle: string;
      link: string;
    }[];
    asLink?: boolean;
    asSvg?: boolean;
    showPlatformTooltip?: boolean;
  }>(),
  {
    identities: () => [],
    asSvg: false,
    size: 'small',
    asLink: true,
    showPlatformTooltip: false,
  },
);

const defaultPlatformConfig = computed(() => lfIdentities[props.platform]);

const platformName = computed(() => {
  if (props.platform === 'domains') {
    return 'Domain';
  }

  if (props.platform === 'emails') {
    return 'Email';
  }

  if (props.platform === 'phoneNumbers') {
    return 'Phone number';
  }

  return lfIdentities[props.platform]?.name || 'Custom';
});
</script>

<script lang="ts">
export default {
  name: 'AppPlatform',
};
</script>

<style lang="scss">
// Custom tooltip for external links
.custom-identity-tooltip {
  span:first-child {
    @apply flex gap-1.5 items-center;
  }
}

a[href]:hover {
  opacity: 1;
}
</style>
