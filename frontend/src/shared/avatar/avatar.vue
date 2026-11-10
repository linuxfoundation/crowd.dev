<template>
  <div
    class="relative"
    :class="{
      'flex justify-center': !isSmallAvatar,
    }"
  >
    <div
      v-if="entity"
      class="avatar"
      :style="computedStyle"
      :aria-label="computedInitials"
    >
      <app-avatar-image
        :src="url"
      />
    </div>
    <app-avatar-new-badge
      :entity="entity"
      :is-small-avatar="isSmallAvatar"
      :entity-name="entityName"
    />

    <slot name="icon" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import AppAvatarImage from '@/shared/avatar-image/avatar-image.vue';
import AppAvatarNewBadge from '@/shared/avatar/avatar-new-badge.vue';

interface AvatarEntity {
  displayName?: string | null;
  label?: string | null;
  avatar?: string | null;
  avatarUrl?: string | null;
  joinedAt?: string | null;
  attributes?: { avatarUrl?: { default?: string | null } };
}

defineOptions({ name: 'AppAvatar' });

const props = withDefaults(defineProps<{
  entity?: AvatarEntity;
  entityName?: string | null;
  size?: string;
}>(), {
  entity: undefined,
  entityName: null,
  size: 'md',
});

const backgroundColors = ['#DBEBFE'];
const textColors = ['#205089'];
const EMOJI_PATTERN = /([\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF])/g;
const sizes: Record<string, string> = {
  xl: '4.5rem',
  lg: '4rem',
  md: '3rem',
  sm: '2.5rem',
  xs: '2rem',
  xxs: '1.25rem',
  xxxs: '1rem',
};

const isSmallAvatar = computed(() => ['xxxs', 'xxs', 'xs', 'sm'].includes(props.size));

const computedSize = computed(() => sizes[props.size] ?? props.size);

const computedBackgroundColor = computed(() => backgroundColors[
  (props.entity?.displayName || '').length
    % backgroundColors.length
]);

const computedTextColor = computed(() => textColors[
  (props.entity?.displayName || '').length
    % textColors.length
]);

const url = computed(() => props.entity?.avatar
  ?? props.entity?.attributes?.avatarUrl?.default
  ?? props.entity?.avatarUrl
  ?? '');

const computedStyle = computed(() => ({
  backgroundColor: computedBackgroundColor.value,
  borderColor: computedBackgroundColor.value,
  color: computedTextColor.value,
  height: computedSize.value,
  width: computedSize.value,
  lineHeight: computedSize.value,
  fontSize: `calc(${computedSize.value} * 0.44)`,
}));

const computedInitials = computed(() => (
  props.entity?.displayName
  || props.entity?.label
  || ''
)
  .replace(EMOJI_PATTERN, '')
  .split(/\s+/g)
  .map((n) => n[0])
  .slice(0, 2)
  .join(''));
</script>

<style lang="scss">
.avatar {
  @apply rounded-full h-12 w-12 shrink-0 flex justify-center items-center;
  background-size: contain;
  background-position: center;
  background-repeat: no-repeat;
  border: 1px solid #dedede;
  position: relative;
  overflow: hidden;

  img{
    object-fit: cover;
    z-index: 1;
    @apply bg-white;
    width: 100%;
    height: 100%;
  }

  &:after{
    content: attr(aria-label);
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    @apply uppercase font-semibold whitespace-nowrap;
  }

}
</style>
