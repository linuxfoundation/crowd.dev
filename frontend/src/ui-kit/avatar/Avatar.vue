<template>
  <div
    class="c-avatar"
    :style="{ '--lf-avatar-size': `${props.size / 16}rem` }"
    :data-initials="!!$slots.placeholder ? '' : initials"
  >
    <lf-img v-if="props.src" :alt="props.name" :src="props.src" :class="imgClass" class="w-full h-full" />
    <slot v-if="$slots.placeholder" name="placeholder" />
    <slot v-if="$slots.default" />
    <div v-if="$slots.overlay" class="c-avatar__overlay">
      <slot name="overlay" />
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import LfImg from '@/ui-kit/img/Img.vue';

defineOptions({
  name: 'LfAvatar',
});

const props = withDefaults(defineProps<{
  size?: number,
  name?: string,
  src?: string,
  imgClass?: string,
}>(), {
  size: 48,
  name: '',
  src: '',
  imgClass: '',
});

const initials = computed(() => props.name.match(/\b\w/g)?.join('').substring(0, 2) || '');
</script>
