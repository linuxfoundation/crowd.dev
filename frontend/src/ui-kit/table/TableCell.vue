<template>
  <td
    :class="{
      'is-sticky': props.sticky,
    }"
  >
    <slot />
  </td>
</template>

<script lang="ts" setup>
defineOptions({
  name: 'LfTableCell',
});

const props = defineProps<{
  sticky?: boolean,
}>();
</script>
