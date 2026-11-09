<template>
  <div class="flex items-center justify-center h-screen">
    <lf-spinner size="5rem" />
  </div>
</template>

<script lang="ts" setup>
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { onMounted } from 'vue';
import { useAuthStore } from '@/modules/auth/store/auth.store';

defineOptions({
  name: 'AuthLogoutPage',
});

const { logout } = useAuthStore();

onMounted(() => {
  logout();
});
</script>
