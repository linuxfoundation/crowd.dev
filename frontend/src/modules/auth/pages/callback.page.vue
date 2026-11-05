<template>
  <div class="flex items-center justify-center h-screen">
    <lf-spinner size="5rem" />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import { Auth0Service } from '@/modules/auth/services/auth0.service';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import { router } from '@/router';

const { authCallback, logout } = useAuthStore();

const auth0State = ref<{ returnTo?: string } | null>(null);

onMounted(() => {
  Auth0Service.handleAuth()
    .then(({ appState }) => {
      auth0State.value = appState;
      return Auth0Service.authData();
    })
    .then((token) => authCallback(token))
    .then(() => {
      router.push(auth0State.value?.returnTo ?? '/');
    })
    .catch(() => {
      logout();
    });
});
</script>

<script lang="ts">
export default {
  name: 'AuthCallback',
};
</script>
