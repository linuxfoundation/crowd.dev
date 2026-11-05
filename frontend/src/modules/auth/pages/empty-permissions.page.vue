<template>
  <div class="text-center">
    <h4 class="mb-2">
      You have no permissions yet.
    </h4>
    <div class="font-semibold text-base">
      Wait for the admin to grant you privileges.
    </div>
    <div class="pt-10">
      <lf-button size="large" @click="logout()">
        Sign out
      </lf-button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { useAuthStore } from '@/modules/auth/store/auth.store';
import LfButton from '@/ui-kit/button/Button.vue';

const { logout } = useAuthStore();
</script>

<script lang="ts">
export default {
  name: 'AuthEmptyPermissionsPage',
};
</script>
