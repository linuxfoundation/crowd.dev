<template>
  <div>
    <div class="pt-6 pb-6">
      <lf-search
        v-model="search"
        class="h-9"
        :lazy="true"
        placeholder="Search users..."
        @update:model-value="searchUsers()"
      />
    </div>
    <div v-if="users.length > 0">
      <lf-table>
        <thead>
          <tr>
            <th>User</th>
            <th>Email</th>
            <th>Role</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="user of users" :key="user.id">
            <td>
              <div class="flex items-center gap-3">
                <!--<lf-avatar :name="nameDisplay(user)" :size="32" />-->
                <p class="text-medium font-semibold">
                  {{ nameDisplay(user) }}
                </p>
              </div>
            </td>
            <td>
              <p class="text-medium">
                {{ user.email }}
              </p>
            </td>
            <td>
              <div class="flex items-center">
                <div
                  v-for="(role, roleIndex) in user.roles"
                  :key="role"
                  class="border rounded-full border-gray-200 px-2.5 py-1"
                  :class="roleIndex !== user.roles.length - 1 ? 'mr-3' : ''"
                >
                  <el-tooltip
                    v-if="role !== UserRole.projectAdmin"
                    content="This role applies to all project groups"
                    placement="top"
                  >
                    <p class="text-medium">
                      {{ roleDisplay(role) }}
                    </p>
                  </el-tooltip>
                  <el-popover v-else trigger="hover" placement="top" :width="250">
                    <template #reference>
                      <div class="flex items-baseline text-medium">
                        {{ roleDisplay(role) }}
                        <lf-icon class="mx-1" :name="'folders'" :size="16" />
                        <p>
                          {{ user.adminSegments?.length }}
                        </p>
                      </div>
                    </template>
                    <div class="p-1">
                      <span class="text-xs text-gray-400">Project groups</span>
                      <div class="overflow-auto max-h-30 mt-4 flex items-baseline flex-wrap gap-1">
                        <lf-badge
                          v-for="segment in user.adminSegments"
                          :key="segment.id"
                          type="tertiary"
                          size="medium"
                          class="truncate"
                        >
                          {{ segment.name }}
                        </lf-badge>
                      </div>
                    </div>
                  </el-popover>
                </div>
              </div>
            </td>
          </tr>
        </tbody>
      </lf-table>
      <div v-if="users.length < total" class="pt-4">
        <lf-button type="primary-ghost" loading-text="Loading users..." :loading="loading" @click="loadMore()">
          Load more
        </lf-button>
      </div>
    </div>

    <div v-else-if="!loading">
      <app-empty-state-cta icon="user-group" title="No users found" />
    </div>
    <div v-if="loading" class="pt-8 flex justify-center">
      <lf-spinner />
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import { UsersService } from '@/modules/admin/modules/users/services/users.service';
import type {
  UserModel,
} from '@/modules/admin/modules/users/models/User.model';
import {
  UserRole,
} from '@/modules/admin/modules/users/models/User.model';
import LfTable from '@/ui-kit/table/Table.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';

const search = ref('');
const loading = ref<boolean>(false);
const offset = ref(0);
const limit = ref(20);
const total = ref(0);
const users = ref<UserModel[]>([]);

const fetchUsers = () => {
  if (loading.value) {
    return;
  }
  loading.value = true;
  UsersService.list({
    filter: {
      query: search.value,
    },
    offset: offset.value,
    limit: limit.value,
    orderBy: 'fullName_ASC',
  })
    .then((res) => {
      if (offset.value > 0) {
        users.value = [...users.value, ...res.rows];
      } else {
        users.value = res.rows;
      }

      if (res.rows.length < limit.value) {
        total.value = users.value.length;
      } else {
        total.value = res.count;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const searchUsers = () => {
  offset.value = 0;
  fetchUsers();
};

const loadMore = () => {
  offset.value = users.value.length;
  fetchUsers();
};

const roleDisplay = (role: UserRole) => {
  if (role === UserRole.admin) {
    return 'Admin';
  }
  if (role === UserRole.projectAdmin) {
    return 'Project Admin';
  }

  if (role === UserRole.readonly) {
    return 'Read-only';
  }
  return role;
};

const nameDisplay = (user: UserModel) => {
  if ((user.fullName || '').length > 0) {
    return user.fullName;
  }
  return user.email.split('@')[0];
};

onMounted(() => {
  searchUsers();
});
</script>

<script lang="ts">
export default {
  name: 'LfAdminUsers',
};
</script>
