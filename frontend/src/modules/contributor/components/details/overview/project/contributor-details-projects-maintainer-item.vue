<template>
  <el-popover placement="top" width="20rem">
    <template #reference>
      <lf-badge
        type="secondary"
        class="!flex items-center gap-1"
      >
        <slot />
      </lf-badge>
    </template>
    <div class="p-1 flex flex-col gap-6">
      <section v-if="currentRepos.length > 0">
        <p class="text-small font-semibold text-gray-400 pb-4">
          Repositories
        </p>
        <div class="flex flex-col gap-4">
          <article v-for="repo of currentRepos" :key="repo.id" class="flex items-center justify-between">
            <div class="flex items-center gap-2">
              <img :src="lfIdentities[repo.repoType]?.image" :alt="repo.repoType" class="min-w-4 h-4" />
              <a
                :href="repo.url"
                target="_blank"
                rel="noopener noreferrer"
                class="cursor-pointer text-small leading-5 underline decoration-dashed text-black
             decoration-gray-500 underline-offset-4 hover:decoration-gray-900 hover:!text-black inline truncate"
                style="max-width: 18ch"
              >
                {{ getName(repo.url) }}
              </a>
            </div>
            <p class="text-small text-gray-400">
              since {{ dateHelper(repo.dateStart).format('MMM YYYY') }}
            </p>
          </article>
        </div>
      </section>
      <section v-if="previousRepos.length > 0">
        <p class="text-small font-semibold text-gray-400 pb-4">
          Previously
        </p>
        <div v-for="repo of previousRepos" :key="repo.id" class="flex flex-col gap-4">
          <article class="flex items-center justify-between">
            <div class="flex items-center gap-2">
              <img :src="lfIdentities[repo.repoType]?.image" :alt="repo.repoType" class="min-w-4 h-4" />
              <a
                :href="repo.url"
                target="_blank"
                rel="noopener noreferrer"
                class="cursor-pointer text-small leading-5 underline decoration-dashed text-black
             decoration-gray-500 underline-offset-4 hover:decoration-gray-900 hover:!text-black inline truncate"
                style="max-width: 18ch"
              >
                {{ getName(repo.url) }}
              </a>
            </div>
            <p class="text-small text-gray-400">
              {{ dateHelper(repo.dateStart).format('MMM YYYY') }} → {{ dateHelper(repo.dateEnd).format('MMM YYYY') }}
            </p>
          </article>
        </div>
      </section>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import LfBadge from '@/ui-kit/badge/Badge.vue';
import type { ContributorMaintainerRole } from '@/modules/contributor/types/Contributor';
import { computed } from 'vue';
import { dateHelper } from '@/shared/date-helper/date-helper';
import { lfIdentities } from '@/config/identities';

const props = defineProps<{
  maintainerRoles: ContributorMaintainerRole[],
}>();

const currentRepos = computed<ContributorMaintainerRole[]>(() => props.maintainerRoles.filter((role) => !role.dateEnd));
const previousRepos = computed<ContributorMaintainerRole[]>(() => props.maintainerRoles.filter((role) => !!role.dateEnd));

const getName = (url: string) => url.split('/').pop();
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsProjectsMaintainerItem',
};
</script>
