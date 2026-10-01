<template>
  <div v-if="props.emails.length" class="flex flex-col gap-5">
    <div
      class="flex gap-3 items-start relative min-h-4"
      :class="{
        [`px-${xPadding}`]: !!xPadding,
      }"
    >
      <app-platform
        platform="emails"
        :as-link="false"
        size="large"
        class="mt-1"
        show-platform-tooltip
      />

      <div class="flex flex-wrap items-center gap-2">
        <div class="flex flex-col gap-1">
          <template v-for="(identity) of Object.values(distinctEmails)" :key="identity.handle">
            <div class="flex items-center">
              <a
                :href="identity.link"
                class="text-gray-900 text-xs font-medium leading-5 items-center w-auto break-all"
                :class="{
                  'underline decoration-dashed decoration-gray-400 underline-offset-4 ':
                    identity.link,
                  'hover:decoration-gray-900 hover:cursor-pointer hover:!text-gray-900':
                    identity.link,
                }"
                target="_blank"
                rel="noopener noreferrer"
              >
                {{ identity.handle }}
              </a>
              <div v-if="getPlatformsLabel(identity.platforms)" class="ml-2 flex items-center">
                <el-tooltip placement="top">
                  <template #content>
                    <span class="font-semibold">Source:&nbsp;</span>{{ getPlatformsLabel(identity.platforms) }}
                  </template>
                  <lf-icon name="sparkle" :size="14" :class="isEnrichment(identity.platforms) ? 'text-purple-400' : 'text-gray-300'" />
                </el-tooltip>
              </div>
              <lf-verified-identity-badge v-if="identity.verified" />
            </div>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import AppPlatform from '@/shared/modules/platform/components/platform.vue';
import { computed } from 'vue';
import LfVerifiedIdentityBadge from '@/shared/modules/identities/components/verified-identity-badge.vue';
import useIdentitiesHelpers from '@/config/identities/identities.helpers';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  emails: {
      handle: string;
      link: string | null;
      verified: boolean;
  }[];
  xPadding?: number;
}>();

const { getPlatformsLabel } = useIdentitiesHelpers();

const distinctEmails = computed(() => props.emails.reduce((obj: Record<string, any>, identity: any) => {
  const emailObject = { ...obj };
  if (!(identity.handle in emailObject)) {
    emailObject[identity.handle] = {
      ...identity,
      platforms: [],
    };
  }
  emailObject[identity.handle].platforms.push(identity.platform);
  emailObject[identity.handle].verified = emailObject[identity.handle].verified || identity.verified;

  return emailObject;
}, {}));

const isEnrichment = (platforms:string[]) => platforms.includes('enrichment');

</script>

<script lang="ts">
export default {
  name: 'AppEmailsVerticalList',
};
</script>
