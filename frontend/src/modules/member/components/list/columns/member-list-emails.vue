<template>
  <div
    v-if="distinctEmails?.length && distinctEmails?.some((e) => !!e)"
    class="text-sm cursor-auto flex flex-col items-start gap-1"
  >
    <div
      v-for="email of distinctEmails.slice(0, 3)"
      :key="email"
      class="flex items-center"
    >
      <el-tooltip
        :disabled="!email"
        popper-class="custom-identity-tooltip"
        placement="top"
      >
        <template #content>
          <span>Send email
            <lf-icon name="arrow-up-right-from-square" :size="16" class="text-gray-400" />
          </span>
        </template>
        <div @click.prevent>
          <a
            target="_blank"
            rel="noopener noreferrer"
            class="badge--interactive flex items-center"
            :href="email.link"
            @click.stop="trackEmailClick"
          >
            {{ email.handle }}
            <div v-if="email.verified">
              <el-tooltip placement="top" content="Verified email">
                <lf-icon name="badge-check" type="solid" :size="16" class="text-primary-500 leading-4" />
              </el-tooltip>
            </div>
          </a>
        </div>
      </el-tooltip>
      <div v-if="getPlatformLabel(email.platforms)" class="ml-2 flex items-center">
        <el-tooltip placement="top">
          <template #content>
            <span class="font-semibold">Source:&nbsp;</span>{{ getPlatformLabel(email.platforms) }}
          </template>
          <lf-icon name="sparkle" type="solid" :size="14" :class="isEnrichment(email.platforms) ? 'text-purple-400' : 'text-gray-300'" />
        </el-tooltip>
      </div>
    </div>

    <el-popover
      v-if="distinctEmails?.length > 3"
      placement="top"
      trigger="hover"
      popper-class="support-popover max-h-100 overflow-y-auto !w-auto"
    >
      <template #reference>
        <span
          class="badge--interactive hover:text-gray-900"
        >+{{ distinctEmails.length - 3 }}</span>
      </template>
      <div class="flex flex-col items-start gap-3 my-1">
        <div
          v-for="email of distinctEmails.slice(3)"
          :key="email.handle"
          class="flex items-center"
        >
          <el-tooltip

            :disabled="!email.handle"
            popper-class="custom-identity-tooltip flex "
            placement="top"
          >
            <template #content>
              <span>Send email
                <lf-icon v-if="email.link" name="arrow-up-right-from-square" :size="16" class="text-gray-400" />
              </span>
            </template>
            <div @click.prevent>
              <a
                target="_blank"
                rel="noopener noreferrer"
                class="badge--interactive flex items-center"
                :href="email.link"
                @click.stop="trackEmailClick"
              >
                {{ email.handle }}
                <div v-if="email.verified">
                  <el-tooltip placement="top" content="Verified email">
                    <lf-icon name="badge-check" type="solid" :size="16" class="text-primary-500 leading-4" />
                  </el-tooltip>
                </div>
              </a>
            </div>
          </el-tooltip>
          <div v-if="getPlatformLabel(email.platforms)" class="ml-2 flex items-center">
            <el-tooltip placement="top">
              <template #content>
                <span class="font-semibold">Source:&nbsp;</span>{{ getPlatformLabel(email.platforms) }}
              </template>
              <lf-icon name="sparkle" type="solid" :size="14" :class="isEnrichment(email.platforms) ? 'text-purple-400' : 'text-gray-300'" />
            </el-tooltip>
          </div>
        </div>
      </div>
    </el-popover>
  </div>
  <span v-else class="text-gray-500">-</span>
</template>

<script lang="ts" setup>
import type { Member } from '@/modules/member/types/Member';
import { computed } from 'vue';
import useMemberIdentities from '@/shared/modules/identities/config/useMemberIdentities';
import memberOrder from '@/shared/modules/identities/config/identitiesOrder/member';
import { lfIdentities } from '@/config/identities';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  member: Member
}>();

const emails = computed(() => useMemberIdentities({
  member: props.member,
  order: memberOrder.list,
}).getEmails());

const distinctEmails = computed(() => {
  const emailsdata = emails.value.reduce((obj: Record<string, any>, identity: any) => {
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
  }, {});
  return Object.keys(emailsdata).map((email) => ({
    handle: email,
    ...emailsdata[email],
  }));
});

const isEnrichment = (platforms:string[]) => platforms.includes('enrichment');

const getPlatformLabel = (platforms: string[]) => platforms
  .filter((platform) => !['integration_or_enrichment'].includes(platform))
  .map((platform) => {
    if (platform === 'lfid') {
      return 'LFID';
    }
    if (platform === 'integration') {
      return 'Integration';
    }
    if (platform === 'enrichment') {
      return 'Enrichment';
    }
    return lfIdentities[platform]?.name || platform;
  }).join(', ');

const trackEmailClick = () => {
  window.analytics.track('Click Member Contact', {
    channel: 'Email',
  });
};

</script>

<script lang="ts">
export default {
  name: 'AppMemberListEmails',
};
</script>
