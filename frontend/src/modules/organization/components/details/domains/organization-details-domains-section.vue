<template>
  <section v-if="domainList.length">
    <p v-if="props.title" class="text-small font-semibold pb-3">
      {{ pluralize(props.title, domainList.length) }}
    </p>

    <div class="flex flex-col gap-3">
      <lf-organization-details-domain-item
        v-for="domain of domainList.slice(0, showMore ? domainList.length : limit)"
        :key="domain"
        :domain="domain"
        :organization="props.organization"
        @edit="emit('edit', domain)"
        @unmerge="emit('unmerge', domain)"
      />
    </div>

    <lf-button
      v-if="domainList.length > limit"
      type="primary-link"
      size="small"
      class="mt-4"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </section>
</template>

<script setup lang="ts">
import type { Organization, OrganizationIdentity } from '@/modules/organization/types/Organization';
import LfButton from '@/ui-kit/button/Button.vue';
import { computed, ref } from 'vue';
import type { MemberIdentity } from '@/modules/member/types/Member';
import pluralize from 'pluralize';
import LfOrganizationDetailsDomainItem
  from '@/modules/organization/components/details/domains/organization-details-domain-item.vue';

const props = defineProps<{
  title?: string;
  domains: OrganizationIdentity[],
  organization: Organization,
}>();

const emit = defineEmits<{(e: 'edit', value: OrganizationIdentity): void, (e: 'unmerge', value: OrganizationIdentity): void}>();

const limit = 5;

const showMore = ref<boolean>(false);

const domainList = computed(() => {
  const domainData = (props.domains || [])
    .reduce((obj: Record<string, any>, identity: MemberIdentity) => {
      const domainObject = { ...obj };
      if (!(identity.value in domainObject)) {
        domainObject[identity.value] = {
          ...identity,
          platforms: [],
        };
      }
      domainObject[identity.value].platforms.push(identity.platform);
      domainObject[identity.value].verified = domainObject[identity.value].verified || identity.verified;

      return domainObject;
    }, {});
  return Object.keys(domainData).map((domain) => ({
    value: domain,
    ...domainData[domain],
  }));
});

</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsDomainsSection',
};
</script>
