<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-hackerNews-drawer"
    title="Hacker News"
    size="600px"
    pre-title="Integration"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="cancel"
  >
    <template #beforeTitle>
      <img class="min-w-6 h-6 mr-2" :src="logoUrl" alt="Hacker News logo" />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="hackernews" />
    </template>
    <template #content>
      <div class="flex flex-col gap-2 items-start mb-2">
        <span class="text-xs font-light mb-2 text-gray-900">
          Monitor mentions of your community/organization on Hacker News.
          <br />Historical data is available after the 1st of December 2022.
          <a
            href="https://docs.crowd.dev/docs/hacker-news-integration"
            target="__blank"
          >
            Read more</a>.
        </span>
        <span class="text-sm font-medium">Track posts mentioning your community/organization</span>
        <span class="text-2xs font-light mb-2 text-gray-600">
          Monitor your community/organization being mentioned in the top 500 of
          Hacker News. <br />
        </span>
        <app-keywords-input v-model="keywords" placeholder="e.g. LFX, linux" />
      </div>
      <el-form class="form integration-hackerNews-form" @submit.prevent>
        <div class="flex flex-col gap-2 items-start">
          <span class="text-sm font-medium">Track your URL</span>
          <span class="text-2xs font-light mb-2 text-gray-600">
            Monitor when a post with your URL is published in the top 500 of
            Hacker News. <br />
          </span>
          <el-form-item
            v-for="url in urls"
            :key="url.id"
            class="mb-4 w-full"
            :class="{
              'is-error': url.touched && !url.valid,
              'is-success': url.touched && url.valid,
            }"
          >
            <div class="flex flex-row items-center w-full gap-4">
              <el-input
                id="devUrl"
                v-model="url.url"
                class="text-green-500"
                spellcheck="false"
                placeholder="Enter a URL"
                @blur="handleUrlValidation(url.id)"
              >
                <template #prepend>
                  <span class="font-light text-gray-800">https://</span>
                </template>
              </el-input>
              <lf-button
                v-if="!isLastUrl"
                type="primary-link"
                size="medium"
                class="w-10 h-10"
                @click="removeUrl(url.id)"
              >
                <lf-icon name="trash-can" :size="20" class="text-black" />
              </lf-button>
            </div>
            <span
              v-if="url.touched && !url.valid"
              class="el-form-item__error"
            >Url slug is not valid</span>
          </el-form-item>
          <lf-button type="primary-link" size="medium" @click="addNewUrl">
            + Add URL
          </lf-button>
        </div>
      </el-form>
    </template>
    <template #footer>
      <drawer-footer-buttons
        :integration="integration"
        :is-edit-mode="!!integration?.settings?.urls"
        :has-form-changed="hasFormChanged"
        :is-loading="loading"
        :is-submit-disabled="connectDisabled || loading"
        :cancel="cancel"
        :revert-changes="revertChanges"
        :connect="save"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script>
import { mapActions } from 'vuex';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';
import hackernews from '@/config/integrations/hackernews/config';
import isUrl from '@/utils/isUrl';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfButton from '@/ui-kit/button/Button.vue';
import AppDrawer from '@/shared/drawer/drawer.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';

export default {
  name: 'LfHackernewsSettingsDrawer',
  components: {
    LfIcon,
    LfButton,
    AppDrawer,
    DrawerDescription,
    DrawerFooterButtons,
    ChangesConfirmationModal,
  },
  props: {
    integration: {
      type: Object,
      default: null,
    },
    modelValue: {
      type: Boolean,
      default: false,
    },
    segmentId: {
      type: String,
      default: null,
    },
    grandparentId: {
      type: String,
      default: null,
    },
  },
  emits: ['update:modelValue'],
  data() {
    return {
      logoUrl: hackernews.image,
      users: [],
      urls: [],
      keywords: [],
      loading: false,
      changesConfirmationModalRef: null,
    };
  },
  computed: {
    hasFormChanged() {
      const validUrls = this.urls.filter((u) => !!u.url);
      const validKeywords = this.keywords.filter((k) => !!k);
      const originalUrls = this.integration?.settings?.urls || [];
      const originalKeywords = this.integration?.settings?.keywords || [];

      if (validUrls.length !== originalUrls.length) return true;
      if (validKeywords.length !== originalKeywords.length) return true;
      if (!validUrls.every((o) => originalUrls.includes(o.url))) return true;
      if (!validKeywords.every((k) => originalKeywords.includes(k))) return true;

      return false;
    },
    maxId() {
      return this.urls.length;
    },
    isVisible: {
      get() {
        return this.modelValue;
      },
      set(value) {
        this.$emit('update:modelValue', value);
      },
    },
    isValid() {
      const relevantUrls = this.urls.filter((u) => !!u.url);

      const relevantKeywords = this.keywords.filter((k) => !!k);

      if (relevantUrls.some((url) => !url.valid)) {
        return false;
      }

      return relevantUrls.length > 0 || relevantKeywords.length > 0;
    },
    connectDisabled() {
      if (!this.isValid) {
        return true;
      }

      const validUrls = this.urls.filter((u) => !!u.url);

      const validKeywords = this.keywords.filter((k) => !!k);

      const empty = validUrls.length === 0 && validKeywords.length === 0;
      if (this.integration?.settings && !empty) {
        return (
          validUrls.length === this.integration?.settings.urls.length
          && validUrls.every((o) => this.integration?.settings.urls.includes(o.url))
          && validKeywords.length === this.integration?.settings.keywords.length
        );
      }
      return empty;
    },
    isLastUrl() {
      return this.urls.length === 1;
    },
  },
  watch: {
    integration: {
      handler(newVal) {
        if (newVal) {
          this.syncData();
        }
      },
    },
  },

  mounted() {
    this.syncData();
  },

  methods: {
    ...mapActions({
      doHackerNewsConnect: 'integration/doHackerNewsConnect',
    }),

    canClose(done) {
      if (this.hasFormChanged) {
        this.$refs.changesConfirmationModalRef?.open().then((discardChanges) => {
          if (discardChanges) {
            this.revertChanges();
            done(false);
          } else {
            done(true);
          }
        });
      } else {
        done(false);
      }
    },

    toggle() {
      this.isVisible = !this.isVisible;
    },

    syncData() {
      this.urls = [];

      if (this.integration && this.integration?.settings) {
        this.integration?.settings.urls.forEach((k) => this.addNewUrl(k));
      }

      if (this.urls.length === 0) {
        this.addNewUrl();
      }
    },

    revertChanges() {
      this.syncData();
      this.keywords = this.integration?.settings.keywords;
    },

    addNewUrl(url) {
      this.urls.push({
        id: this.maxId + 1,
        url: typeof url === 'string' || url instanceof String ? url : '',
        touched: false,
        valid: false,
        validating: false,
      });
    },

    handleUrlValidation(id) {
      const url = this.urls.find((o) => o.id === id);
      url.validating = true;
      url.url = url.url.replace('https://', '');
      url.url = url.url.replace('http://', '');

      const isValid = isUrl(url.url);

      url.valid = isValid && !!url.url;
      url.validating = false;
      url.touched = true;
    },

    removeUrl(id) {
      this.urls = this.urls.filter((u) => u.id !== id);
    },

    cancel() {
      this.isVisible = false;
      this.syncData();
    },

    async save() {
      this.loading = true;

      const relevantUrls = this.urls.filter((u) => !!u.url);

      const relevantKeywords = this.keywords.filter((k) => !!k);

      await this.doHackerNewsConnect({
        urls: relevantUrls.map((u) => u.url),
        keywords: relevantKeywords,
        segmentId: this.segmentId,
        grandparentId: this.grandparentId,
      });

      const { trackEvent } = useProductTracking();

      const isUpdate = !!this.integration?.settings;

      trackEvent({
        key: isUpdate
          ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS
          : FeatureEventKey.CONNECT_INTEGRATION,
        type: EventType.FEATURE,
        properties: { platform: Platform.HACKER_NEWS },
      });

      this.isVisible = false;
      this.loading = false;
    },
  },
};
</script>
<style lang="scss">
.integration-hackerNews-form {
  .el-form-item {
    @apply mb-3;
    &__content {
      @apply mb-0;
    }
  }

  .el-input-group__prepend {
    @apply px-3;
  }
}
</style>
