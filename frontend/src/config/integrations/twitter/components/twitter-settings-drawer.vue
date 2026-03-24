<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-twitter-drawer"
    title="X/Twitter"
    size="600px"
    pre-title="Integration"
    :pre-title-img-src="logoUrl"
    pre-title-img-alt="X/Twitter logo"
    close-on-click-modal="true"
    :close-function="canClose"
    @close="isVisible = false"
  >
    <template #belowTitle>
      <drawer-description integration-key="twitter" />
    </template>
    <template #content>
      <el-form label-position="top" class="form integration-twitter-form" @submit.prevent>
        <el-form-item :label="hashtagField.label">
          <el-input v-model="model.hashtag" clearable class="hashtag-input">
            <template #prefix>
              <span>#</span>
            </template>
          </el-input>

          <div class="app-form-hint leading-tight mt-2">
            Tip: Choose a hashtag that's specific to your
            company/community for better data
          </div>
        </el-form-item>
      </el-form>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="integration"
        :is-edit-mode="integration.settings?.hashtags > 0"
        :has-form-changed="hasFormChanged"
        :is-loading="false"
        :is-submit-disabled="!hasFormChanged"
        :cancel="() => (isVisible = false)"
        :revert-changes="doReset"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup>
import {
  defineEmits,
  defineProps,
  computed,
  ref,
} from 'vue';
import isEqual from 'lodash/isEqual';
import { FormSchema } from '@/shared/form/form-schema';
import StringField from '@/shared/fields/string-field';
import twitter from '@/config/integrations/twitter/config';
import config from '@/config';
import { AuthService } from '@/modules/auth/services/auth.service';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  integration: {
    type: Object,
    default: null,
  },
  segmentId: {
    type: String,
    default: null,
  },
  grandparentId: {
    type: String,
    default: null,
  },
});

const emit = defineEmits(['update:modelValue']);
const changesConfirmationModalRef = ref(null);

const getHashtags = () => props.integration.settings?.hashtags || [];

const getConnectUrl = () => {
  const redirectUrl = props.grandparentId && props.segmentId
    ? `${window.location.protocol}//${window.location.host}/integrations/${props.grandparentId}/${props.segmentId}?success=true`
    : `${window.location.protocol}//${window.location.host}${window.location.pathname}?success=true`;

  return `${config.backendUrl}/twitter/connect?redirectUrl=${redirectUrl}&crowdToken=${AuthService.getToken()}&segments[]=${
    props.segmentId
  }`;
};

const parsedHashtags = computed(() => (getHashtags().length
  ? getHashtags()[getHashtags().length - 1]
  : ''));
const hashtagField = new StringField(
  'hashtag',
  'Track hashtag',
);
const formSchema = ref(new FormSchema([hashtagField]));
const model = ref(
  formSchema.value.initialValues({
    hashtag: parsedHashtags.value,
  }),
);

const logoUrl = twitter.image;

const hasFormChanged = computed(
  () => !isEqual(
    formSchema.value.initialValues({
      hashtag: parsedHashtags.value,
    }),
    model.value,
  ),
);

const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    return emit('update:modelValue', value);
  },
});

const computedConnectUrl = computed(() => {
  const encodedHashtags = model.value.hashtag
    ? `&hashtags[]=${model.value.hashtag}`
    : '';

  return `${getConnectUrl()}${encodedHashtags}`;
});

const connect = () => {
  if (hasFormChanged.value) {
    window.open(computedConnectUrl.value, '_self');
  }
};

const doReset = () => {
  model.value = formSchema.value.initialValues({
    hashtag: parsedHashtags.value,
  });
};

const canClose = (done) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges) => {
      if (discardChanges) {
        doReset();
        done(false);
      } else {
        done(true);
      }
    });
  } else {
    done(false);
  }
};
</script>

<script>
export default {
  name: 'LfTwitterSettingsDrawer',
};
</script>

<style lang="scss">
.integration-twitter-form {
    .el-form-item {
        @apply mb-3;

        &__content {
            @apply mb-0;

            .hashtag-input .el-input__inner {
                @apply pl-1;
            }
        }
    }

    .el-input-group__prepend {
        @apply px-3;
    }
}
</style>
