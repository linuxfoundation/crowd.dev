<template>
  <app-drawer
    v-model="isVisible"
    title="Sched"
    size="600px"
    pre-title="Integration"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="cancel"
  >
    <template #beforeTitle>
      <img class="min-w-6 h-6 mr-2" :src="logoUrl" alt="Sched logo" />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="sched" />
    </template>
    <template #content>
      <div class="text-gray-900 text-sm font-medium">
        Events
      </div>
      <div class="text-2xs text-gray-500 mb-4">
        Configure one or more Sched events to sync speakers and attendees data.
      </div>

      <el-form @submit.prevent>
        <div
          v-for="(_, index) of form.events"
          :key="index"
          class="border border-gray-200 rounded-lg p-4 mb-4"
        >
          <div class="flex items-center justify-between mb-3">
            <span class="text-gray-900 text-xs font-semibold">Event {{ index + 1 }}</span>
            <lf-button
              v-if="form.events.length > 1"
              type="primary-link"
              size="medium"
              class="w-8 h-8"
              icon-only
              @click="removeEvent(index)"
            >
              <lf-icon name="trash-can" :size="16" />
            </lf-button>
          </div>

          <app-form-item
            class="mb-3"
            label="Event subdomain"
            :required="true"
            :error-messages="{ required: 'This field is required' }"
          >
            <el-input
              v-model="form.events[index].subdomain"
              placeholder="e.g. myconference"
              spellcheck="false"
            />
          </app-form-item>

          <app-form-item
            class="mb-3"
            label="API key"
            :required="true"
            :error-messages="{ required: 'This field is required' }"
          >
            <el-input
              v-model="form.events[index].apiKey"
              type="password"
              placeholder="Enter API key"
              spellcheck="false"
            />
          </app-form-item>

          <app-form-item
            class="mb-3"
            label="Event ID"
            :required="true"
            :error-messages="{ required: 'This field is required' }"
          >
            <el-input
              v-model="form.events[index].eventId"
              placeholder="Enter event ID"
              spellcheck="false"
            />
          </app-form-item>

          <app-form-item
            class="mb-3"
            label="Event name"
            :required="true"
            :error-messages="{ required: 'This field is required' }"
          >
            <el-input
              v-model="form.events[index].eventName"
              placeholder="Enter event name"
              spellcheck="false"
            />
          </app-form-item>

          <app-form-item
            class="mb-0"
            label="Speaker role name"
            :required="true"
            :error-messages="{ required: 'This field is required' }"
          >
            <el-input
              v-model="form.events[index].speakerRoleName"
              placeholder="e.g. Speaker"
              spellcheck="false"
            />
          </app-form-item>
        </div>

        <lf-button type="primary-link" @click="addEvent()">
          + Add event
        </lf-button>
      </el-form>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="props.integration"
        :is-edit-mode="!!props.integration?.settings"
        :has-form-changed="hasFormChanged"
        :is-loading="loading"
        :is-submit-disabled="$v.$invalid || !hasFormChanged || loading"
        :cancel="cancel"
        :revert-changes="revertChanges"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup lang="ts">
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import sched from '@/config/integrations/sched/config';
import formChangeDetector from '@/shared/form/form-change';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppDrawer from '@/shared/drawer/drawer.vue';
import AppFormItem from '@/shared/form/form-item.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

interface SchedEvent {
  subdomain: string;
  apiKey: string;
  eventId: string;
  eventName: string;
  speakerRoleName: string;
}

const emptyEvent = (): SchedEvent => ({
  subdomain: '',
  apiKey: '',
  eventId: '',
  eventName: '',
  speakerRoleName: '',
});

const emit = defineEmits(['update:modelValue']);
const props = defineProps({
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
    required: true,
  },
  grandparentId: {
    type: String,
    required: true,
  },
});

const { trackEvent } = useProductTracking();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);
const loading = ref(false);

const form = reactive<{ events: SchedEvent[] }>({
  events: [emptyEvent()],
});

const isEventValid = (e: SchedEvent) => e.subdomain.trim() !== ''
  && e.apiKey.trim() !== ''
  && e.eventId.trim() !== ''
  && e.eventName.trim() !== ''
  && e.speakerRoleName.trim() !== '';

const { formSnapshot, hasFormChanged } = formChangeDetector(form);

const $v = useVuelidate({
  events: {
    required: (value: SchedEvent[]) => value.length > 0 && value.every(isEventValid),
  },
}, form, { $stopPropagation: true });

const { doSchedConnect } = mapActions('integration');

const isVisible = computed({
  get() { return props.modelValue; },
  set(value) { emit('update:modelValue', value); },
});

const logoUrl = sched.image;

const syncData = () => {
  if (props.integration?.settings?.events?.length) {
    form.events = props.integration.settings.events.map((e: SchedEvent) => ({ ...e }));
  } else {
    form.events = [emptyEvent()];
  }
  formSnapshot();
};

onMounted(() => {
  syncData();
});

const revertChanges = () => {
  syncData();
};

const addEvent = () => {
  form.events.push(emptyEvent());
};

const removeEvent = (index: number) => {
  form.events.splice(index, 1);
};

const cancel = () => {
  isVisible.value = false;
};

const canClose = (done: (value: boolean) => void) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges: boolean) => {
      if (discardChanges) {
        revertChanges();
        done(false);
      } else {
        done(true);
      }
    });
  } else {
    done(false);
  }
};

const connect = async () => {
  loading.value = true;

  const isUpdate = props.integration?.settings;

  doSchedConnect({
    id: props.integration?.id,
    settings: {
      events: form.events,
    },
    isUpdate,
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
  })
    .then(() => {
      trackEvent({
        key: isUpdate
          ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS
          : FeatureEventKey.CONNECT_INTEGRATION,
        type: EventType.FEATURE,
        properties: {
          platform: Platform.SCHED,
        },
      });

      isVisible.value = false;
    })
    .finally(() => {
      loading.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfSchedSettingsDrawer',
};
</script>
