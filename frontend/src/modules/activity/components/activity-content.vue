<template>
  <div class="flex gap-10">
    <div class="flex-grow">
      <div v-if="(activity.title || displayTitleBody) || activity.body">
        <!-- @vue-expect-error legacy always-true comparison kept for parity -->
        <div
          v-if="
            activity.title
              && (!activity.parent
                || !activity.parent.body
                || !activity.parent.body !== activity.title)
              && displayTitle
          "
        >
          <span
            class="block"
            :class="{
              title: !titleClasses,
              [titleClasses]: titleClasses,
            }"
            v-html="
              contentRenderEmojis(
                $sanitize($marked(activity.title),
                ))
            "
          />
        </div>

        <div v-if="activity.display?.default && displayTitleBody">
          <div
            class="first-letter:uppercase font-semibold text-gray-900 text-sm mb-1"
            v-html="
              contentRenderEmojis(
                $sanitize($marked(activity.display.default)),
              )
            "
          />
        </div>

        <div
          v-if="activity.title && activity.body && displayTitle"
          class="mt-3"
        />
        <div class="content">
          <div v-if="activity.body">
            <blockquote
              v-if="activity.thread && displayThread"
              class="relative px-3 border-l-4 text-gray-500 border-gray-200 text-xs leading-5 mb-4 parsed-body"
              v-html="$sanitize($marked(activity.thread.body))"
            />
            <span
              v-if="
                activity.type === 'reaction_added'
                  && displayBody
              "
              v-html="contentRenderEmojis(`:${activity.body}:`)"
            />
            <span
              v-else-if="displayBody"
              ref="body"
              class="block custom-break-all activity-body parsed-body c-content"
              :class="
                showMore && !more ? `line-clamp-${limit}` : ''
              "
              v-html="
                contentRenderEmojis(
                  $sanitize($marked(`<div class='whitespace-pre-wrap' style='word-break: break-word'>${activity.body}</div>`)),
                )
              "
            />
          </div>
        </div>
        <div class="flex justify-between items-center">
          <div>
            <div
              v-if="displayShowMore"
              class="text-sm text-primary-500 mt-6 cursor-pointer"
              @click.stop="more = !more"
            >
              Show {{ more ? 'less' : 'more' }}
            </div>
          </div>
          <div>
            <slot name="bottomLink" />
          </div>
        </div>
        <div class="mt-2">
          <slot name="details" />
        </div>
      </div>
      <div v-else-if="activity.display?.default">
        <div
          class="first-letter:uppercase font-semibold text-gray-900 text-sm"
          v-html="
            contentRenderEmojis(
              $sanitize($marked(activity.display.default)),
            )
          "
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import emoji from 'node-emoji';
import type { Activity } from '@/shared/modules/activity/types/Activity';

defineOptions({ name: 'AppActivityContent' });

const props = withDefaults(defineProps<{
  activity: Activity & { thread?: { body: string } };
  titleClasses?: string;
  showMore?: boolean;
  limit?: number;
  displayTitle?: boolean;
  displayThread?: boolean;
  displayBody?: boolean;
  displayTitleBody?: boolean;
}>(), {
  titleClasses: '',
  showMore: false,
  limit: 4,
  displayTitle: true,
  displayThread: true,
  displayBody: true,
  displayTitleBody: false,
});

const body = ref<HTMLElement | null>(null);
const more = ref(false);
const displayShowMore = ref(false);

onMounted(() => {
  if (props.showMore && body.value) {
    displayShowMore.value = body.value.scrollHeight > body.value.clientHeight;
  }
});

const contentRenderEmojis = (content: string) => emoji.emojify(
  content,
  () => '<abbr class=“no-underline” title=“Unable to detect emoji”>&#65533;</abbr>',
);
</script>

<style scoped lang="scss">
:deep(.content) {
  a {
    @apply text-primary-500;
  }

  h1,
  h2,
  h3,
  h4,
  h5,
  h6 {
    font-size: 16px;
  }

  .emoji {
    @apply w-4 h-4 inline;
  }
}

.title {
  @apply text-sm font-medium;
}
</style>
