// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface RedditSubredditForm {
  value: string;
  validating?: boolean;
  touched?: boolean;
  valid?: boolean;
  loading?: boolean;
}

export interface RedditSettings {
  subreddits: string[];
}
