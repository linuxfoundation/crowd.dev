export enum Widgets {
  ACTIVE_CONTRIBUTORS = 'activeContributors',
  ACTIVE_ORGANIZATION = 'activeOrganization',
  CONTRIBUTORS_LEADERBOARD = 'contributorsLeaderboard',
  ORGANIZATIONS_LEADERBOARD = 'organizationsLeaderboard',
  CONTRIBUTOR_DEPENDENCY = 'contributorDependency',
  ORGANIZATION_DEPENDENCY = 'organizationDependency',
  RETENTION = 'retention',
  GEOGRAPHICAL_DISTRIBUTION = 'geographicalDistribution',
  STARS = 'stars',
  FORKS = 'forks',
  // SOCIAL_MENTIONS = 'socialMentions',
  // GITHUB_MENTIONS = 'githubMentions',
  // PRESS_MENTIONS = 'pressMentions',
  SEARCH_QUERIES = 'searchQueries',
  PACKAGE_DOWNLOADS = 'packageDownloads',
  PACKAGE_DEPENDENCY = 'packageDependency',
  MAILING_LIST_MESSAGES = 'mailingListMessages',
  ISSUES_RESOLUTION = 'issuesResolution',
  COMMIT_ACTIVITIES = 'commitActivities',
  PULL_REQUESTS = 'pullRequests',
  ACTIVE_DAYS = 'activeDays',
  CONTRIBUTIONS_OUTSIDE_WORK_HOURS = 'contributionsOutsideWorkHours',
  MERGE_LEAD_TIME = 'mergeLeadTime',
  REVIEW_TIME_BY_PULL_REQUEST_SIZE = 'reviewTimeByPullRequestSize',
  AVERAGE_TIME_TO_MERGE = 'averageTimeToMerge',
  WAIT_TIME_FOR_1ST_REVIEW = 'waitTimeFor1stReview',
  CODE_REVIEW_ENGAGEMENT = 'codeReviewEngagement',
  REVIEW_EFFICIENCY = 'reviewEfficiency',
  PATCHSET_PER_REVIEW = 'patchsetPerReview',
  MEDIAN_TIME_TO_CLOSE = 'medianTimeToClose',
  MEDIAN_TIME_TO_REVIEW = 'medianTimeToReview',
}

export const WIDGETS_GROUPS = [
  {
    name: 'Contributors',
    widgets: [
      {
        name: 'Active contributors',
        key: Widgets.ACTIVE_CONTRIBUTORS,
      },
      {
        name: 'Active company',
        key: Widgets.ACTIVE_ORGANIZATION,
      },
      {
        name: 'Contributors leaderboard',
        key: Widgets.CONTRIBUTORS_LEADERBOARD,
      },
      {
        name: 'Companies leaderboard',
        key: Widgets.ORGANIZATIONS_LEADERBOARD,
      },
      {
        name: 'Contributor dependency',
        key: Widgets.CONTRIBUTOR_DEPENDENCY,
      },
      {
        name: 'Company dependency',
        key: Widgets.ORGANIZATION_DEPENDENCY,
      },
      {
        name: 'Retention',
        key: Widgets.RETENTION,
      },
      {
        name: 'Geographical distribution',
        key: Widgets.GEOGRAPHICAL_DISTRIBUTION,
      },
    ],
  },
  {
    name: 'Popularity',
    widgets: [
      {
        name: 'Stars',
        key: Widgets.STARS,
      },
      {
        name: 'Forks',
        key: Widgets.FORKS,
      },
      // NOTE: Not implemented yet
      // {
      //   name: 'Social mentions',
      //   key: Widgets.SOCIAL_MENTIONS,
      // },
      // {
      //   name: 'GitHub Mentions',
      //   key: Widgets.GITHUB_MENTIONS,
      // },
      // {
      //   name: 'Press mentions',
      //   key: Widgets.PRESS_MENTIONS,
      // },
      {
        name: 'Search queries',
        key: Widgets.SEARCH_QUERIES,
      },
      {
        name: 'Package downloads',
        key: Widgets.PACKAGE_DOWNLOADS,
      },
      {
        name: 'Package dependency',
        key: Widgets.PACKAGE_DEPENDENCY,
      },
      {
        name: 'Mailing lists messages',
        key: Widgets.MAILING_LIST_MESSAGES,
      },
    ],
  },
  {
    name: 'Development',
    widgets: [
      {
        name: 'Issues resolution',
        key: Widgets.ISSUES_RESOLUTION,
      },
      {
        name: 'Commit activities',
        key: Widgets.COMMIT_ACTIVITIES,
      },
      {
        name: 'Pull requests',
        key: Widgets.PULL_REQUESTS,
      },
      {
        name: 'Active days',
        key: Widgets.ACTIVE_DAYS,
      },
      {
        name: 'Contributions outside work hours',
        key: Widgets.CONTRIBUTIONS_OUTSIDE_WORK_HOURS,
      },
      {
        name: 'Merge lead time',
        key: Widgets.MERGE_LEAD_TIME,
      },
      {
        name: 'Review time by pull request size',
        key: Widgets.REVIEW_TIME_BY_PULL_REQUEST_SIZE,
      },
      {
        name: 'Code review engagement',
        key: Widgets.CODE_REVIEW_ENGAGEMENT,
      },
      {
        name: 'Review efficiency',
        key: Widgets.REVIEW_EFFICIENCY,
      },
      {
        name: 'Patchsets per review',
        key: Widgets.PATCHSET_PER_REVIEW,
      },
      {
        name: 'Median time to close',
        key: Widgets.MEDIAN_TIME_TO_CLOSE,
      },
      {
        name: 'Median time to review',
        key: Widgets.MEDIAN_TIME_TO_REVIEW,
      },
    ],
  },
];

export const getDefaultWidgets = (): Widgets[] => WIDGETS_GROUPS.flatMap((group) => group.widgets.map((widget) => widget.key));
