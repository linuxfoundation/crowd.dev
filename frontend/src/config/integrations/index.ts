import config from '@/config';
import github from './github/config';
import githubNango from './github-nango/config';
import git from './git/config';
import groupsio from './groupsio/config';
import confluence from './confluence/config';
import jira from './jira/config';
import slack from './slack/config';
import discord from './discord/config';
import linkedin from './linkedin/config';
import twitter from './twitter/config';
import reddit from './reddit/config';
import hackernews from './hackernews/config';
import stackoverflow from './stackoverflow/config';
import gitlab from './gitlab/config';
import gerrit from './gerrit/config';
import discourse from './discourse/config';
import devto from './devto/config';

export interface ActionRequiredMessage {
  key: string
  text: string
}
export interface IntegrationConfig {
  key: string // Unique key for the integration
  name: string // Display name of the integration
  image: string // Image URL for the integration
  description: string // Description of the integration
  link?: string // Documentation link for the integration
  connectComponent?: Vue.Component // Component rendered for user to connect integration
  actionComponent?: Vue.Component // Component rendered when integration needs user action
  statusComponent?: Vue.Component // Component rendered to show integration status
  connectedParamsComponent?: Vue.Component // Component rendered to show connected integration params (repositories, channels)
  dropdownComponent?: Vue.Component // Component rendered inside dropdown for extra options
  settingComponent?: Vue.Component // Component rendered next to dropdown for extra options
  mappedReposComponent?: Vue.Component // Component rendered to show mapped repositories
  showProgress: boolean // Show progress bar when connecting
  actionRequiredMessage?: ActionRequiredMessage[]
}

export const getGithubIntegration = () => {
  if (config.env === 'staging') {
    const useGitHubNango = localStorage.getItem('useGitHubNango') === 'true';

    return useGitHubNango ? githubNango : github;
  }

  return github;
};

export const lfIntegrations: (useGitHubNango?: boolean) => Record<string, IntegrationConfig> = (
  useGitHubNango?: boolean,
) => ({
  github: useGitHubNango ? githubNango : getGithubIntegration(),
  git,
  gitlab,
  gerrit,
  groupsio,
  confluence,
  jira,
  slack,
  discord,
  linkedin,
  twitter,
  reddit,
  hackernews,
  stackoverflow,
  discourse,
  devto,
});
