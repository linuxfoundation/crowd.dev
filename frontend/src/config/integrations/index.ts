import type { Component } from 'vue';
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
import octolens from './octolens/config';
import credly from './credly/config';

export interface ActionRequiredMessage {
  key: string;
  text: string;
}
export interface IntegrationConfig {
  key: string; // Unique key for the integration
  name: string; // Display name of the integration
  image: string; // Image URL for the integration
  description: string; // Description of the integration
  link?: string; // Documentation link for the integration
  connectComponent?: Component; // Component rendered for user to connect integration
  actionComponent?: Component; // Component rendered when integration needs user action
  statusComponent?: Component; // Component rendered to show integration status
  connectedParamsComponent?: Component; // Component rendered to show connected integration params (repositories, channels)
  dropdownComponent?: Component; // Component rendered inside dropdown for extra options
  settingComponent?: Component; // Component rendered next to dropdown for extra options
  mappedReposComponent?: Component; // Component rendered to show mapped repositories
  showProgress: boolean; // Show progress bar when connecting
  actionRequiredMessage?: ActionRequiredMessage[];
  autoManaged?: boolean; // Configured automatically by the platform; never offered for users to connect
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
  octolens,
  credly,
});
