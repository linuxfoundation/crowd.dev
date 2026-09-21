/**
 * Project Onboarding Script
 *
 * This script automates the onboarding of multiple open-source projects from a CSV file
 * into the CDP platform. It performs the following operations for each project:
 *
 * 1. Creates a new project in the specified project group (LF OSS Index)
 * 2. Sets up GitHub integration for the project's repository
 *
 * Features:
 * - Batch processing of projects from CSV input
 * - Dry run mode for testing (processes only the first project)
 * - Error handling and detailed logging
 * - Console output of failed projects for easy troubleshooting
 *
 * Usage:
 *   tsx src/bin/onboard-projects.ts <bearer-token> <github-token> <csv-file-path> [--dry-run]
 *
 * CSV Format:
 *   project name,project slug,repo url
 *   My Project,my-project,https://github.com/owner/repo
 *
 * Environment Variables Required:
 *   CROWD_API_SERVICE_URL - Base URL for the CDP API service
 */
import axios from 'axios'
import { parse } from 'csv-parse'
import fs from 'fs'
import path from 'path'

import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('onboard-projects-script')

interface ProjectRow {
  name: string
  slug: string
  repoUrl: string
}

interface FailedProjectRow extends ProjectRow {
  reason: string
}

interface ProjectSubproject {
  id: string
  name: string
  slug: string
  [key: string]: unknown
}

interface ProjectResponse {
  id: string
  name: string
  slug: string
  subprojects: ProjectSubproject[]
  [key: string]: unknown
}

const LF_OSS_INDEX_PROJECT_GROUP_SLUG = 'lf-oss-index'

/**
 * Main function to onboard projects from a CSV file
 *
 * Reads a CSV file containing project information, validates the data,
 * and processes each project by creating the project and setting up
 * GitHub integration.
 *
 * @param csvFilePath - Path to the CSV file containing project data
 * @param bearerToken - Authentication token for CDP API calls
 * @param githubToken - GitHub token for API calls
 * @param isDryRun - If true, only processes the first project for testing
 * @returns Promise resolving to results summary including success/failure counts and failed projects
 */
async function onboardProjectsFromCsv(
  csvFilePath: string,
  bearerToken: string,
  githubToken: string,
  isDryRun = false,
): Promise<{
  successCount: number
  failureCount: number
  errors: string[]
  failedProjects: FailedProjectRow[]
}> {
  log.info(`Starting project onboarding from CSV: ${csvFilePath}`)

  const projects: ProjectRow[] = []
  const failedProjects: FailedProjectRow[] = []
  const errors: string[] = []

  // Read and parse CSV file
  try {
    const csvData = fs.readFileSync(csvFilePath, 'utf8')
    const records = await new Promise<Record<string, string>[]>((resolve, reject) => {
      parse(
        csvData,
        {
          columns: true,
          skip_empty_lines: true,
          trim: true,
        },
        (err: Error | null, output: Record<string, string>[]) => {
          if (err) reject(err)
          else resolve(output)
        },
      )
    })

    for (const record of records) {
      if (record['project name'] && record['project slug'] && record['repo url']) {
        projects.push({
          name: record['project name'].trim(),
          slug: record['project slug'].trim(),
          repoUrl: record['repo url'].trim(),
        })
      } else {
        failedProjects.push({
          name: record['project name'].trim(),
          slug: record['project slug'].trim(),
          repoUrl: record['repo url'].trim(),
          reason: 'invalid row',
        })
        log.warn(`Skipping invalid row: ${JSON.stringify(record)}`)
        errors.push(`Invalid row missing required fields: ${JSON.stringify(record)}`)
      }
    }

    if (isDryRun && projects.length > 0) {
      const firstProject = projects[0]
      projects.splice(1)
      log.info(`DRY RUN: Processing only first project: ${firstProject.name}`)
    } else {
      log.info(`Loaded ${projects.length} projects from CSV`)
    }
  } catch (error) {
    const errorMsg = `Failed to read or parse CSV file: ${error.message}`
    log.error(error, errorMsg)
    errors.push(errorMsg)
    return { successCount: 0, failureCount: 0, errors, failedProjects: [] }
  }

  // Process each project
  let successCount = 0
  for (let i = 0; i < projects.length; i++) {
    const project = projects[i]
    let segmentId = ''
    log.info(`Processing project ${i + 1}/${projects.length}: ${project.name}`)

    try {
      // Create project
      segmentId = await createProject(project, bearerToken)
      log.info(`Created project ${project.name} with segment ID: ${segmentId}`)
    } catch (error) {
      const errorMsg = `Failed to create project ${project.name}: ${error.message}`
      log.error(error, errorMsg)
      errors.push(errorMsg)
      failedProjects.push({ ...project, reason: 'project creation' })
    }

    // Don't create integration if project creation failed
    if (segmentId) {
      try {
        // Create GitHub integration
        await createGithubIntegration(project, segmentId, bearerToken, githubToken)
        log.info(`Created GitHub integration for project ${project.name}`)

        successCount++
      } catch (error) {
        const errorMsg = `Failed to process project ${project.name}: ${error.message}`
        log.error(error, errorMsg)
        errors.push(errorMsg)
        failedProjects.push({ ...project, reason: 'integration creation' })
      }
    }

    // Wait 15 seconds before processing next project (except for the last one)
    if (i < projects.length - 1) {
      log.info('Waiting 15 seconds before processing next project...')
      await new Promise((resolve) => setTimeout(resolve, 15000))
    }
  }

  const result = {
    successCount,
    failureCount: failedProjects.length,
    errors,
    failedProjects,
  }

  log.info(`Onboarding completed: ${successCount} successful, ${failedProjects.length} failed`)
  return result
}

/**
 * Creates a new project in the CDP platform
 *
 * Makes an API call to create a project within the LF OSS Index project group.
 * The project is created as a non-LF project with the specified name and slug.
 *
 * @param project - Project data containing name, slug, and repository URL
 * @param bearerToken - Authentication token for API authorization
 * @returns Promise resolving to the segment ID of the created project
 * @throws Error if project creation fails or returns invalid response
 */
async function createProject(project: ProjectRow, bearerToken: string): Promise<string> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/segment/project`

  const projectData = {
    name: project.name,
    slug: project.slug,
    isLF: false,
    parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG,
  }

  try {
    await axios.post(url, projectData, {
      headers: {
        Authorization: `Bearer ${bearerToken}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      timeout: 30000,
    })

    log.info('Project creation completed')

    // Query the project by slug to get the segment ID
    // Using this workaround because the request above not always returns a response payload
    const projectResponse = await queryProjectByName(project.name, bearerToken)
    if (projectResponse?.subprojects?.[0]) {
      log.info(
        `Successfully retrieved project ${project.name} with segment ID: ${projectResponse.subprojects[0].id}`,
      )
      return projectResponse.subprojects[0].id
    }

    throw new Error(
      'project creation: Invalid response from project creation API and failed to query created project',
    )
  } catch (error) {
    if (error.response) {
      throw new Error(
        `project creation: HTTP ${error.response.status} - ${error.response.data?.message || error.response.statusText}`,
      )
    }
    throw new Error(`project creation: ${error.message}`)
  }
}

/**
 * Queries for a project by slug to retrieve its segment information
 *
 * @param name - Project name to search for
 * @param bearerToken - Authentication token for API authorization
 * @returns Promise resolving to the project data or null if not found
 */
async function queryProjectByName(
  name: string,
  bearerToken: string,
): Promise<ProjectResponse | null> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/segment/project/query`

  try {
    const response = await axios.post(
      url,
      {
        filter: {
          name,
          parentSlug: LF_OSS_INDEX_PROJECT_GROUP_SLUG,
        },
        limit: 1,
        offset: 0,
      },
      {
        headers: {
          Authorization: `Bearer ${bearerToken}`,
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        timeout: 30000,
      },
    )

    if (response.data?.rows?.length > 0) {
      return response.data.rows[0]
    }

    return null
  } catch (error) {
    log.error(`Failed to query project by slug ${name}: ${error.message}`)
    return null
  }
}

/**
 * Fetches the GitHub organization or user avatar URL
 *
 * Makes an API call to GitHub's users endpoint to retrieve the organization's
 * or user's avatar URL for use as a logo in the integration settings.
 *
 * @param owner - GitHub organization or user name
 * @param bearerToken - Authentication token for GitHub API calls
 * @returns Promise resolving to the avatar URL, or empty string if fetch fails
 */
async function fetchGithubOrgLogo(owner: string, bearerToken: string): Promise<string> {
  try {
    const response = await axios.get(`https://api.github.com/users/${owner}`, {
      headers: {
        Authorization: `Bearer ${bearerToken}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      timeout: 10000,
    })

    return response.data.avatar_url || ''
  } catch (error) {
    log.warn(`Failed to fetch GitHub logo for ${owner}: ${error.message}`)
    return ''
  }
}

/**
 * Fetches repository information to determine if it's a fork
 *
 * Makes an API call to GitHub's repos endpoint to retrieve repository details,
 * including fork status and parent repository information.
 *
 * @param owner - GitHub organization or user name
 * @param repo - Repository name
 * @param bearerToken - Authentication token for GitHub API calls
 * @returns Promise resolving to the parent repository URL if forked, null otherwise
 */
async function fetchGithubRepoForkInfo(
  owner: string,
  repo: string,
  bearerToken: string,
): Promise<string | null> {
  try {
    const response = await axios.get(`https://api.github.com/repos/${owner}/${repo}`, {
      headers: {
        Authorization: `Bearer ${bearerToken}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      timeout: 10000,
    })

    if (response.data.fork && response.data.parent) {
      let forkedFrom = response.data.parent.html_url
      if (forkedFrom.endsWith('github.com/torvalds/linux')) {
        // use git url instead of github as it's the one onboarded in our system
        forkedFrom = 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux'
      }
      return forkedFrom
    }

    return null
  } catch (error) {
    log.warn(`Failed to fetch fork info for ${owner}/${repo}: ${error.message}`)
    return null
  }
}

/**
 * Creates a GitHub integration for the specified project
 *
 * Sets up GitHub repository integration by parsing the repository URL,
 * creating integration settings with repository mapping, and linking
 * it to the project segment.
 *
 * @param project - Project data containing repository URL and metadata
 * @param segmentId - The segment ID of the created project to link integration to
 * @param bearerToken - Authentication token for API authorization
 * @returns Promise that resolves when integration is successfully created
 * @throws Error if integration creation fails or returns invalid response
 */
async function createGithubIntegration(
  project: ProjectRow,
  segmentId: string,
  bearerToken: string,
  githubToken: string,
): Promise<void> {
  // Parse GitHub repo URL to extract owner and repo name
  const { owner, repo } = parseGithubUrl(project.repoUrl)

  // Fetch organization logo and fork information
  const orgLogo = await fetchGithubOrgLogo(owner, githubToken)
  const forkedFrom = await fetchGithubRepoForkInfo(owner, repo, githubToken)

  // Create integration
  const integrationUrl = `${process.env['CROWD_API_SERVICE_URL']}/github-nango-connect`

  const integrationData = {
    settings: {
      orgs: [
        {
          name: owner,
          url: project.repoUrl,
          logo: orgLogo,
          fullSync: false,
          updatedAt: new Date().toISOString(),
          repos: [
            {
              name: repo,
              url: project.repoUrl,
              forkedFrom,
              updatedAt: new Date().toISOString(),
            },
          ],
        },
      ],
      updateMemberAttributes: true,
    },
    mapping: {
      [project.repoUrl]: segmentId,
    },
    segments: [segmentId],
  }

  try {
    const response = await axios.post(integrationUrl, integrationData, {
      headers: {
        Authorization: `Bearer ${bearerToken}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      timeout: 30000,
    })

    if (!response.data || !response.data.id) {
      throw new Error('integration creation: Invalid response from integration creation API')
    }

    log.info(`Created integration ${response.data.id} for ${owner}/${repo}`)
  } catch (error) {
    if (error.response) {
      throw new Error(
        `integration creation: HTTP ${error.response.status} - ${error.response.data?.message || error.response.statusText}`,
      )
    }
    throw new Error(`integration creation: ${error.message}`)
  }
}

/**
 * Parses a GitHub repository URL to extract owner and repository name
 *
 * Handles various GitHub URL formats including HTTPS and SSH URLs.
 * Normalizes SSH URLs to HTTPS format for consistent parsing.
 *
 * @param repoUrl - GitHub repository URL (HTTPS or SSH format)
 * @returns Object containing the repository owner and name
 * @throws Error if URL format is invalid or cannot be parsed
 *
 * @example
 * parseGithubUrl('https://github.com/owner/repo') // { owner: 'owner', repo: 'repo' }
 * parseGithubUrl('git@github.com:owner/repo.git') // { owner: 'owner', repo: 'repo' }
 */
function parseGithubUrl(repoUrl: string): { owner: string; repo: string } {
  try {
    // Handle different GitHub URL formats
    const url = new URL(repoUrl.replace('git@github.com:', 'https://github.com/'))
    const pathParts = url.pathname
      .replace(/^\//, '')
      .replace(/\.git$/, '')
      .split('/')

    if (pathParts.length < 2) {
      throw new Error('Invalid GitHub URL format')
    }

    return {
      owner: pathParts[0],
      repo: pathParts[1],
    }
  } catch (error) {
    throw new Error(`Failed to parse GitHub URL "${repoUrl}": ${error.message}`)
  }
}

/**
 * Main entry point for the project onboarding script
 *
 * Handles command-line argument parsing, file validation, and orchestrates
 * the project onboarding process. Displays results and error information
 * to the console upon completion.
 *
 * Command-line arguments:
 * - bearerToken: Authentication token for API calls
 * - csvFilePath: Path to CSV file containing project data
 * - --dry-run: Optional flag to process only the first project
 *
 * Exits with code 0 on success, 1 if any projects failed to onboard
 */
async function main() {
  const args = process.argv.slice(2)

  if (args.length < 3 || args.length > 4) {
    log.error(`
      Usage: tsx src/bin/onboard-projects.ts <bearer-token> <github-token> <csv-file-path> [--dry-run]
      
      Arguments:
        bearer-token: Bearer token for CDP API authentication
        github-token: GitHub token for fetching organization logos
        csv-file-path: Path to CSV file containing projects to onboard
        --dry-run: (optional) Test mode - only process the first project from CSV
      
      CSV Format:
        The CSV file should have the following columns:
        - project name: Project name
        - project slug: Project slug (will be namespaced for non-LF projects)
        - repo url: GitHub repository URL
      
      Example CSV content:
        project name,project slug,repo url
        My Project,my-project,https://github.com/owner/repo
        Another Project,another-project,git@github.com:owner/another-repo.git
    `)
    process.exit(1)
  }

  const [bearerToken, githubToken, csvFilePath, dryRunFlag] = args
  const isDryRun = dryRunFlag === '--dry-run'

  // Validate file exists
  const resolvedPath = path.resolve(csvFilePath)
  try {
    fs.accessSync(resolvedPath, fs.constants.F_OK)
  } catch {
    log.error(`Error: CSV file not found at path: ${resolvedPath}`)
    process.exit(1)
  }

  if (isDryRun) {
    log.info(`Starting DRY RUN - will only process first project from CSV: ${resolvedPath}`)
  } else {
    log.info(`Starting project onboarding from CSV: ${resolvedPath}`)
  }

  try {
    // Run the onboarding function directly
    const result = await onboardProjectsFromCsv(resolvedPath, bearerToken, githubToken, isDryRun)

    log.info(`
      Onboarding completed successfully: ${result.successCount} successful, ${result.failureCount} failed
    
      === Onboarding Results ===
      ✅ Successfully onboarded: ${result.successCount} projects
      ❌ Failed to onboard: ${result.failureCount} projects
    `)

    if (result.failedProjects.length > 0) {
      log.info(`=== Failed Projects ===`)
      result.failedProjects.forEach((project, index) => {
        log.info(`
          ${index + 1}. ${project.name} (${project.slug}) - ${project.reason}
            Repo: ${project.repoUrl}
        `)
      })
    }

    log.info(`
      === Summary ===
      ${isDryRun ? '🧪 DRY RUN MODE: Only first project was processed for testing' : ''}
      Total processed: ${result.successCount + result.failureCount}
      Success rate: ${result.successCount + result.failureCount > 0 ? ((result.successCount / (result.successCount + result.failureCount)) * 100).toFixed(1) : '0'}%
    `)

    process.exit(result.failureCount > 0 ? 1 : 0)
  } catch (error) {
    log.error(error, 'Failed to run onboarding')
    log.error(`\n❌ Error: ${error.message}`)
    process.exit(1)
  }
}

main().catch((error) => {
  log.error('Unexpected error:', error)
  process.exit(1)
})
