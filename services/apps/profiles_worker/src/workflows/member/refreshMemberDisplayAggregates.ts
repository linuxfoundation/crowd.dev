import {
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  executeChild,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../../activities'
import { IRefreshDisplayAggregatesArgs } from '../../types/common'
import { calculateProjectGroupMemberAggregates } from './calculateProjectGroupMemberAggregates'
import { calculateProjectMemberAggregates } from './calculateProjectMemberAggregates'

const { getSegmentHierarchy } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
})

/*
  Temporal workflow to refresh member aggregates for project and project group segments.

  This workflow rolls up member aggregates from subproject (leaf) segments to:
  1. Project segments - aggregated from child subprojects
  2. Project group segments - aggregated from all subprojects in the group

  Note: Subproject (leaf) level aggregates are populated via Kafka Connect from Tinybird.

  The workflow spawns child workflows for each project group, which in turn process
  each project within them. This provides:
  - Individual failure tracking per segment in Temporal UI
  - Automatic retries at the segment level
  - Better observability into which specific segments fail
*/
export async function refreshMemberDisplayAggregates(
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  _args: IRefreshDisplayAggregatesArgs = {},
): Promise<void> {
  console.log('Starting member aggregates refresh workflow')

  // Load segment hierarchy
  const hierarchy = await getSegmentHierarchy()

  const info = workflowInfo()

  console.log(
    `Found ${hierarchy.projectGroupSegments.length} project groups and ${hierarchy.projectSegments.length} projects`,
  )

  // Process all project groups in parallel
  const projectGroupPromises = hierarchy.projectGroupSegments.map(async (projectGroup) => {
    const projectGroupName = hierarchy.segmentNames[projectGroup.id] || projectGroup.id
    const subprojectIds = hierarchy.subprojectsByGrandparent[projectGroup.id] || []

    if (subprojectIds.length === 0) {
      console.log(`Skipping project group "${projectGroupName}" - no subprojects`)
      return
    }

    console.log(
      `Processing project group "${projectGroupName}" with ${subprojectIds.length} subprojects`,
    )

    // Find all projects that belong to this project group
    const projectsInGroup = hierarchy.projectSegments.filter(
      (p) => hierarchy.projectToProjectGroup[p.id] === projectGroup.id,
    )

    // First, process all projects within this project group in parallel
    const projectPromises = projectsInGroup.map((project) => {
      const projectName = hierarchy.segmentNames[project.id] || project.id
      const projectSubprojectIds = hierarchy.subprojectsByParent[project.id] || []

      if (projectSubprojectIds.length === 0) {
        console.log(`Skipping project "${projectName}" - no subprojects`)
        return Promise.resolve(0)
      }

      return executeChild(calculateProjectMemberAggregates, {
        workflowId: `${info.workflowId}/project/${project.id}`,
        cancellationType: ChildWorkflowCancellationType.ABANDON,
        parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
        retry: {
          backoffCoefficient: 2,
          initialInterval: 2 * 1000,
          maximumInterval: 30 * 1000,
          maximumAttempts: 3,
        },
        args: [
          {
            projectId: project.id,
            projectName,
            subprojectIds: projectSubprojectIds,
          },
        ],
      })
    })

    // Wait for all projects in this group to complete
    await Promise.all(projectPromises)

    // Then, process the project group itself (aggregates from all subprojects)
    await executeChild(calculateProjectGroupMemberAggregates, {
      workflowId: `${info.workflowId}/project-group/${projectGroup.id}`,
      cancellationType: ChildWorkflowCancellationType.ABANDON,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      retry: {
        backoffCoefficient: 2,
        initialInterval: 2 * 1000,
        maximumInterval: 30 * 1000,
        maximumAttempts: 3,
      },
      args: [
        {
          projectGroupId: projectGroup.id,
          projectGroupName,
          subprojectIds,
        },
      ],
    })

    console.log(`Completed project group "${projectGroupName}"`)
  })

  // Wait for all project groups to complete
  await Promise.all(projectGroupPromises)

  console.log('Member aggregates refresh workflow completed')
}
