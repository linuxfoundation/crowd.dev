export interface IPriorityConfig {
  /** Maximum number of projects in the 'evaluate' queue at any time. */
  evaluateLimit: number
  /**
   * Ordered list of source names by descending priority.
   * Sources not in this list rank below all listed ones.
   */
  sourcePriority: string[]
}

export interface IEvaluateProjectsInput {
  batchSize?: number
  priorityConfig?: IPriorityConfig
}
