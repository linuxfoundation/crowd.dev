import type { SourceCounter } from '../../../types'
import { githubCheapCounters } from './cheap'
import { githubPrTimelineCounters } from './prTimeline'

export const githubCounters: SourceCounter[] = [...githubCheapCounters, ...githubPrTimelineCounters]
