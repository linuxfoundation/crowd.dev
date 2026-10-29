import type { SourceCounter } from '../../../types'
import { githubCheapCounters } from './cheap'

export const githubCounters: SourceCounter[] = [...githubCheapCounters]
