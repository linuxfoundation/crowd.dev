import {
  DATA_SINK_WORKER_PARTITIONS,
  INTEGRATION_RUN_WORKER_PARTITIONS,
  INTEGRATION_STREAM_WORKER_PARTITIONS,
  SEARCH_SYNC_WORKER_PARTITIONS,
} from '@crowd/common'

import { CrowdQueue } from '../../types'
import { IKafkaChannelConfig } from './types'

export const INTEGRATION_RUN_WORKER_QUEUE_SETTINGS: IKafkaChannelConfig = {
  name: CrowdQueue.INTEGRATION_RUN_WORKER,
  replicationFactor: 1,
  partitions: {
    default: 1,
    ...INTEGRATION_RUN_WORKER_PARTITIONS,
  },
}

export const DATA_SINK_WORKER_QUEUE_SETTINGS: IKafkaChannelConfig = {
  name: CrowdQueue.DATA_SINK_WORKER,
  replicationFactor: 1,
  partitions: {
    default: 1,
    ...DATA_SINK_WORKER_PARTITIONS,
  },
}

export const SEARCH_SYNC_WORKER_QUEUE_SETTINGS: IKafkaChannelConfig = {
  name: CrowdQueue.SEARCH_SYNC_WORKER,
  replicationFactor: 1,
  partitions: {
    default: 1,
    ...SEARCH_SYNC_WORKER_PARTITIONS,
  },
}

export const INTEGRATION_STREAM_WORKER_QUEUE_SETTINGS: IKafkaChannelConfig = {
  name: CrowdQueue.INTEGRATION_STREAM_WORKER,
  replicationFactor: 1,
  partitions: {
    default: 1,
    ...INTEGRATION_STREAM_WORKER_PARTITIONS,
  },
}

export const ACTIVITIES_QUEUE_SETTINGS: IKafkaChannelConfig = {
  name: CrowdQueue.ACTIVITIES,
  useOnlyNameAsChannel: true,
  replicationFactor: 1,
  partitions: {
    default: 1,
  },
}

export const configMap = {
  [CrowdQueue.INTEGRATION_RUN_WORKER]: INTEGRATION_RUN_WORKER_QUEUE_SETTINGS,
  [CrowdQueue.INTEGRATION_STREAM_WORKER]: INTEGRATION_STREAM_WORKER_QUEUE_SETTINGS,
  [CrowdQueue.DATA_SINK_WORKER]: DATA_SINK_WORKER_QUEUE_SETTINGS,
  [CrowdQueue.SEARCH_SYNC_WORKER]: SEARCH_SYNC_WORKER_QUEUE_SETTINGS,
}
