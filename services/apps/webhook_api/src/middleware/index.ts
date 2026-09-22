import { Request } from 'express'

import { IDatabaseRequest } from './database'
import { IEmittersRequest } from './emitters'
import { ILoggingRequest } from './logging'
import { IQueueRequest } from './queue'

export interface ApiRequest
  extends Request, ILoggingRequest, IDatabaseRequest, IQueueRequest, IEmittersRequest {}
