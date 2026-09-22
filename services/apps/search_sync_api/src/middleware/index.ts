import { Request } from 'express'

import { IDatabaseRequest } from './database'
import { ILoggingRequest } from './logging'
import { IOpenSearchRequest } from './opensearch'
import { IRedisRequest } from './redis'

export interface ApiRequest
  extends Request, ILoggingRequest, IOpenSearchRequest, IRedisRequest, IDatabaseRequest {}
