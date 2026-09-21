import { NextFunction } from 'express'
import { Namespace, Server } from 'socket.io'

import { Logger, getServiceChildLogger } from '@crowd/logging'

import { databaseInit } from '../../database/databaseConnection'
import SequelizeRepository from '../../database/repositories/sequelizeRepository'
import TenantUserRepository from '../../database/repositories/tenantUserRepository'
import AuthService from '../../services/auth/authService'
import { IAuthenticatedSocket, ISocket, ISocketHandler } from './types'

const logger = getServiceChildLogger('websockets/namespaces')

export default class WebSocketNamespace<TSocket extends ISocket = ISocket> {
  private readonly log: Logger

  public readonly socketIoNamespace: Namespace

  public constructor(
    socketIoServer: Server,
    public readonly namespace: string,
    public readonly authenticated: boolean,
  ) {
    this.log = logger.child({ namespace }, true)

    this.socketIoNamespace = socketIoServer.of(namespace)

    // database middleware
    this.socketIoNamespace.use(async (socket: any, next: NextFunction) => {
      try {
        socket.database = await databaseInit()
        next()
      } catch (err) {
        this.log.error(err, 'Database connection error!')
        next(err)
      }
    })

    if (authenticated) {
      // auth middleware
      this.socketIoNamespace.use(async (socket: any, next: NextFunction) => {
        try {
          if (socket.handshake.query && socket.handshake.query.token) {
            socket.user = await AuthService.findByToken(socket.handshake.query.token, socket)
            next()
          } else {
            next(new Error('Authentication error'))
          }
        } catch (err) {
          this.log.error(err, 'WebSockets authentication error!')
          next(err)
        }
      })
    }

    // handle connect
    this.socketIoNamespace.on('connection', (socket: ISocket) => {
      if (authenticated) {
        this.log.info(
          { userId: (socket as IAuthenticatedSocket).user.id },
          'Authenticated user connected!',
        )
        // add to user room if we need to send a notification to this user only
        socket.join(`user-${(socket as IAuthenticatedSocket).user.id}`)
      } else {
        this.log.info('User connected!')
      }

      socket.emit('connected')

      // handle disconnect
      socket.on('disconnect', () => {
        if (authenticated) {
          this.log.info(
            { userId: (socket as IAuthenticatedSocket).user.id },
            'Authenticated user disconnected!',
          )
        } else {
          this.log.info('User disconnected!')
        }

        ;(socket as any).leaveAll()
      })
    })

    this.log.info('WebSockets namespace initialized!')
  }

  public on(event: string, handler: ISocketHandler<TSocket>) {
    this.socketIoNamespace.on(event, handler)
  }

  public emit(event: string, data: string) {
    this.socketIoNamespace.emit(event, data)
  }

  public emitToRoom(room: string, event: string, data: string) {
    this.socketIoNamespace.to(room).emit(event, data)
  }

  public emitToUserRoom(userId: string, event: string, data: string) {
    this.emitToRoom(`user-${userId}`, event, data)
  }

  public async emitForTenant(tenantId: string, event: string, data: string) {
    const options = await SequelizeRepository.getDefaultIRepositoryOptions()
    const tenantUsers = await TenantUserRepository.findByTenant(tenantId, options)

    for (const user of tenantUsers) {
      this.emitToUserRoom(user.userId, event, data)
    }
  }
}
