import { Server } from 'http'

import { Server as SocketServer } from 'socket.io'

import { Logger, getServiceChildLogger } from '@crowd/logging'

import WebSocketNamespace from './namespace'
import { IAuthenticatedSocket } from './types'

export default class WebSockets {
  private readonly log: Logger

  private readonly socketIo: SocketServer

  public constructor(server: Server) {
    this.log = getServiceChildLogger('websockets')
    this.socketIo = new SocketServer(server)

    this.log.info('Socket.IO server initialized!')
  }

  public authenticatedNamespace(name: string): WebSocketNamespace<IAuthenticatedSocket> {
    return new WebSocketNamespace<IAuthenticatedSocket>(this.socketIo, name, true)
  }

  public static async initialize(
    server: Server,
  ): Promise<WebSocketNamespace<IAuthenticatedSocket>> {
    const websockets = new WebSockets(server)
    return websockets.authenticatedNamespace('/user')
  }
}
