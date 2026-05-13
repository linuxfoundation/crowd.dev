import crypto from 'crypto'
import lodash from 'lodash'

import Roles from '../../security/roles'

import { IRepositoryOptions } from './IRepositoryOptions'
import SegmentRepository from './segmentRepository'
import SequelizeRepository from './sequelizeRepository'

export default class TenantUserRepository {
  static async findByTenant(tenantId: string, options: IRepositoryOptions): Promise<any[]> {
    const transaction = SequelizeRepository.getTransaction(options)

    const records = await options.database.tenantUser.findAll({
      where: {
        tenantId,
      },
      transaction,
    })

    return records
  }

  static async findByTenantAndUser(tenantId, userId, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const record = await options.database.tenantUser.findOne({
      where: {
        tenantId,
        userId,
      },
      transaction,
    })

    return record
  }

  static async findByInvitationToken(invitationToken, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const record = await options.database.tenantUser.findOne({
      where: {
        invitationToken,
      },
      include: ['tenant', 'user'],
      transaction,
    })

    return record
  }

  static async convertRoles(roles: string[], options: IRepositoryOptions) {
    const segmentRepository = new SegmentRepository(options)

    const adminSegments = []
    roles = lodash.uniq(
      roles.map((role) => {
        if (role.startsWith(`${Roles.values.admin}:`)) {
          adminSegments.push(role.split(':')[1].trim())
          return Roles.values.projectAdmin
        }
        return role
      }),
    )
    const adminSegmentIds = await segmentRepository.findBySourceIds(adminSegments)

    return {
      roles,
      adminSegments: adminSegmentIds,
    }
  }

  static async create(tenant, user, rawRoles, options: IRepositoryOptions) {
    rawRoles = rawRoles || []
    const transaction = SequelizeRepository.getTransaction(options)

    const { roles, adminSegments } = await this.convertRoles(rawRoles, {
      currentTenant: tenant,
      ...options,
    })

    const status = selectStatus('active', roles)

    await options.database.tenantUser.create(
      {
        tenantId: tenant.id,
        userId: user.id,
        status,
        roles,
        adminSegments,
      },
      { transaction },
    )
  }

  static async destroy(tenantId, id, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const tenantUser = await this.findByTenantAndUser(tenantId, id, options)

    await tenantUser.destroy({ transaction })
  }

  static async updateRoles(tenantId, id, roles, options, isInvited = false) {
    const transaction = SequelizeRepository.getTransaction(options)

    let tenantUser = await this.findByTenantAndUser(tenantId, id, options)

    if (!tenantUser) {
      tenantUser = await options.database.tenantUser.create(
        {
          tenantId,
          userId: id,
          status: selectStatus('invited', []),
          invitationToken: crypto.randomBytes(20).toString('hex'),
          roles: [],
          invitedById: isInvited ? options.currentUser.id : undefined,
        },
        { transaction },
      )
    }

    const { roles: existingRoles } = tenantUser

    let newRoles = [] as Array<string>

    if (options.addRoles) {
      newRoles = [...new Set([...existingRoles, ...roles])]
    } else if (options.removeOnlyInformedRoles) {
      newRoles = existingRoles.filter((existingRole) => !roles.includes(existingRole))
    } else {
      newRoles = roles || []
    }

    tenantUser.roles = newRoles
    tenantUser.status = selectStatus(tenantUser.status, newRoles)

    await tenantUser.save({
      transaction,
    })

    return tenantUser
  }

  static async updateSettings(userId: string, data, options: IRepositoryOptions) {
    const currentUser = SequelizeRepository.getCurrentUser(options)
    const transaction = SequelizeRepository.getTransaction(options)

    const tenantUser = await this.findByTenantAndUser(options.currentTenant.id, userId, options)

    await tenantUser.update(
      {
        settings: { ...tenantUser.settings, ...data },
        updatedById: currentUser.id,
      },
      { transaction },
    )

    return tenantUser
  }

  static async updateEagleEyeSettings(userId: string, data, options: IRepositoryOptions) {
    const currentUser = SequelizeRepository.getCurrentUser(options)
    const transaction = SequelizeRepository.getTransaction(options)

    const tenantUser = await this.findByTenantAndUser(options.currentTenant.id, userId, options)

    await tenantUser.update(
      {
        settings: {
          ...tenantUser.settings,
          eagleEye: { ...tenantUser.settings.eagleEye, ...data },
        },
        updatedById: currentUser.id,
      },
      { transaction },
    )

    return tenantUser
  }

  static async acceptInvitation(invitationToken, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const currentUser = SequelizeRepository.getCurrentUser(options)

    const invitationTenantUser = await this.findByInvitationToken(invitationToken, options)

    const isSameEmailFromInvitation = invitationTenantUser.userId === currentUser.id

    const existingTenantUser = await this.findByTenantAndUser(
      invitationTenantUser.tenantId,
      currentUser.id,
      options,
    )

    // There might be a case that the invite was sent to another email,
    // and the current user is also invited or is already a member
    if (existingTenantUser && existingTenantUser.id !== invitationTenantUser.id) {
      // destroys the new invite
      await this.destroy(invitationTenantUser.tenantId, invitationTenantUser.userId, options)

      // Merges the roles from the invitation and the current tenant user
      existingTenantUser.roles = [
        ...new Set([...existingTenantUser.roles, ...invitationTenantUser.roles]),
      ]

      // Change the status to active (in case the existing one is also invited)
      existingTenantUser.invitationToken = null
      existingTenantUser.status = selectStatus('active', existingTenantUser.roles)

      await existingTenantUser.save({
        transaction,
      })
    } else {
      // In this case there's no tenant user for the current user and the tenant

      // Sometimes the invitation is sent not to the
      // correct email. In those cases the userId must be changed
      // to match the correct user.
      invitationTenantUser.userId = currentUser.id
      invitationTenantUser.invitationToken = null
      invitationTenantUser.status = selectStatus('active', invitationTenantUser.roles)

      await invitationTenantUser.save({
        transaction,
      })
    }

    const emailVerified = currentUser.emailVerified || isSameEmailFromInvitation

    await options.database.user.update(
      {
        emailVerified,
      },
      { where: { id: currentUser.id }, transaction },
    )
  }

  static async replaceRoles(tenantUserId, rawRoles, options: IRepositoryOptions) {
    const transaction = SequelizeRepository.getTransaction(options)

    const { roles, adminSegments } = await TenantUserRepository.convertRoles(rawRoles, options)

    await options.database.tenantUser.update(
      { roles, adminSegments, status: 'active', invitationToken: null },
      {
        where: {
          id: tenantUserId,
        },
        transaction,
      },
    )
  }
}

function selectStatus(oldStatus, newRoles) {
  newRoles = newRoles || []

  if (oldStatus === 'invited') {
    return oldStatus
  }

  if (!newRoles.length) {
    return 'empty-permissions'
  }

  return 'active'
}
