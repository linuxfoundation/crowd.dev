import Roles from './roles'
import Storage from './storage'

const storage = Storage.values
const roles = Roles.values

class Permissions {
  static get values() {
    return {
      tenantEdit: {
        id: 'tenantEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      tenantDestroy: {
        id: 'tenantDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      userRead: {
        id: 'userRead',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      userAutocomplete: {
        id: 'userAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      auditLogRead: {
        id: 'auditLogRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      settingsRead: {
        id: 'settingsRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
        allowedStorage: [storage.settingsBackgroundImages, storage.settingsLogos],
      },
      settingsEdit: {
        id: 'settingsEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [storage.settingsBackgroundImages, storage.settingsLogos],
      },
      memberAttributesRead: {
        id: 'memberAttributesRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
        allowedStorage: [],
      },
      memberAttributesEdit: {
        id: 'memberAttributesEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberAttributesDestroy: {
        id: 'memberAttributesDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberAttributesCreate: {
        id: 'memberAttributesCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberImport: {
        id: 'memberImport',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      memberCreate: {
        id: 'memberCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberEdit: {
        id: 'memberEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberDestroy: {
        id: 'memberDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      memberRead: {
        id: 'memberRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      memberAutocomplete: {
        id: 'memberAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      activityImport: {
        id: 'activityImport',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      activityCreate: {
        id: 'activityCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      activityEdit: {
        id: 'activityEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      activityDestroy: {
        id: 'activityDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      activityRead: {
        id: 'activityRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      activityAutocomplete: {
        id: 'activityAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      tagImport: {
        id: 'tagImport',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      tagCreate: {
        id: 'tagCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      tagEdit: {
        id: 'tagEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      tagDestroy: {
        id: 'tagDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      tagRead: {
        id: 'tagRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      tagAutocomplete: {
        id: 'tagAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      organizationImport: {
        id: 'organizationImport',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      organizationCreate: {
        id: 'organizationCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      organizationEdit: {
        id: 'organizationEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      organizationDestroy: {
        id: 'organizationDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      organizationRead: {
        id: 'organizationRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      organizationAutocomplete: {
        id: 'organizationAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      integrationCreate: {
        id: 'integrationCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      integrationEdit: {
        id: 'integrationEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      integrationDestroy: {
        id: 'integrationDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      integrationRead: {
        id: 'integrationRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      integrationAutocomplete: {
        id: 'integrationAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      conversationCreate: {
        id: 'conversationCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      conversationEdit: {
        id: 'conversationEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      conversationDestroy: {
        id: 'conversationDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      conversationRead: {
        id: 'conversationRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeActionCreate: {
        id: 'eagleEyeActionCreate',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeActionDestroy: {
        id: 'eagleEyeActionDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeContentCreate: {
        id: 'eagleEyeContentCreate',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeContentRead: {
        id: 'eagleEyeContentRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeContentSearch: {
        id: 'eagleEyeContentSearch',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      eagleEyeContentEdit: {
        id: 'eagleEyeContentEdit',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      noteImport: {
        id: 'noteImport',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      noteCreate: {
        id: 'noteCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      noteEdit: {
        id: 'noteEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      noteDestroy: {
        id: 'noteDestroy',
        allowedRoles: [roles.admin, roles.projectAdmin],
        allowedStorage: [],
      },
      noteRead: {
        id: 'noteRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      noteAutocomplete: {
        id: 'noteAutocomplete',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      segmentRead: {
        id: 'segmentRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      segmentCreate: {
        id: 'segmentCreate',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      projectGroupCreate: {
        id: 'projectGroupCreate',
        allowedRoles: [roles.admin],
      },
      segmentEdit: {
        id: 'segmentEdit',
        allowedRoles: [roles.admin, roles.projectAdmin],
      },
      customViewCreate: {
        id: 'customViewCreate',
        allowedRoles: [roles.admin, roles.readonly],
      },
      customViewEdit: {
        id: 'customViewEdit',
        allowedRoles: [roles.admin, roles.readonly],
      },
      customViewDestroy: {
        id: 'customViewDestroy',
        allowedRoles: [roles.admin, roles.readonly],
      },
      customViewRead: {
        id: 'customViewRead',
        allowedRoles: [roles.admin, roles.readonly],
      },
      mergeActionRead: {
        id: 'mergeActionRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
      dataIssueCreate: {
        id: 'dataIssueCreate',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },

      collectionEdit: {
        id: 'collectionEdit',
        allowedRoles: [roles.admin],
      },
      collectionRead: {
        id: 'collectionRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },

      categoryEdit: {
        id: 'categoryEdit',
        allowedRoles: [roles.admin],
      },
      categoryRead: {
        id: 'categoryRead',
        allowedRoles: [roles.admin, roles.projectAdmin, roles.readonly],
      },
    }
  }

  static get asArray() {
    return Object.keys(this.values).map((value) => this.values[value])
  }
}

export default Permissions
