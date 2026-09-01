const es = {
  app: {
    title: 'Aplicación',
  },
  auth: {
    userNotFound: 'Lo sentimos, no reconocemos tus credenciales',
    wrongPassword: 'Lo sentimos, no reconocemos tus credenciales',
    weakPassword: 'Esta contraseña es muy débil.',
    emailAlreadyInUse: 'Correo electrónico ya está en uso',
    invalidEmail: 'Por favor proporcione un correo electrónico válido',
    passwordReset: {
      invalidToken: 'El enlace de restablecimiento de contraseña no es válido o ha expirado',
      error: 'Correo electrónico no reconocido',
    },
    emailAddressVerificationEmail: {
      invalidToken: 'El enlace de verificación de correo electrónico no es válido o ha expirado.',
      error: 'Correo electrónico no reconocido',
      signedInAsWrongUser:
        'Esta confirmación por correo electrónico se envió a {0} pero ha iniciado sesión como {1}.',
    },
    passwordChange: {
      invalidPassword: 'La contraseña anterior no es válida.',
    },
  },
  user: {
    errors: {
      userAlreadyExists: 'El usuario con este correo electrónico ya existe.',
      userNotFound: 'Usuario no encontrado.',
      destroyingHimself: 'No puedes eliminarte a ti mismo.',
      revokingOwnPermission: 'No puede revocar su propio permiso de administrador.',
      revokingPlanUser: 'No puede revocar el permiso de administrador del administrador del plan.',
      destroyingPlanUser: 'No puede eliminar el administrador del plan.',
    },
  },
  tenant: {
    exists: 'Ya hay un espacio de trabajo en esta aplicación.',
    url: {
      exists: 'Esta URL del espacio de trabajo ya está en uso.',
    },
    invitation: {
      notSameEmail: 'Esta invitación se envió a {0} pero has iniciado sesión como {1}.',
    },
  },
  importer: {
    errors: {
      invalidFileEmpty: 'El archivo esta vacio',
      invalidFileExcel: 'Solo se permiten archivos de Excel(.xlsx)',
      invalidFileUpload:
        'Archivo inválido. Asegúrese de estar utilizando la última versión de la plantilla.',
      importHashRequired: 'Se requiere hash de importación',
      importHashExistent: 'Los datos ya han sido importados',
    },
  },
  errors: {
    notFound: {
      message: 'Extraviado',
    },
    forbidden: {
      message: 'Prohibido',
    },
    validation: {
      message: 'Ocurrió un error',
    },
    alreadyOnboarded: 'El repositorio {0} ya está incorporado o la incorporación está en curso.',
  },
  email: {
    error: 'El proveedor de correo electrónico no está configurado.',
  },
  preview: {
    error: 'Lo sentimos, esta operación no está permitida en el modo de vista previa.',
  },

  entities: {
    project: {
      errors: {
        unique: {},
      },
    },
    repo: {
      errors: {
        unique: {
          url: 'URL debe ser único',
        },
      },
    },
    member: {
      errors: {
        unique: {
          username: 'Username debe ser único',
          email: 'Email debe ser único',
        },
      },
    },
    activity: {
      errors: {
        unique: {},
      },
    },
    tag: {
      errors: {
        unique: {},
      },
    },
  },
}

export default es
