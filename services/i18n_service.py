# services/i18n_service.py
"""
Internationalization (i18n) Service
Provides translation support for English and Spanish.
"""
from flask import session, request, g
from functools import wraps

# Supported languages
SUPPORTED_LANGUAGES = ['en', 'es']
DEFAULT_LANGUAGE = 'en'

# Translation dictionary
TRANSLATIONS = {
    # Navigation
    'nav.dashboard': {
        'en': 'Dashboard',
        'es': 'Panel de Control'
    },
    'nav.jobs': {
        'en': 'Jobs',
        'es': 'Trabajos'
    },
    'nav.schedule': {
        'en': 'Schedule',
        'es': 'Calendario'
    },
    'nav.equipment': {
        'en': 'Equipment',
        'es': 'Equipos'
    },
    'nav.inventory': {
        'en': 'Inventory',
        'es': 'Inventario'
    },
    'nav.documents': {
        'en': 'Documents',
        'es': 'Documentos'
    },
    'nav.reports': {
        'en': 'Reports',
        'es': 'Reportes'
    },
    'nav.people': {
        'en': 'People',
        'es': 'Personal'
    },
    'nav.towns': {
        'en': 'Towns',
        'es': 'Pueblos'
    },
    'nav.settings': {
        'en': 'Settings',
        'es': 'Configuración'
    },
    'nav.logout': {
        'en': 'Logout',
        'es': 'Cerrar Sesión'
    },
    'nav.login': {
        'en': 'Login',
        'es': 'Iniciar Sesión'
    },
    'nav.chat': {
        'en': 'Chat',
        'es': 'Chat'
    },
    'nav.integrations': {
        'en': 'Integrations',
        'es': 'Integraciones'
    },

    # Common Actions
    'action.save': {
        'en': 'Save',
        'es': 'Guardar'
    },
    'action.cancel': {
        'en': 'Cancel',
        'es': 'Cancelar'
    },
    'action.delete': {
        'en': 'Delete',
        'es': 'Eliminar'
    },
    'action.edit': {
        'en': 'Edit',
        'es': 'Editar'
    },
    'action.add': {
        'en': 'Add',
        'es': 'Agregar'
    },
    'action.create': {
        'en': 'Create',
        'es': 'Crear'
    },
    'action.update': {
        'en': 'Update',
        'es': 'Actualizar'
    },
    'action.search': {
        'en': 'Search',
        'es': 'Buscar'
    },
    'action.view': {
        'en': 'View',
        'es': 'Ver'
    },
    'action.download': {
        'en': 'Download',
        'es': 'Descargar'
    },
    'action.upload': {
        'en': 'Upload',
        'es': 'Subir'
    },
    'action.submit': {
        'en': 'Submit',
        'es': 'Enviar'
    },
    'action.confirm': {
        'en': 'Confirm',
        'es': 'Confirmar'
    },
    'action.back': {
        'en': 'Back',
        'es': 'Volver'
    },
    'action.next': {
        'en': 'Next',
        'es': 'Siguiente'
    },
    'action.previous': {
        'en': 'Previous',
        'es': 'Anterior'
    },

    # Common Labels
    'label.name': {
        'en': 'Name',
        'es': 'Nombre'
    },
    'label.description': {
        'en': 'Description',
        'es': 'Descripción'
    },
    'label.status': {
        'en': 'Status',
        'es': 'Estado'
    },
    'label.date': {
        'en': 'Date',
        'es': 'Fecha'
    },
    'label.time': {
        'en': 'Time',
        'es': 'Hora'
    },
    'label.location': {
        'en': 'Location',
        'es': 'Ubicación'
    },
    'label.address': {
        'en': 'Address',
        'es': 'Dirección'
    },
    'label.phone': {
        'en': 'Phone',
        'es': 'Teléfono'
    },
    'label.email': {
        'en': 'Email',
        'es': 'Correo Electrónico'
    },
    'label.category': {
        'en': 'Category',
        'es': 'Categoría'
    },
    'label.type': {
        'en': 'Type',
        'es': 'Tipo'
    },
    'label.quantity': {
        'en': 'Quantity',
        'es': 'Cantidad'
    },
    'label.unit': {
        'en': 'Unit',
        'es': 'Unidad'
    },
    'label.price': {
        'en': 'Price',
        'es': 'Precio'
    },
    'label.cost': {
        'en': 'Cost',
        'es': 'Costo'
    },
    'label.total': {
        'en': 'Total',
        'es': 'Total'
    },
    'label.notes': {
        'en': 'Notes',
        'es': 'Notas'
    },
    'label.actions': {
        'en': 'Actions',
        'es': 'Acciones'
    },
    'label.created': {
        'en': 'Created',
        'es': 'Creado'
    },
    'label.updated': {
        'en': 'Updated',
        'es': 'Actualizado'
    },
    'label.by': {
        'en': 'By',
        'es': 'Por'
    },
    'label.all': {
        'en': 'All',
        'es': 'Todos'
    },
    'label.none': {
        'en': 'None',
        'es': 'Ninguno'
    },

    # Status values
    'status.active': {
        'en': 'Active',
        'es': 'Activo'
    },
    'status.inactive': {
        'en': 'Inactive',
        'es': 'Inactivo'
    },
    'status.pending': {
        'en': 'Pending',
        'es': 'Pendiente'
    },
    'status.completed': {
        'en': 'Completed',
        'es': 'Completado'
    },
    'status.in_progress': {
        'en': 'In Progress',
        'es': 'En Progreso'
    },
    'status.cancelled': {
        'en': 'Cancelled',
        'es': 'Cancelado'
    },
    'status.approved': {
        'en': 'Approved',
        'es': 'Aprobado'
    },
    'status.rejected': {
        'en': 'Rejected',
        'es': 'Rechazado'
    },

    # Jobs
    'jobs.title': {
        'en': 'Jobs',
        'es': 'Trabajos'
    },
    'jobs.create': {
        'en': 'Create Job',
        'es': 'Crear Trabajo'
    },
    'jobs.edit': {
        'en': 'Edit Job',
        'es': 'Editar Trabajo'
    },
    'jobs.job_name': {
        'en': 'Job Name',
        'es': 'Nombre del Trabajo'
    },
    'jobs.estimated_cost': {
        'en': 'Estimated Cost',
        'es': 'Costo Estimado'
    },
    'jobs.actual_cost': {
        'en': 'Actual Cost',
        'es': 'Costo Real'
    },
    'jobs.start_date': {
        'en': 'Start Date',
        'es': 'Fecha de Inicio'
    },
    'jobs.end_date': {
        'en': 'End Date',
        'es': 'Fecha de Fin'
    },
    'jobs.progress': {
        'en': 'Progress',
        'es': 'Progreso'
    },
    'jobs.crew': {
        'en': 'Crew',
        'es': 'Cuadrilla'
    },

    # Equipment
    'equipment.title': {
        'en': 'Equipment',
        'es': 'Equipos'
    },
    'equipment.add': {
        'en': 'Add Equipment',
        'es': 'Agregar Equipo'
    },
    'equipment.maintenance': {
        'en': 'Maintenance',
        'es': 'Mantenimiento'
    },
    'equipment.serial_number': {
        'en': 'Serial Number',
        'es': 'Número de Serie'
    },
    'equipment.model': {
        'en': 'Model',
        'es': 'Modelo'
    },
    'equipment.brand': {
        'en': 'Brand',
        'es': 'Marca'
    },
    'equipment.hours': {
        'en': 'Hours',
        'es': 'Horas'
    },
    'equipment.condition': {
        'en': 'Condition',
        'es': 'Condición'
    },

    # Inventory
    'inventory.title': {
        'en': 'Inventory',
        'es': 'Inventario'
    },
    'inventory.add_item': {
        'en': 'Add Item',
        'es': 'Agregar Artículo'
    },
    'inventory.supplier': {
        'en': 'Supplier',
        'es': 'Proveedor'
    },
    'inventory.reorder': {
        'en': 'Reorder',
        'es': 'Reordenar'
    },
    'inventory.low_stock': {
        'en': 'Low Stock',
        'es': 'Bajo Inventario'
    },
    'inventory.out_of_stock': {
        'en': 'Out of Stock',
        'es': 'Sin Inventario'
    },
    'inventory.in_stock': {
        'en': 'In Stock',
        'es': 'En Inventario'
    },

    # Schedule
    'schedule.title': {
        'en': 'Schedule',
        'es': 'Calendario'
    },
    'schedule.today': {
        'en': 'Today',
        'es': 'Hoy'
    },
    'schedule.week': {
        'en': 'Week',
        'es': 'Semana'
    },
    'schedule.month': {
        'en': 'Month',
        'es': 'Mes'
    },
    'schedule.add_event': {
        'en': 'Add Event',
        'es': 'Agregar Evento'
    },

    # Documents
    'documents.title': {
        'en': 'Documents',
        'es': 'Documentos'
    },
    'documents.upload': {
        'en': 'Upload Document',
        'es': 'Subir Documento'
    },
    'documents.version': {
        'en': 'Version',
        'es': 'Versión'
    },

    # People/Users
    'people.title': {
        'en': 'People',
        'es': 'Personal'
    },
    'people.add': {
        'en': 'Add Person',
        'es': 'Agregar Persona'
    },
    'people.full_name': {
        'en': 'Full Name',
        'es': 'Nombre Completo'
    },
    'people.job_title': {
        'en': 'Job Title',
        'es': 'Cargo'
    },
    'people.role': {
        'en': 'Role',
        'es': 'Rol'
    },
    'people.certifications': {
        'en': 'Certifications',
        'es': 'Certificaciones'
    },

    # Towns
    'towns.title': {
        'en': 'Towns',
        'es': 'Pueblos'
    },
    'towns.add': {
        'en': 'Add Town',
        'es': 'Agregar Pueblo'
    },
    'towns.town_name': {
        'en': 'Town Name',
        'es': 'Nombre del Pueblo'
    },
    'towns.contact': {
        'en': 'Contact',
        'es': 'Contacto'
    },

    # Reports
    'reports.title': {
        'en': 'Reports',
        'es': 'Reportes'
    },
    'reports.generate': {
        'en': 'Generate Report',
        'es': 'Generar Reporte'
    },
    'reports.from_date': {
        'en': 'From Date',
        'es': 'Desde Fecha'
    },
    'reports.to_date': {
        'en': 'To Date',
        'es': 'Hasta Fecha'
    },
    'reports.format': {
        'en': 'Format',
        'es': 'Formato'
    },

    # Dashboard
    'dashboard.title': {
        'en': 'Dashboard',
        'es': 'Panel de Control'
    },
    'dashboard.welcome': {
        'en': 'Welcome',
        'es': 'Bienvenido'
    },
    'dashboard.overview': {
        'en': 'Overview',
        'es': 'Resumen'
    },
    'dashboard.recent_activity': {
        'en': 'Recent Activity',
        'es': 'Actividad Reciente'
    },
    'dashboard.quick_actions': {
        'en': 'Quick Actions',
        'es': 'Acciones Rápidas'
    },

    # Messages
    'message.success': {
        'en': 'Success',
        'es': 'Éxito'
    },
    'message.error': {
        'en': 'Error',
        'es': 'Error'
    },
    'message.warning': {
        'en': 'Warning',
        'es': 'Advertencia'
    },
    'message.info': {
        'en': 'Information',
        'es': 'Información'
    },
    'message.confirm_delete': {
        'en': 'Are you sure you want to delete this?',
        'es': '¿Está seguro de que desea eliminar esto?'
    },
    'message.no_results': {
        'en': 'No results found',
        'es': 'No se encontraron resultados'
    },
    'message.loading': {
        'en': 'Loading...',
        'es': 'Cargando...'
    },
    'message.saved': {
        'en': 'Saved successfully',
        'es': 'Guardado exitosamente'
    },
    'message.deleted': {
        'en': 'Deleted successfully',
        'es': 'Eliminado exitosamente'
    },
    'message.permission_denied': {
        'en': 'Permission denied',
        'es': 'Permiso denegado'
    },
    'message.not_found': {
        'en': 'Not found',
        'es': 'No encontrado'
    },
    'message.required_field': {
        'en': 'This field is required',
        'es': 'Este campo es obligatorio'
    },

    # Auth
    'auth.login': {
        'en': 'Login',
        'es': 'Iniciar Sesión'
    },
    'auth.logout': {
        'en': 'Logout',
        'es': 'Cerrar Sesión'
    },
    'auth.username': {
        'en': 'Username',
        'es': 'Usuario'
    },
    'auth.password': {
        'en': 'Password',
        'es': 'Contraseña'
    },
    'auth.remember_me': {
        'en': 'Remember me',
        'es': 'Recordarme'
    },
    'auth.forgot_password': {
        'en': 'Forgot password?',
        'es': '¿Olvidó su contraseña?'
    },

    # Weather
    'weather.title': {
        'en': 'Weather',
        'es': 'Clima'
    },
    'weather.temperature': {
        'en': 'Temperature',
        'es': 'Temperatura'
    },
    'weather.conditions': {
        'en': 'Conditions',
        'es': 'Condiciones'
    },
    'weather.forecast': {
        'en': 'Forecast',
        'es': 'Pronóstico'
    },
}


def get_language():
    """
    Get the current language from session or request.
    Priority: session > cookie > browser preference > default
    """
    # Check session first
    if 'language' in session:
        lang = session['language']
        if lang in SUPPORTED_LANGUAGES:
            return lang

    # Check cookie
    lang = request.cookies.get('language')
    if lang in SUPPORTED_LANGUAGES:
        return lang

    # Check browser Accept-Language header
    if request.accept_languages:
        for lang_code, _ in request.accept_languages:
            # Extract primary language (e.g., 'es-PR' -> 'es')
            primary = lang_code.split('-')[0].lower()
            if primary in SUPPORTED_LANGUAGES:
                return primary

    return DEFAULT_LANGUAGE


def set_language(lang):
    """Set the current language in session."""
    if lang in SUPPORTED_LANGUAGES:
        session['language'] = lang
        return True
    return False


def translate(key, **kwargs):
    """
    Translate a key to the current language.
    Supports variable substitution with kwargs.

    Usage:
        translate('nav.dashboard')  # Returns 'Dashboard' or 'Panel de Control'
        translate('message.welcome', name='John')  # Variable substitution
    """
    lang = get_language()

    if key in TRANSLATIONS:
        text = TRANSLATIONS[key].get(lang, TRANSLATIONS[key].get(DEFAULT_LANGUAGE, key))
    else:
        # Return the key itself if no translation found
        text = key

    # Apply variable substitution
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass

    return text


def _(key, **kwargs):
    """Shorthand alias for translate()"""
    return translate(key, **kwargs)


def init_i18n(app):
    """
    Initialize i18n for the Flask app.
    Adds the translation functions to the template context.
    """
    @app.context_processor
    def i18n_context_processor():
        return dict(
            _=translate,
            translate=translate,
            get_language=get_language,
            SUPPORTED_LANGUAGES=SUPPORTED_LANGUAGES
        )

    @app.before_request
    def set_locale():
        """Set the language for each request."""
        g.lang = get_language()

    @app.route('/set_language/<lang>')
    def set_language_route(lang):
        """Route to change the language."""
        from flask import redirect, request as req, make_response
        if set_language(lang):
            response = make_response(redirect(req.referrer or '/'))
            response.set_cookie('language', lang, max_age=365*24*60*60)  # 1 year
            return response
        return redirect(req.referrer or '/')

    return app
