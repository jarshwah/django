APPS_ON_OTHER_DATABASE = {
    # Contrib apps
    'auth',
    'contenttypes',
    'sites',
    # Test apps
    'multiple_database',
    'prefetch_related',
}


class DjangoTestSuiteRouter(object):
    def db_for_read(self, model, **hints):
        return None

    def db_for_write(self, model, **hints):
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, model):
        """
        Migrate only apps that have tests cases with multi_db = True
        on the 'other' database.
        """
        if db == 'other':
            return model._meta.app_label in APPS_ON_OTHER_DATABASE
        return None
