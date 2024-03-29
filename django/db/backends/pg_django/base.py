from django.db.backends.postgresql_psycopg2.base import *
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as _DatabaseWrapper
from django.db.backends.postgresql_psycopg2.base import DatabaseFeatures as _DatabaseFeatures
from django.db.backends.postgresql_psycopg2.introspection import DatabaseIntrospection as _DatabaseIntrospection
from django.db.backends.postgresql_psycopg2.operations import DatabaseOperations as _DatabaseOperations
from django.db.backends.pg_django.creation import DatabaseCreation



class DatabaseIntrospection(_DatabaseIntrospection):
    def get_unique_name(self):
        return self.connection.connection.dsn

    def sequence_exists(self, sequence):
        """return True if the given sequence exists in database"""
        cur = self.connection.cursor()
        cur.execute("""SELECT * FROM pg_catalog.pg_class
                   WHERE relname = %s AND relkind = 'S'""",[sequence])
        if len(cur.fetchall()):
            return True
        return False


class DatabaseOperations(_DatabaseOperations):
    def get_sql_for_shared_sequence(self,field):
        return "INTEGER DEFAULT nextval('%s')" % field.sequence


class DatabaseFeatures(_DatabaseFeatures):
    support_arrays = True
    support_views = True
    support_rewrite = True
    support_materialized_view_base = True
    support_shared_sequence = True


class DatabaseWrapper(_DatabaseWrapper):
    # TODO PG: now that we override about anything in it, we should rewrite
    #          the whole method
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)

