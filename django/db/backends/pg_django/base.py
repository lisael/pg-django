from django.db.backends.postgresql_psycopg2.base import *
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as _DatabaseWrapper
from django.db.backends.pg_django.creation import DatabaseCreation


class DatabaseWrapper(_DatabaseWrapper):
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.creation = DatabaseCreation(self)

