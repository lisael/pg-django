from django.conf import settings
from django.db.backends.postgresql_psycopg2.base import DatabaseCreation as _DatabaseCreation
from django.db.models.fields import FieldDoesNotExist

from django.db.models import ArrayFieldBase
from django.db.models.fields import SharedAutoField
#from pg_django.models import ViewCType

class DatabaseCreation(_DatabaseCreation):

    def sql_for_inline_foreign_key_references(self, field, known_models, style):
        """
        Return the SQL snippet defining the foreign key reference for a field.
        """
        from django.db.models import CASCADE
        qn = self.connection.ops.quote_name
        if field.rel.to in known_models:
            cascade = getattr(settings, 'DB_CASCADE', False
                             ) and field.rel.on_delete == CASCADE
            output = [style.SQL_KEYWORD('REFERENCES') + ' ' +
                style.SQL_TABLE(qn(field.rel.to._meta.db_table)) + ' (' +
                style.SQL_FIELD(qn(field.rel.to._meta.get_field(
                    field.rel.field_name).column)) + ')' +
                    (cascade and ' ON DELETE CASCADE' or '') +
                self.connection.ops.deferrable_sql()
            ]
            pending = False
        else:
            # We haven't yet created the table to which this field
            # is related, so save it for later.
            output = []
            pending = True

        return output, pending

    def sql_for_pending_references(self, model, style, pending_references):
        """
        Returns any ALTER TABLE statements to add constraints after the fact.
        pg_django adds ON DELETE CASCADE if settings.DELETE_CASCADE is True
        """
        from django.db.backends.util import truncate_name
        from django.db.models import CASCADE

        if not model._meta.managed or model._meta.proxy:
            return []
        qn = self.connection.ops.quote_name
        final_output = []
        opts = model._meta
        if model in pending_references:
            for rel_class, f in pending_references[model]:
                if getattr(settings, 'DB_CASCADE', False
                          )and f.rel.on_delete == CASCADE:
                    cascade = ' ON DELETE CASCADE'
                else:
                    cascade = ''
                rel_opts = rel_class._meta
                r_table = rel_opts.db_table
                r_col = f.column
                table = opts.db_table
                col = opts.get_field(f.rel.field_name).column
                r_name = '%s_refs_%s_%s' % (
                    r_col, col, self._digest(r_table, table))
                final_output.append(style.SQL_KEYWORD('ALTER TABLE') +
                    ' %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;' %
                    (qn(r_table), qn(truncate_name(
                        r_name, self.connection.ops.max_name_length())),
                    qn(r_col), qn(table), qn(col), cascade,
                    self.connection.ops.deferrable_sql()))
            del pending_references[model]
        return final_output

    def sql_indexes_for_field(self, model, f, style):
        from django.db.backends.util import truncate_name
        if f.db_index and not f.unique:
            qn = self.connection.ops.quote_name
            db_table = model._meta.db_table
            tablespace = f.db_tablespace or model._meta.db_tablespace
            if tablespace:
                tablespace_sql = self.connection.ops.tablespace_sql(tablespace)
                if tablespace_sql:
                    tablespace_sql = ' ' + tablespace_sql
            else:
                tablespace_sql = ''

            if isinstance(f,ArrayFieldBase):
                using = 'gin'
            else:
                using = 'btree'

            def get_index_sql(index_name, opclass=''):
                return (style.SQL_KEYWORD('CREATE INDEX') + ' ' +
                        style.SQL_TABLE(qn(truncate_name(index_name,self.connection.ops.max_name_length()))) + ' ' +
                        style.SQL_KEYWORD('ON') + ' ' +
                        style.SQL_TABLE(qn(db_table)) + ' ' +
                        style.SQL_KEYWORD('USING') + ' ' +
                        style.SQL_KEYWORD(using) + ' ' +
                        "(%s%s)" % (style.SQL_FIELD(qn(f.column)), opclass) +
                        "%s;" % tablespace_sql)

            output = [get_index_sql('%s_%s' % (db_table, f.column))]

            # Fields with database column types of `varchar` and `text` need
            # a second index that specifies their operator class, which is
            # needed when performing correct LIKE queries outside the
            # C locale. See #12234.
            if using == 'btree':
                db_type = f.db_type(connection=self.connection)
                if db_type.startswith('varchar'):
                    output.append(get_index_sql('%s_%s_like' % (db_table, f.column),
                                                ' varchar_pattern_ops'))
                elif db_type.startswith('text'):
                    output.append(get_index_sql('%s_%s_like' % (db_table, f.column),
                                                ' text_pattern_ops'))
        else:
            output = []
        return output

    def sql_create_model(self, model, style, known_models=set()):
        """
        Returns the SQL required to create a single model, as a tuple of:
            (list_of_sql, pending_references_dict)

        pg-django adds support for materialized views, db-level cascades
        and defaults
        """
        opts = model._meta
        if not opts.managed or opts.proxy:
            return [], {}
        final_output = []
        table_output = []
        pending_references = {}
        qn = self.connection.ops.quote_name


        ctype = model._meta.leaf and model._meta.leaf_id or ''

        matview = model._meta.materialized_view
        if matview:
            ## match the view fields with children fields. For now i has to
            ## have the same db column name. It may change in th future
            ## TODO PG: import __future__
            col_names = [f.column for f in model._meta.local_fields[:-1] ]
            columns = {}
            for child in model._meta.leaves:
                child_columns = []
                for col_name in col_names:
                    try:
                        f = child._meta.get_field_by_name(col_name)[0]
                        child_columns.append((col_name,col_name))
                        if f.rel:
                            # TODO PG: think it doesn't work yet, as materialized
                            # views are created after everything (get_models
                            # is monkey-patched for this purpose). Test this
                            pending_references.setdefault(f.rel.to, []).append(
                                                                    (model, f))
                    except FieldDoesNotExist:
                        child_columns.append((None,col_name))
                columns[child] = child_columns

            # create the view
            view_name = model._meta.db_table + '_view'
            full_view_statement = style.SQL_KEYWORD('CREATE OR REPLACE VIEW') + ' '
            full_view_statement += style.SQL_TABLE(qn(view_name)) + ' '
            full_view_statement += style.SQL_KEYWORD('AS') + '\n    '
            subselects = []
            for child in columns:
                child_statement = style.SQL_KEYWORD('SELECT ')
                for value, name in columns[child]:
                    if value is None:
                        child_statement += style.SQL_KEYWORD('NULL AS ')
                    child_statement += style.SQL_FIELD(qn(name)) + ', '
                child_statement += str(child._meta.leaf_id)
                child_statement += ' ' + style.SQL_KEYWORD('AS') + ' '
                child_statement += style.SQL_FIELD(qn('pgd_child_type'))
                child_statement += '\n        ' + style.SQL_KEYWORD('FROM')
                child_statement += ' ' + style.SQL_TABLE(qn(child._meta.db_table))
                subselects.append(child_statement)
            union = '\n    ' + style.SQL_KEYWORD('UNION ALL') + '\n    '
            subselects = union.join(subselects)
            full_view_statement += subselects + '\n;'

            final_output.append(full_view_statement)
        ### ... end

        for f in opts.local_fields:
            col_type = f.db_type(connection=self.connection)
            tablespace = f.db_tablespace or opts.db_tablespace
            if col_type is None:
                # Skip ManyToManyFields, because they're not represented as
                # database columns in this table.
                continue
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)]

            #### added...
            if isinstance(f, SharedAutoField):
                if not f.is_created:
                    seq = f.sequence
                    final_output.append(style.SQL_KEYWORD(
                        'CREATE SEQUENCE ') +
                        style.SQL_TABLE(qn(seq)) + ';\n')
                    SharedAutoField._created[seq] = True
            ### ... end

            if not f.null and not matview:
                field_output.append(style.SQL_KEYWORD('NOT NULL'))
            if f.primary_key:
                field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
            elif f.unique and not matview:
                field_output.append(style.SQL_KEYWORD('UNIQUE'))
            if tablespace and f.unique and not matview:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                tablespace_sql = self.connection.ops.tablespace_sql(
                    tablespace, inline=True)
                if tablespace_sql:
                    field_output.append(tablespace_sql)
            if f.rel:
                ref_output, pending = self.sql_for_inline_foreign_key_references(
                    f, known_models, style)
                if pending:
                    pending_references.setdefault(f.rel.to, []).append(
                        (model, f))
                else:
                    field_output.extend(ref_output)
            table_output.append(' '.join(field_output))
        for field_constraints in opts.unique_together:
            if matview:
                break
            table_output.append(style.SQL_KEYWORD('UNIQUE') + ' (%s)' %
                ", ".join(
                    [style.SQL_FIELD(qn(opts.get_field(f).column))
                     for f in field_constraints]))

        full_statement = [style.SQL_KEYWORD('CREATE TABLE') + ' ' +
                          style.SQL_TABLE(qn(opts.db_table)) + ' (']
        for i, line in enumerate(table_output): # Combine and add commas.
            full_statement.append(
                '    %s%s' % (line, i < len(table_output)-1 and ',' or ''))
        full_statement.append(')')
        if opts.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(
                opts.db_tablespace)
            if tablespace_sql:
                full_statement.append(tablespace_sql)
        full_statement.append(';')
        final_output.append('\n'.join(full_statement))

        ### added ...
        if matview:
            # create the triggers
            final_output.extend(self.make_view_update_statements(
                model, style, opts, columns))
            final_output.extend(self.make_view_insert_statements(
                model, style, opts, columns))
            final_output.extend(self.make_view_delete_statements(
                model, style, opts, columns))

        ### ... end

        if opts.has_auto_field:
            # Add any extra SQL needed to support auto-incrementing primary
            # keys.
            auto_column = opts.auto_field.db_column or opts.auto_field.name
            autoinc_sql = self.connection.ops.autoinc_sql(opts.db_table,
                                                          auto_column)
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)

        return final_output, pending_references

    def sequence_exists(self, sequence):
        """return True if the given sequence exists in database"""
        cur = self.connection.cursor()
        cur.execute("""SELECT * FROM pg_catalog.pg_class
                   WHERE relname = %s and relkind = 'S'""",[sequence])
        if len(cur.fetchall()):
            return True
        return False

    def make_view_update_statements(self, model, style, meta, columns):
        output = []
        for child in model._meta.leaves:
            qn = self.connection.ops.quote_name
            # update trigger
            ut_name = child._meta.db_table + '_ut'
            ut_body = style.SQL_KEYWORD('UPDATE') + ' '
            ut_body += style.SQL_TABLE(qn(meta.db_table)) + '\n'
            ut_body += style.SQL_KEYWORD('SET') + ' '
            ut_body += ', '.join([style.SQL_FIELD(qn(name)
                            ) + style.SQL_KEYWORD(' = new.'
                            ) + style.SQL_FIELD(qn(name)
            ) for value, name in columns[child] if value not in (None, 'id')])
            ut_body += '\n    ' + style.SQL_KEYWORD('WHERE') + ' '
            ut_body += style.SQL_FIELD(qn('id')) + ' '
            ut_body += style.SQL_KEYWORD('=') + ' new.'
            ut_body += style.SQL_FIELD(qn('id')) + ';\n'
            ut_body += style.SQL_KEYWORD('RETURN NULL') + ';'

            output.append(self.make_proc_statement(style,
                                        name=ut_name,
                                        returns='TRIGGER',
                                        body=ut_body) + '\n')

            output.append(self.make_create_trigger_statement(
                                        style,
                                        ut_name,
                                        'AFTER UPDATE',
                                        child._meta.db_table,
                                        ut_name) + '\n')
        return output

    def make_view_delete_statements(self, model, style, meta, columns):
        output = []
        for child in model._meta.leaves:
            qn = self.connection.ops.quote_name
            # update trigger
            dt_name = child._meta.db_table + '_dt'
            dt_body = style.SQL_KEYWORD('DELETE FROM') + ' '
            dt_body += style.SQL_TABLE(qn(meta.db_table)) + '\n'
            dt_body += '\n    ' + style.SQL_KEYWORD('WHERE') + ' '
            dt_body += style.SQL_FIELD(qn('id')) + ' '
            dt_body += style.SQL_KEYWORD('=') + ' old.'
            dt_body += style.SQL_FIELD(qn('id')) + ';\n'
            dt_body += style.SQL_KEYWORD('RETURN NULL') + ';'

            output.append(self.make_proc_statement(style,
                                        name=dt_name,
                                        returns='TRIGGER',
                                        body=dt_body) + '\n')

            output.append(self.make_create_trigger_statement(
                                        style,
                                        dt_name,
                                        'AFTER DELETE',
                                        child._meta.db_table,
                                        dt_name) + '\n')
        return output

    def make_view_insert_statements(self, model, style, meta, columns):
        output = []
        for child in model._meta.leaves:
            qn = self.connection.ops.quote_name
            # update trigger
            it_name = child._meta.db_table + '_it'
            it_body = style.SQL_KEYWORD('INSERT INTO') + ' '
            it_body += style.SQL_TABLE(qn(meta.db_table)) + ' ( '
            it_body += ', '.join([style.SQL_FIELD(qn(name)
                ) for value, name in columns[child] if value is not None])
            it_body += ', ' + style.SQL_FIELD('"pgd_child_type"') + ')\n    '
            it_body += style.SQL_KEYWORD('VALUES') + ' ( new.'
            it_body += ', new.'.join([style.SQL_FIELD(qn(name)
                ) for value, name in columns[child] if value is not None])
            it_body += ', %s );\n' % child._meta.leaf_id
            it_body += style.SQL_KEYWORD('RETURN NULL') + ';'

            output.append(self.make_proc_statement(style,
                                        name=it_name,
                                        returns='TRIGGER',
                                        body=it_body) + '\n')

            output.append(self.make_create_trigger_statement(
                                        style,
                                        it_name,
                                        'AFTER INSERT',
                                        child._meta.db_table,
                                        it_name) + '\n')
        return output

    def make_create_trigger_statement(self, style, name, event, table, proc):
        qn = self.connection.ops.quote_name
        ut = style.SQL_KEYWORD('CREATE TRIGGER') + ' '
        ut += style.SQL_TABLE(qn(name)) + ' '
        ut += style.SQL_KEYWORD('%s ON' % event) + ' '
        ut += style.SQL_TABLE(qn(table)) + '\n'
        ut += style.SQL_KEYWORD('FOR EACH ROW EXECUTE PROCEDURE')
        ut += ' ' + style.SQL_TABLE(qn(proc)) + ' ()\n;'

        return ut

    def make_proc_statement(self, style, name='', returns='', body='', args='',
                            language='PLPGSQL'):
        """Format a sql procedure according to style"""
        qn = self.connection.ops.quote_name
        statement = style.SQL_KEYWORD('CREATE OR REPLACE FUNCTION') + ' '
        statement += style.SQL_TABLE(qn(name)) + ' (%s)\n'%args
        statement += style.SQL_KEYWORD('RETURNS') + ' '
        statement += style.SQL_COLTYPE(returns) + '\n'
        statement += style.SQL_KEYWORD('SECURITY DEFINER LANGUAGE') + ' '
        statement += style.SQL_TABLE("'%s'" % language) + ' '
        statement += style.SQL_KEYWORD('AS')+ ' $$\n'
        statement += style.SQL_KEYWORD('BEGIN') + '\n    '
        statement += '\n    '.join(body.splitlines()) + '\n'
        statement += style.SQL_KEYWORD('END') + '\n$$\n;'

        return statement

