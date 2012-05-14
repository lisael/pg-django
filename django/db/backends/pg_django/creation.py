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
            if model._meta.intermediate:
                if not model._meta.concrete:
                    # only a view, no index
                    return []
                else:
                    # TODO PG: set view and table name in options
                    if f.column == 'pgd_child_type':
                        return []
                    db_table = model._meta.db_table[:-5]
            else:
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

    def get_columns_for_mat_view(self, model, pending_references=None):
        """match the view fields with children fields. For now i has to
        have the same db column name. It may change in the future
        """
        ## TODO PG: import __future__
        col_names = [f.column for f in model._meta.local_fields[:-1] ]
        col_types = [f.db_type(self.connection) for f in model._meta.local_fields[:-1] ]
        columns = {}
        for child in model._meta.leaves:
            child_columns = []
            it = iter(col_types)
            for col_name in col_names:
                col_type = it.next()
                try:
                    f = child._meta.get_field_by_name(col_name)[0]
                    if getattr(f, 'child_field', False):
                        raise FieldDoesNotExist()
                    child_columns.append((col_name,col_name,col_type))
                    if f.rel and pending_references is not None:
                        # TODO PG: think it doesn't work yet, as materialized
                        # views are created after everything (get_models
                        # is monkey-patched for this purpose). Test this
                        pending_references.setdefault(f.rel.to, []).append(
                                                                (model, f))
                except FieldDoesNotExist:
                    child_columns.append((None,col_name, col_type))
            columns[child] = child_columns

        return columns


    def _sql_create_table_for_model(self, model, style, final_output,
                                    pending_references, known_models):
        qn = self.connection.ops.quote_name
        opts = model._meta
        intermediate = opts.intermediate
        matview = opts.materialized_view
        table_name = opts.concrete_table_name
        table_output = []
        for f in opts.local_fields:
            if intermediate and (f.name == 'pgd_child_type' or
                                 getattr(f, 'child_field',False)):
                continue
            col_type = f.db_type(connection=self.connection)
            tablespace = f.db_tablespace or opts.db_tablespace
            if col_type is None:
                # Skip ManyToManyFields, because they're not represented as
                # database columns in this table.
                continue
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)]

            # make the sequence of SharedAutoFields if it does not exist yet
            if isinstance(f, SharedAutoField):
                if not f.is_created:
                    seq = f.sequence
                    final_output.append(style.SQL_KEYWORD(
                        'CREATE SEQUENCE ') +
                        style.SQL_TABLE(qn(seq)) + ';\n')
                    SharedAutoField._created[seq] = True


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
                        style.SQL_TABLE(qn(table_name)) + ' (']
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

        return table_name


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
        pending_references = {}
        qn = self.connection.ops.quote_name

        ctype = model._meta.leaf and model._meta.leaf_id or ''

        view = model._meta.db_view
        matview = model._meta.materialized_view
        intermediate = model._meta.intermediate
        concrete = model._meta.concrete
        if matview:
            for leaf in model._meta.leaves :
                if leaf._meta.intermediate:
                    # the intermediate concrete tables are not created yet.
                    # pre-create it now
                    self._sql_create_table_for_model(leaf, style, final_output,
                                        pending_references, known_models)

            columns = self.get_columns_for_mat_view(model,pending_references)

            # create the view as a union of the leaf tables
            view_name = model._meta.db_table + '_view'
            full_view_statement = style.SQL_KEYWORD('CREATE OR REPLACE VIEW') + ' '
            full_view_statement += style.SQL_TABLE(qn(view_name)) + ' '
            full_view_statement += style.SQL_KEYWORD('AS') + '\n    '
            subselects = []
            type_subselect = style.SQL_KEYWORD('SELECT ')
            for child in columns:
                child_statement = style.SQL_KEYWORD('SELECT ')
                for value, name , col_type in columns[child]:
                    if value is None:
                        child_statement += style.SQL_KEYWORD('NULL::%s AS ' % col_type)
                    child_statement += style.SQL_FIELD(qn(name)) + ', '
                child_statement += str(child._meta.leaf_id)
                child_statement += ' ' + style.SQL_KEYWORD('AS') + ' '
                child_statement += style.SQL_FIELD(qn('pgd_child_type'))
                child_statement += '\n        ' + style.SQL_KEYWORD('FROM')
                child_statement += ' ' + style.SQL_TABLE(qn(child._meta.concrete_table_name))
                subselects.append(child_statement)
            union = '\n    ' + style.SQL_KEYWORD('UNION ALL') + '\n    '
            subselects = union.join(subselects)
            full_view_statement += subselects + '\n;'

            final_output.append(full_view_statement)

        if intermediate:
            # create the view as a subset of base materialized view
            # cherry-picking fields and rows
            columns = self.get_columns_for_mat_view(model._meta.mat_view_base)
            view_name = model._meta.db_table
            base_name = model._meta.mat_view_base._meta.db_table
            full_view_statement = style.SQL_KEYWORD('CREATE OR REPLACE VIEW') + ' '
            full_view_statement += style.SQL_TABLE(qn(view_name)) + ' '
            full_view_statement += style.SQL_KEYWORD('AS') + '\n    '
            full_view_statement += style.SQL_KEYWORD('SELECT ')
            col_names = [ style.SQL_FIELD(qn(f.column)) for f in model._meta.fields]
            full_view_statement += ', '.join(col_names) + '\n    '
            full_view_statement += style.SQL_KEYWORD('FROM ')
            full_view_statement += style.SQL_TABLE(qn(base_name)) + '\n        '
            full_view_statement += style.SQL_KEYWORD('WHERE ')
            full_view_statement += style.SQL_FIELD(qn('pgd_child_type')) + ' '
            full_view_statement += style.SQL_KEYWORD('IN') + ' ('
            leaf_ids = model._meta.leaf_ids.keys()
            full_view_statement += ', '.join(str(l) for l in leaf_ids) + ')\n;'

            final_output.append(full_view_statement)

        elif view:
            pass

        if concrete:
            # intermediate concrete tables are pre-created by their base mat views
            if not intermediate:
                self._sql_create_table_for_model(model, style, final_output,
                                            pending_references, known_models)

            if matview:
                # create the triggers
                for leaf in model._meta.leaves :
                    if not leaf._meta.concrete:
                        continue
                    final_output.extend(self.make_view_update_statements(
                        model, leaf, style, columns))
                    final_output.extend(self.make_view_insert_statements(
                        model, leaf, style, columns))
                    final_output.extend(self.make_view_delete_statements(
                        model, leaf, style, columns))

            if intermediate and concrete:
                table_name = model._meta.concrete_table_name
                # creates rules for intermediate update
                final_output.append(self.make_insert_rule(model, view_name,
                                                          table_name, style))
                final_output.append(self.make_delete_rule(model, view_name,
                                                          table_name, style))
                final_output.append(self.make_update_rule(model, view_name,
                                                          table_name, style))

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

    def make_delete_rule(self, model, view_name, table_name, style):
        qn = self.connection.ops.quote_name
        rule_name = view_name + '_delete_rule'
        stmt = style.SQL_KEYWORD('CREATE RULE ')
        stmt += style.SQL_TABLE(qn(rule_name)) + ' '
        stmt += style.SQL_KEYWORD('AS ON DELETE TO') + ' '
        stmt += style.SQL_TABLE(qn(view_name)) + ' \n    '
        stmt += style.SQL_KEYWORD('DO INSTEAD DELETE FROM') + ' '
        stmt += style.SQL_TABLE(qn(table_name)) + ' '
        stmt += style.SQL_KEYWORD('WHERE ')
        stmt += style.SQL_FIELD(model._meta.pk.column) + ' = '
        stmt += 'OLD.' + style.SQL_FIELD(model._meta.pk.column)
        stmt += '\n;'
        return stmt

    def make_update_rule(self, model, view_name, table_name, style):
        qn = self.connection.ops.quote_name
        rule_name = view_name + '_update_rule'
        stmt = style.SQL_KEYWORD('CREATE RULE ')
        stmt += style.SQL_TABLE(qn(rule_name)) + ' '
        stmt += style.SQL_KEYWORD('AS ON UPDATE TO') + ' '
        stmt += style.SQL_TABLE(qn(view_name)) + ' \n    '
        stmt += style.SQL_KEYWORD('DO INSTEAD UPDATE') + ' '
        stmt += style.SQL_TABLE(qn(table_name)) + ' '
        stmt += style.SQL_KEYWORD('SET') + '  '
        stmt += ', '.join(style.SQL_FIELD(qn(f.column)) + ' = NEW.' + style.SQL_FIELD(qn(f.column))  for f
                            in model._meta.fields
                            if f.name != 'pgd_child_type' and not
                            getattr(f,'child_field',False) and not
                            f.primary_key) + ' \n    '
        stmt += style.SQL_KEYWORD('WHERE ') + style.SQL_FIELD(model._meta.pk.column) + ' = NEW.'
        stmt += style.SQL_FIELD(model._meta.pk.column) + '\n;'
        return stmt

    def make_insert_rule(self, model, view_name, table_name, style):
        qn = self.connection.ops.quote_name
        concrete_fields = [ style.SQL_FIELD(qn(f.column)) for f
                            in model._meta.fields
                            if f.name not in ('pgd_child_type', 'id') and not
                            getattr(f,'child_field',False)]
        rule_name = view_name + '_insert_rule'
        stmt = style.SQL_KEYWORD('CREATE RULE ')
        stmt += style.SQL_TABLE(qn(rule_name)) + ' '
        stmt += style.SQL_KEYWORD('AS ON INSERT TO') + ' '
        stmt += style.SQL_TABLE(qn(view_name)) + ' \n    '
        stmt += style.SQL_KEYWORD('DO INSTEAD INSERT INTO') + ' '
        stmt += style.SQL_TABLE(qn(table_name)) + ' ( '
        stmt += ', '.join(concrete_fields) + ' )\n    '
        stmt += style.SQL_KEYWORD('VALUES') + ' ( NEW.'
        stmt += ', NEW.'.join(concrete_fields)
        stmt += ')\n        ' + style.SQL_KEYWORD('RETURNING') + ' '
        returning_fields = []
        for f in model._meta.fields:
            if f.name == 'pgd_child_type' or getattr(f,'child_field',False):
                returning_fields.append('NULL::%s AS %s' % (f.db_type(self.connection), f.column))
            else:
                returning_fields.append('%s.%s' % (style.SQL_TABLE(qn(table_name)),
                                                  style.SQL_FIELD(f.column))
                                       )
        stmt += ', '.join(returning_fields) + '\n;'
        #stmt += style.SQL_TABLE(qn(table_name)) + '.id, (SELECT NULL::VARCHAR)\n;'
        #+ style.SQL_FIELD(qn(model._meta.pk.column))
        return stmt

    def sequence_exists(self, sequence):
        """return True if the given sequence exists in database"""
        cur = self.connection.cursor()
        cur.execute("""SELECT * FROM pg_catalog.pg_class
                   WHERE relname = %s AND relkind = 'S'""",[sequence])
        if len(cur.fetchall()):
            return True
        return False

    def make_view_update_statements(self, model, leaf, style, columns):
        output = []
        leaf_table = leaf._meta.concrete_table_name
        qn = self.connection.ops.quote_name
        # update trigger
        ut_name = leaf_table + '_ut'
        ut_body = style.SQL_KEYWORD('UPDATE') + ' '
        ut_body += style.SQL_TABLE(qn(model._meta.db_table)) + '\n'
        ut_body += style.SQL_KEYWORD('SET') + ' '
        ut_body += ', '.join([style.SQL_FIELD(qn(name)
                        ) + style.SQL_KEYWORD(' = new.'
                        ) + style.SQL_FIELD(qn(name)
        ) for value, name, _ in columns[leaf] if value not in (None, model._meta.pk.column)])
        ut_body += '\n    ' + style.SQL_KEYWORD('WHERE') + ' '
        ut_body += style.SQL_FIELD(qn(model._meta.pk.column)) + ' '
        ut_body += style.SQL_KEYWORD('=') + ' NEW.'
        ut_body += style.SQL_FIELD(qn(model._meta.pk.column)) + ';\n'
        ut_body += style.SQL_KEYWORD('RETURN NULL') + ';'

        output.append(self.make_proc_statement(style,
                                    name=ut_name,
                                    returns='TRIGGER',
                                    body=ut_body) + '\n')

        output.append(self.make_create_trigger_statement(
                                    style,
                                    ut_name,
                                    'AFTER UPDATE',
                                    leaf_table,
                                    ut_name) + '\n')
        return output

    def make_view_delete_statements(self, model, leaf, style, columns):
        output = []
        leaf_table = leaf._meta.concrete_table_name
        qn = self.connection.ops.quote_name
        # update trigger
        dt_name = leaf_table + '_dt'
        dt_body = style.SQL_KEYWORD('DELETE FROM') + ' '
        dt_body += style.SQL_TABLE(qn(model._meta.db_table)) + '\n'
        dt_body += '\n    ' + style.SQL_KEYWORD('WHERE') + ' '
        dt_body += style.SQL_FIELD(qn(model._meta.pk.column)) + ' '
        dt_body += style.SQL_KEYWORD('=') + ' old.'
        dt_body += style.SQL_FIELD(qn(model._meta.pk.column)) + ';\n'
        dt_body += style.SQL_KEYWORD('RETURN NULL') + ';'

        output.append(self.make_proc_statement(style,
                                    name=dt_name,
                                    returns='TRIGGER',
                                    body=dt_body) + '\n')

        output.append(self.make_create_trigger_statement(
                                    style,
                                    dt_name,
                                    'AFTER DELETE',
                                    leaf_table,
                                    dt_name) + '\n')
        return output

    def make_view_insert_statements(self, model, leaf, style, columns):
        output = []
        leaf_table = leaf._meta.concrete_table_name
        qn = self.connection.ops.quote_name
        # update trigger
        it_name = leaf_table + '_it'
        it_body = style.SQL_KEYWORD('INSERT INTO') + ' '
        it_body += style.SQL_TABLE(qn(model._meta.db_table)) + ' ( '
        it_body += ', '.join([style.SQL_FIELD(qn(name)
            ) for value, name , _ in columns[leaf] if value is not None])
        it_body += ', ' + style.SQL_FIELD('"pgd_child_type"') + ')\n    '
        it_body += style.SQL_KEYWORD('VALUES') + ' ( NEW.'
        it_body += ', NEW.'.join([style.SQL_FIELD(qn(name)
            ) for value, name, _ in columns[leaf] if value is not None])
        it_body += ', %s );\n' % leaf._meta.leaf_id
        it_body += style.SQL_KEYWORD('RETURN NULL') + ';'

        output.append(self.make_proc_statement(style,
                                    name=it_name,
                                    returns='TRIGGER',
                                    body=it_body) + '\n')

        output.append(self.make_create_trigger_statement(
                                    style,
                                    it_name,
                                    'AFTER INSERT',
                                    leaf_table,
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

