"""
Code to manage the creation and SQL rendering of 'where' constraints.
"""

from __future__ import absolute_import

import datetime
from itertools import repeat

from django.utils import tree
from django.db.models.fields import Field
from django.db.models.sql.datastructures import EmptyResultSet, FullResultSet
from django.db.models.sql.aggregates import Aggregate

# Connection types
AND = 'AND'
OR = 'OR'


def get_sql_type(lvalue):
    """ pg-djando only. find sql type of an array"""
    if isinstance(lvalue, tuple):
        return lvalue[2]


class EmptyShortCircuit(Exception):
    """
    Internal exception used to indicate that a "matches nothing" node should be
    added to the where-clause.
    """
    pass

class WhereNode(tree.Node):
    """
    Used to represent the SQL where-clause.

    The class is tied to the Query class that created it (in order to create
    the correct SQL).

    The children in this tree are usually either Q-like objects or lists of
    [table_alias, field_name, db_type, lookup_type, value_annotation, params].
    However, a child could also be any class with as_sql() and relabel_aliases() methods.
    """
    default = AND

    def add(self, data, connector):
        """
        Add a node to the where-tree. If the data is a list or tuple, it is
        expected to be of the form (obj, lookup_type, value), where obj is
        a Constraint object, and is then slightly munged before being stored
        (to avoid storing any reference to field objects). Otherwise, the 'data'
        is stored unchanged and can be any class with an 'as_sql()' method.
        """
        if not isinstance(data, (list, tuple)):
            super(WhereNode, self).add(data, connector)
            return

        obj, lookup_type, value = data
        if hasattr(value, '__iter__') and hasattr(value, 'next'):
            # Consume any generators immediately, so that we can determine
            # emptiness and transform any non-empty values correctly.
            value = list(value)

        # The "value_annotation" parameter is used to pass auxilliary information
        # about the value(s) to the query construction. Specifically, datetime
        # and empty values need special handling. Other types could be used
        # here in the future (using Python types is suggested for consistency).
        if isinstance(value, datetime.datetime):
            value_annotation = datetime.datetime
        elif hasattr(value, 'value_annotation'):
            value_annotation = value.value_annotation
        else:
            value_annotation = bool(value)

        if hasattr(obj, "prepare"):
            value = obj.prepare(lookup_type, value)

        super(WhereNode, self).add(
                (obj, lookup_type, value_annotation, value), connector)

    def as_sql(self, qn, connection):
        """
        Returns the SQL version of the where clause and the value to be
        substituted in. Returns None, None if this node is empty.

        If 'node' is provided, that is the root of the SQL generation
        (generally not needed except by the internal implementation for
        recursion).
        """
        if not self.children:
            return None, []
        result = []
        result_params = []
        empty = True
        for child in self.children:
            try:
                if hasattr(child, 'as_sql'):
                    sql, params = child.as_sql(qn=qn, connection=connection)
                else:
                    # A leaf node in the tree.
                    sql, params = self.make_atom(child, qn, connection)

            except EmptyResultSet:
                if self.connector == AND and not self.negated:
                    # We can bail out early in this particular case (only).
                    raise
                elif self.negated:
                    empty = False
                continue
            except FullResultSet:
                if self.connector == OR:
                    if self.negated:
                        empty = True
                        break
                    # We match everything. No need for any constraints.
                    return '', []
                if self.negated:
                    empty = True
                continue

            empty = False
            if sql:
                result.append(sql)
                result_params.extend(params)
        if empty:
            raise EmptyResultSet

        conn = ' %s ' % self.connector
        sql_string = conn.join(result)
        if sql_string:
            if self.negated:
                sql_string = 'NOT (%s)' % sql_string
            elif len(self.children) != 1:
                sql_string = '(%s)' % sql_string
        return sql_string, result_params

    def make_atom(self, child, qn, connection):
        """
        Turn a tuple (Constraint(table_alias, column_name, db_type),
        lookup_type, value_annotation, params) into valid SQL.

        The first item of the tuple may also be an Aggregate.

        Returns the string for the SQL fragment and the parameters to use for
        it.
        """
        lvalue, lookup_type, value_annotation, params_or_value = child
        if isinstance(lvalue, Constraint):
            try:
                lvalue, params = lvalue.process(lookup_type, params_or_value, connection)
            except EmptyShortCircuit:
                raise EmptyResultSet
        elif isinstance(lvalue, Aggregate):
            params = lvalue.field.get_db_prep_lookup(lookup_type, params_or_value, connection)
        else:
            raise TypeError("'make_atom' expects a Constraint or an Aggregate "
                            "as the first item of its 'child' argument.")

        if isinstance(lvalue, tuple):
            # A direct database column lookup.
            field_sql = self.sql_for_columns(lvalue, qn, connection)
        else:
            # A smart object with an as_sql() method.
            field_sql = lvalue.as_sql(qn, connection)

        if value_annotation is datetime.datetime:
            cast_sql = connection.ops.datetime_cast_sql()
        else:
            cast_sql = '%s'

        if hasattr(params, 'as_sql'):
            extra, params = params.as_sql(qn, connection)
            cast_sql = ''
        else:
            extra = ''

        if (len(params) == 1 and params[0] == '' and lookup_type == 'exact'
            and connection.features.interprets_empty_strings_as_nulls):
            lookup_type = 'isnull'
            value_annotation = True

        if lookup_type in connection.operators:
            format = "%s %%s %%s" % (connection.ops.lookup_cast(lookup_type),)
            return (format % (field_sql,
                              connection.operators[lookup_type] % cast_sql,
                              extra), params)

        if lookup_type == 'in':
            if not value_annotation:
                raise EmptyResultSet
            if extra:
                return ('%s IN %s' % (field_sql, extra), params)
            max_in_list_size = connection.ops.max_in_list_size()
            if max_in_list_size and len(params) > max_in_list_size:
                # Break up the params list into an OR of manageable chunks.
                in_clause_elements = ['(']
                for offset in xrange(0, len(params), max_in_list_size):
                    if offset > 0:
                        in_clause_elements.append(' OR ')
                    in_clause_elements.append('%s IN (' % field_sql)
                    group_size = min(len(params) - offset, max_in_list_size)
                    param_group = ', '.join(repeat('%s', group_size))
                    in_clause_elements.append(param_group)
                    in_clause_elements.append(')')
                in_clause_elements.append(')')
                return ''.join(in_clause_elements), params
            else:
                return ('%s IN (%s)' % (field_sql,
                                        ', '.join(repeat('%s', len(params)))),
                        params)
        elif lookup_type in ('range', 'year'):
            return ('%s BETWEEN %%s and %%s' % field_sql, params)
        elif lookup_type in ('month', 'day', 'week_day'):
            return ('%s = %%s' % connection.ops.date_extract_sql(lookup_type, field_sql),
                    params)
        elif lookup_type == 'isnull':
            return ('%s IS %sNULL' % (field_sql,
                (not value_annotation and 'NOT ' or '')), ())
        elif lookup_type == 'search':
            return (connection.ops.fulltext_search_sql(field_sql), params)
        elif lookup_type in ('regex', 'iregex'):
            return connection.ops.regex_lookup(lookup_type) % (field_sql, cast_sql), params

        ############### pg-django >>>>>
        # @> is faster than ANY. ANY can't use GIN indexes
        #elif lookup_type == 'has':
            #return (' %%s = ANY (%s)' % field_sql, params)
        elif lookup_type in ('has_all', 'has'):
            return ('%s @> ARRAY[%s]::%s' % (field_sql,
                                        ', '.join(repeat('%s', len(params))),
                                            get_sql_type(lvalue)),
                    params)
        elif lookup_type == 'has_one':
            return ('%s && ARRAY[%s]::%s' % (field_sql,
                                        ', '.join(repeat('%s', len(params))),
                                            get_sql_type(lvalue)),
                    params)
        ############## <<<<< pg_django

            raise TypeError('Invalid lookup_type: %r' % lookup_type)

    def sql_for_columns(self, data, qn, connection):
        """
        Returns the SQL fragment used for the left-hand side of a column
        constraint (for example, the "T1.foo" portion in the clause
        "WHERE ... T1.foo = 6").
        """
        table_alias, name, db_type = data
        if table_alias:
            lhs = '%s.%s' % (qn(table_alias), qn(name))
        else:
            lhs = qn(name)
        return connection.ops.field_cast_sql(db_type) % lhs

    def relabel_aliases(self, change_map, node=None):
        """
        Relabels the alias values of any children. 'change_map' is a dictionary
        mapping old (current) alias values to the new values.
        """
        if not node:
            node = self
        for pos, child in enumerate(node.children):
            if hasattr(child, 'relabel_aliases'):
                child.relabel_aliases(change_map)
            elif isinstance(child, tree.Node):
                self.relabel_aliases(change_map, child)
            elif isinstance(child, (list, tuple)):
                if isinstance(child[0], (list, tuple)):
                    elt = list(child[0])
                    if elt[0] in change_map:
                        elt[0] = change_map[elt[0]]
                        node.children[pos] = (tuple(elt),) + child[1:]
                else:
                    child[0].relabel_aliases(change_map)

                # Check if the query value also requires relabelling
                if hasattr(child[3], 'relabel_aliases'):
                    child[3].relabel_aliases(change_map)

class EverythingNode(object):
    """
    A node that matches everything.
    """

    def as_sql(self, qn=None, connection=None):
        raise FullResultSet

    def relabel_aliases(self, change_map, node=None):
        return

class NothingNode(object):
    """
    A node that matches nothing.
    """
    def as_sql(self, qn=None, connection=None):
        raise EmptyResultSet

    def relabel_aliases(self, change_map, node=None):
        return

class ExtraWhere(object):
    def __init__(self, sqls, params):
        self.sqls = sqls
        self.params = params

    def as_sql(self, qn=None, connection=None):
        return " AND ".join(self.sqls), tuple(self.params or ())

class Constraint(object):
    """
    An object that can be passed to WhereNode.add() and knows how to
    pre-process itself prior to including in the WhereNode.
    """
    def __init__(self, alias, col, field):
        self.alias, self.col, self.field = alias, col, field

    def __getstate__(self):
        """Save the state of the Constraint for pickling.

        Fields aren't necessarily pickleable, because they can have
        callable default values. So, instead of pickling the field
        store a reference so we can restore it manually
        """
        obj_dict = self.__dict__.copy()
        if self.field:
            obj_dict['model'] = self.field.model
            obj_dict['field_name'] = self.field.name
        del obj_dict['field']
        return obj_dict

    def __setstate__(self, data):
        """Restore the constraint """
        model = data.pop('model', None)
        field_name = data.pop('field_name', None)
        self.__dict__.update(data)
        if model is not None:
            self.field = model._meta.get_field(field_name)
        else:
            self.field = None

    def prepare(self, lookup_type, value):
        if self.field:
            return self.field.get_prep_lookup(lookup_type, value)
        return value

    def process(self, lookup_type, value, connection):
        """
        Returns a tuple of data suitable for inclusion in a WhereNode
        instance.
        """
        # Because of circular imports, we need to import this here.
        from django.db.models.base import ObjectDoesNotExist
        try:
            if self.field:
                params = self.field.get_db_prep_lookup(lookup_type, value,
                    connection=connection, prepared=True)
                db_type = self.field.db_type(connection=connection)
            else:
                # This branch is used at times when we add a comparison to NULL
                # (we don't really want to waste time looking up the associated
                # field object at the calling location).
                params = Field().get_db_prep_lookup(lookup_type, value,
                    connection=connection, prepared=True)
                db_type = None
        except ObjectDoesNotExist:
            raise EmptyShortCircuit

        return (self.alias, self.col, db_type), params

    def relabel_aliases(self, change_map):
        if self.alias in change_map:
            self.alias = change_map[self.alias]
