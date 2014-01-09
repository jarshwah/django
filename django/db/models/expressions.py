import copy
import datetime

from django.core.exceptions import FieldError
from django.db.models.constants import LOOKUP_SEP
from django.db.models.fields import FieldDoesNotExist, IntegerField, FloatField
from django.db.models.query_utils import refs_aggregate
from django.db.models.sql.datastructures import Col
from django.utils import tree


class ExpressionNode(tree.Node):
    """
    Base class for all query expressions.
    """

    # TODO (Josh): allow @add_implementation to change how as_sql generates
    # its output

    # Arithmetic connectors
    ADD = '+'
    SUB = '-'
    MUL = '*'
    DIV = '/'
    POW = '^'
    MOD = '%%'  # This is a quoted % operator - it is quoted
                # because it can be used in strings that also
                # have parameter substitution.

    # Bitwise operators - note that these are generated by .bitand()
    # and .bitor(), the '&' and '|' are reserved for boolean operator
    # usage.
    BITAND = '&'
    BITOR = '|'

    # hooks for adding functionality
    validate_name = False
    wraps_expression = False

    is_aggregate = False

    def __init__(self, children=None, connector=None, negated=False):
        if children is not None and len(children) > 1 and connector is None:
            raise TypeError('You have to specify a connector.')
        super(ExpressionNode, self).__init__(children, connector, negated)
        self.col = None
        self.source = None


    def _combine(self, other, connector, reversed, node=None):
        if isinstance(other, datetime.timedelta):
            return DateModifierNode([self, other], connector)

        if not isinstance(other, ExpressionNode):
            # everything must be some kind of ExpressionNode, so ValueNode is the fallback
            other = ValueNode(other)

        if reversed:
            obj = ExpressionNode([other], connector)
            obj.add(node or self, connector)
        else:
            obj = node or ExpressionNode([self], connector)
            obj.add(other, connector)
        return obj

    def contains_aggregate(self, existing_aggregates):
        if self.children:
            return any(child.contains_aggregate(existing_aggregates)
                       for child in self.children
                       if hasattr(child, 'contains_aggregate'))

        if self.validate_name:
            return refs_aggregate(self.name.split(LOOKUP_SEP),
                                  existing_aggregates)

        if self.wraps_expression:
            return self.expression.contains_aggregate(existing_aggregates)

        return False, ()

    def prepare_database_save(self, unused):
        return self

    #############
    # EVALUATOR #
    #############

    def relabeled_clone(self, change_map):
        clone = copy.copy(self)
        if hasattr(clone.col, 'relabeled_clone'):
            clone.col = clone.col.relabeled_clone(change_map)
        elif clone.col:
            clone.col = (change_map.get(clone.col[0], clone.col[0]), clone.col[1])

        # rebuild the clones children with new relabeled clones..
        new_children = [
            child.relabeled_clone(change_map) if hasattr(child, 'relabeled_clone')
            else child
            for child in clone.children]
        clone.children = new_children

        if self.wraps_expression:
            clone.expression = clone.expression.relabeled_clone(change_map)

        return clone

    def as_sql(self, compiler, connection):
        expressions = []
        expression_params = []
        for child in self.children:
            sql, params = compiler.compile(child)
            expressions.append(sql)
            expression_params.extend(params)

        if self.connector == self.default:
            return self.get_sql(compiler, connection)

        expression_wrapper = '%s'
        if len(self.children) > 1:
            # order of precedence
            expression_wrapper = '(%s)'

        sql = connection.ops.combine_expression(self.connector, expressions)
        return expression_wrapper % sql, expression_params

    def evaluate(self, compiler, connection):
        # this method is here for compatability purposes
        return self.as_sql(compiler, connection)

    def prepare(self, query=None, allow_joins=True, reuse=None):
        if not allow_joins and self.validate_name and LOOKUP_SEP in self.name:
            raise FieldError("Joined field references are not permitted in this query")

        for child in self.children:
            if hasattr(child, 'prepare'):
                child.prepare(query, allow_joins, reuse)

        if self.wraps_expression:
            self.expression.prepare(query, allow_joins, reuse)

        if self.validate_name:
            self.setup_cols(query, reuse)

        return self

    def setup_cols(self, query, reuse):
        if query is None:
            return
        field_list = self.name.split(LOOKUP_SEP)
        if self.name in query.aggregates:
            self.col = query.aggregate_select[self.name]
        else:
            try:
                field, sources, opts, join_list, path = query.setup_joins(
                    field_list, query.get_meta(),
                    query.get_initial_alias(), reuse)
                self._used_joins = join_list
                targets, _, join_list = query.trim_joins(sources, join_list, path)
                if reuse is not None:
                    reuse.update(join_list)
                for t in targets:
                    source = self.source if self.source is not None else sources[0]
                    self.col = Col(join_list[-1], t, source)
                if self.source is None:
                    self.source = sources[0]
            except FieldDoesNotExist:
                raise FieldError("Cannot resolve keyword %r into field. "
                                 "Choices are: %s" % (self.name,
                                                      [f.name for f in self.opts.fields]))

    def get_cols(self):
        cols = []

        for child in self.children:
            cols.extend(child.get_cols())
        if self.wraps_expression:
            # Note: we intentionally skip returning wrapped columns! They are not needed
            pass
        if isinstance(self.col, tuple):
            cols.append(self.col)
        elif hasattr(self.col, 'get_cols'):
            cols.extend(self.col.get_cols())
        return cols

    def get_sources(self):
        sources = [self.source] if self.source is not None else []
        for child in self.children:
            sources.extend(child.get_sources())
        if self.wraps_expression:
            sources.extend(self.expression.get_sources())
        return sources

    def get_sql(self, compiler, connection):
        return '', []

    #############
    # OPERATORS #
    #############

    def __add__(self, other):
        return self._combine(other, self.ADD, False)

    def __sub__(self, other):
        return self._combine(other, self.SUB, False)

    def __mul__(self, other):
        return self._combine(other, self.MUL, False)

    def __truediv__(self, other):
        return self._combine(other, self.DIV, False)

    def __div__(self, other):  # Python 2 compatibility
        return type(self).__truediv__(self, other)

    def __mod__(self, other):
        return self._combine(other, self.MOD, False)

    def __pow__(self, other):
        return self._combine(other, self.POW, False)

    def __and__(self, other):
        raise NotImplementedError(
            "Use .bitand() and .bitor() for bitwise logical operations."
        )

    def bitand(self, other):
        return self._combine(other, self.BITAND, False)

    def __or__(self, other):
        raise NotImplementedError(
            "Use .bitand() and .bitor() for bitwise logical operations."
        )

    def bitor(self, other):
        return self._combine(other, self.BITOR, False)

    def __radd__(self, other):
        return self._combine(other, self.ADD, True)

    def __rsub__(self, other):
        return self._combine(other, self.SUB, True)

    def __rmul__(self, other):
        return self._combine(other, self.MUL, True)

    def __rtruediv__(self, other):
        return self._combine(other, self.DIV, True)

    def __rdiv__(self, other):  # Python 2 compatibility
        return type(self).__rtruediv__(self, other)

    def __rmod__(self, other):
        return self._combine(other, self.MOD, True)

    def __rpow__(self, other):
        return self._combine(other, self.POW, True)

    def __rand__(self, other):
        raise NotImplementedError(
            "Use .bitand() and .bitor() for bitwise logical operations."
        )

    def __ror__(self, other):
        raise NotImplementedError(
            "Use .bitand() and .bitor() for bitwise logical operations."
        )


class F(ExpressionNode):
    """
    An expression representing the value of the given field.
    """

    validate_name = True

    def __init__(self, name):
        super(F, self).__init__(None, None, False)
        self.name = name

    def get_sql(self, compiler, connection):
        if hasattr(self.col, 'as_sql'):
            return self.col.as_sql(compiler, connection)
        qn = compiler
        return '%s.%s' % (qn(self.col[0]), qn(self.col[1])), []


class ValueNode(ExpressionNode):
    """
    Represents a wrapped value as a node, allowing all children
    to act as nodes
    """

    def __init__(self, name):
        super(ValueNode, self).__init__(None, None, False)
        self.name = name

    def get_sql(self, compiler, connection):
        return '%s' % self.name, []


class ColumnNode(ValueNode):
    """
    Represents a node that wraps a column object, allowing objects
    that can act as Expressions to be used fully as one.
    """

    def __init__(self, column):
        if not hasattr(column, 'as_sql') or not hasattr(column, 'relabeled_clone'):
            raise TypeError("'column' must implement as_sql() and relabeled_clone()")
        super(ColumnNode, self).__init__(column)
        self.col = column
        if hasattr(column, 'source'):
            self.source = column.source

    def relabeled_clone(self, change_map):
        return self.name.relabeled_clone(change_map)

    def get_sql(self, compiler, connection):
        return self.name.as_sql(compiler, connection)


class DateModifierNode(ExpressionNode):
    """
    Node that implements the following syntax:
    filter(end_date__gt=F('start_date') + datetime.timedelta(days=3, seconds=200))

    which translates into:
    POSTGRES:
        WHERE end_date > (start_date + INTERVAL '3 days 200 seconds')

    MYSQL:
        WHERE end_date > (start_date + INTERVAL '3 0:0:200:0' DAY_MICROSECOND)

    ORACLE:
        WHERE end_date > (start_date + INTERVAL '3 00:03:20.000000' DAY(1) TO SECOND(6))

    SQLITE:
        WHERE end_date > django_format_dtdelta(start_date, "+" "3", "200", "0")
        (A custom function is used in order to preserve six digits of fractional
        second information on sqlite, and to format both date and datetime values.)

    Note that microsecond comparisons are not well supported with MySQL, since
    MySQL does not store microsecond information.

    Only adding and subtracting timedeltas is supported, attempts to use other
    operations raise a TypeError.
    """
    def __init__(self, children, connector, negated=False):
        if len(children) != 2:
            raise TypeError('Must specify a node and a timedelta.')
        if not isinstance(children[1], datetime.timedelta):
            raise TypeError('Second child must be a timedelta.')
        if connector not in (self.ADD, self.SUB):
            raise TypeError('Connector must be + or -, not %s' % connector)
        super(DateModifierNode, self).__init__(children, connector, negated)

    def as_sql(self, compiler, connection):
        field, timedelta = self.children
        sql, params = field.as_sql(compiler, connection)

        if (timedelta.days == timedelta.seconds == timedelta.microseconds == 0):
            return sql, params

        return connection.ops.date_interval_sql(sql, self.connector, timedelta), params
