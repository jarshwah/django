import copy
import datetime

from django.core.exceptions import FieldError
from django.db.models.aggregates import refs_aggregate
from django.db.models.constants import LOOKUP_SEP
from django.db.models.fields import FieldDoesNotExist, IntegerField, FloatField
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

        return False

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

        if self.connector is self.default:
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
        if not allow_joins and hasattr(self, 'name') and LOOKUP_SEP in self.name:
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
                    self.col = (join_list[-1], t.column)
            except FieldDoesNotExist:
                raise FieldError("Cannot resolve keyword %r into field. "
                                 "Choices are: %s" % (self.name,
                                                      [f.name for f in self.opts.fields]))

    def get_cols(self):
        cols = []
        for child in self.children:
            cols.extend(child.get_cols())
        if self.wraps_expression:
            cols.extend(self.expression.get_cols())
        if isinstance(self.col, tuple):
            cols.append(self.col)
        return cols

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
        return '%s.%s' % (compiler(self.col[0]), compiler(self.col[1])), []


class ValueNode(ExpressionNode):
    """
    Represents a wrapped value as a node, allowing all children
    to act as nodes
    """

    def __init__(self, value):
        super(ValueNode, self).__init__(None, None, False)
        self.value = value

    def get_sql(self, compiler, connection):
        return '%s' % self.value, []


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


class Aggregate(ExpressionNode):
    # what does an aggregate need to do?
    #   - inform compiler that this is an aggregate
    #   - produce the sql required
    #   - possibly hold on to the alias?

    # what needs doing?
    #   - modify query.annotate to accept all types of expression nodes
    #   - modify query.aggregate to accept Aggregate Nodes
    #   - potentially modify compiler.py to use different value types

    is_aggregate = True
    wraps_expression = True
    is_ordinal = False
    is_computed = False
    sql_template = '%(function)s(%(field)s)'
    sql_function = None
    aggregate_name = None

    def __init__(self, expression, **extra):
        if not isinstance(expression, ExpressionNode):
            self.original_lookup = expression
            # handle traditional string fields by wrapping
            expression = F(expression)
        self.expression = expression
        self.extra = extra
        super(Aggregate, self).__init__(None, None, False)

    def _default_alias(self):
        if not hasattr(self, 'original_lookup'):
            raise TypeError("Must supply an alias for complex aggregates")
        return '%s__%s' % (self.original_lookup, self.name.lower())
    default_alias = property(_default_alias)

    def get_sql(self, compiler, connection):
        sql, params = compiler.compile(self.expression)
        substitutions = {
            'function': self.sql_function,
            'field': sql
        }
        substitutions.update(self.extra)
        return self.sql_template % substitutions, params


class Avg(Aggregate):
    is_computed = True
    sql_function = 'AVG'
    aggregate_name = 'Avg'


class Count(Aggregate):
    is_ordinal = True
    sql_function = 'COUNT'
    aggregate_name = 'Count'
    sql_template = '%(function)s(%(distinct)s%(field)s)'

    def __init__(self, expression, distinct=False, **extra):
        self.distinct = distinct
        super(Count, self).__init__(expression, distinct='DISTINCT ' if distinct else '', **extra)


class Max(Aggregate):
    sql_function = 'MAX'
    aggregate_name = 'Max'


class Min(Aggregate):
    sql_function = 'MIN'
    aggregate_name = 'Min'


class StdDev(Aggregate):
    is_computed = True
    aggregate_name = 'StdDev'

    def __init__(self, expression, sample=False, **extra):
        super(StdDev, self).__init__(expression, **extra)
        self.sql_function = 'STDDEV_SAMP' if sample else 'STDDEV_POP'


class Sum(Aggregate):
    sql_function = 'SUM'
    aggregate_name = 'Sum'


class Variance(Aggregate):
    is_computed = True
    aggregate_name = 'Variance'

    def __init__(self, expression, sample=False, **extra):
        super(Variance, self).__init__(expression, **extra)
        self.sql_function = 'VAR_SAMP' if sample else 'VAR_POP'
