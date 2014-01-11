"""
Classes to represent the definitions of aggregate functions.
"""
from django.core.exceptions import FieldError
from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import (
    ExpressionNode,
    F,
    ValueNode,
    ColumnNode,
    ordinal_aggregate_field,
    computed_aggregate_field)
from django.db.models.fields import IntegerField, FloatField

__all__ = [
    'Aggregate', 'Avg', 'Count', 'Max', 'Min', 'StdDev', 'Sum', 'Variance',
]


class Aggregate(ExpressionNode):
    # what needs doing?
    #   - potentially modify compiler.py to use different value types

    is_aggregate = True
    wraps_expression = True
    sql_template = '%(function)s(%(field)s)'
    sql_function = None
    name = None

    def __init__(self, expression, field_type=None, **extra):
        super(Aggregate, self).__init__(None, None, False)
        if not isinstance(expression, ExpressionNode):
            if hasattr(expression, 'as_sql'):
                expression = ColumnNode(expression)
            else:
                # handle traditional string fields by wrapping
                expression = F(expression)
        self.expression = expression
        self.extra = extra
        self.source = field_type

        if expression.is_aggregate:
            raise FieldError("Aggregates %s(%s(..)) cannot be nested" %
                (self.name, expression.name))

    def prepare(self, query=None, allow_joins=True, reuse=None):
        if self.expression.validate_name: # simple lookup
            name = self.expression.name
            field_list = name.split(LOOKUP_SEP)
            # this whole block was moved from sql/query.py to encapsulate, but will that
            # possibly break custom query classes? The derived prepare() saves us a lot
            # of extra boilerplate though
            if len(field_list) == 1 and name in query.aggregates:
                if not self.is_summary:
                    raise FieldError("Cannot compute %s('%s'): '%s' is an aggregate" % (
                        self.name, name, name))
                # aggregation is over an annotation
                # manually set the column, and don't prepare the expression
                # otherwise the full annotation, including the agg function,
                # is built inside this aggregation.
                annotation = query.aggregates[name]
                self.expression.col = (None, name)
                self.source = annotation.source
                return
        super(Aggregate, self).prepare(query, allow_joins, reuse)
        self._resolve_source()

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
    name = 'Avg'


class Count(Aggregate):
    is_ordinal = True
    sql_function = 'COUNT'
    name = 'Count'
    sql_template = '%(function)s(%(distinct)s%(field)s)'

    def __init__(self, expression, distinct=False, **extra):
        if expression == '*':
            expression = ValueNode(expression)
            expression.source = ordinal_aggregate_field
        super(Count, self).__init__(expression, distinct='DISTINCT ' if distinct else '', **extra)


class Max(Aggregate):
    sql_function = 'MAX'
    name = 'Max'


class Min(Aggregate):
    sql_function = 'MIN'
    name = 'Min'


class StdDev(Aggregate):
    is_computed = True
    name = 'StdDev'

    def __init__(self, expression, sample=False, **extra):
        super(StdDev, self).__init__(expression, **extra)
        self.sql_function = 'STDDEV_SAMP' if sample else 'STDDEV_POP'


class Sum(Aggregate):
    sql_function = 'SUM'
    name = 'Sum'


class Variance(Aggregate):
    is_computed = True
    name = 'Variance'

    def __init__(self, expression, sample=False, **extra):
        super(Variance, self).__init__(expression, **extra)
        self.sql_function = 'VAR_SAMP' if sample else 'VAR_POP'