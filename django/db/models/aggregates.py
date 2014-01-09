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
        self.is_summary = False
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
                annotation = query.aggregates[name]
                if not hasattr(annotation, 'expression'):
                    # trying to aggregate over a complex annotation.. how?!
                    # somehow need to build the column, from the annotation,
                    # without including the annotation function
                    raise FieldError("Cannot compute an aggregation over a complex annotation")
                    # investigate how to fix this
                else:
                    if self.source is None:
                        self.source = annotation.source
                    col = annotation.expression.col
                    # use the annotation alias otherwise we rebuild the full node inside the aggregation
                    # this is too hacky - solve this, and we can solve the above fielderror
                    if hasattr(col, 'as_sql'):
                        self.expression.col = (col.alias, name)
                    else:
                        self.expression.col = (col[0], name)
                    return
        elif self.is_summary:
            # complex aggregation, check all parts:
            #   - .aggregate(Sum(F('field')+F('other')))
            #   - .aggregate(Sum('field')+Sum('other'))
            pass
        super(Aggregate, self).prepare(query, allow_joins, reuse)
        if self.source is None:
            # try to resolve it
            sources = self.get_sources()
            num_sources = len(sources)
            if num_sources == 0:
                raise FieldError("Cannot resolve aggregate type, unknown field_type")
            elif num_sources == 1:
                self.source = sources[0]
            else:
                # this could be smarter by allowing certain combinations
                self.source = sources[0]
                for source in sources:
                    if not isinstance(self.source, source.__class__):
                        raise FieldError("Complex aggregate contains mixed types. You \
                            must set field_type")

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