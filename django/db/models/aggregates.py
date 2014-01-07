"""
Classes to represent the definitions of aggregate functions.
"""
from django.core.exceptions import FieldError
from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import ExpressionNode, F, ValueNode
from django.db.models.fields import IntegerField, FloatField

__all__ = [
    'Aggregate', 'Avg', 'Count', 'Max', 'Min', 'StdDev', 'Sum', 'Variance',
]


ordinal_aggregate_field = IntegerField()
computed_aggregate_field = FloatField()


class Aggregate(ExpressionNode):
    # what needs doing?
    #   - potentially modify compiler.py to use different value types

    is_aggregate = True
    wraps_expression = True
    is_ordinal = False
    is_computed = False
    sql_template = '%(function)s(%(field)s)'
    sql_function = None
    name = None

    def __init__(self, expression, field_type=None, **extra):
        super(Aggregate, self).__init__(None, None, False)
        if not isinstance(expression, ExpressionNode):
            # handle traditional string fields by wrapping
            expression = F(expression)
        self.expression = expression
        self.extra = extra
        self.is_summary = False

        if expression.is_aggregate:
            raise FieldError("Aggregates %s(%s(..)) cannot be nested" %
                (self.name, expression.name))

        if self.is_ordinal:
            self.source = ordinal_aggregate_field
        elif self.is_computed:
            self.source = computed_aggregate_field
        else:
            self.source = field_type

    @property
    def output_type(self):
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
        return self.source

    @property
    def field(self):
        return self.output_type

    def _default_alias(self):
        return '%s__%s' % (self.expression.name, self.name.lower())
    default_alias = property(_default_alias)

    def get_sql(self, compiler, connection):
        sql, params = compiler.compile(self.expression)
        substitutions = {
            'function': self.sql_function,
            'field': sql
        }
        substitutions.update(self.extra)
        return self.sql_template % substitutions, params

    def get_lookup(self, lookup):
        return self.output_type.get_lookup(lookup)

    def __bool__(self):
        """
        For truth value testing.
        """
        return True


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