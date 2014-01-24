import copy
from django.db.models.expressions import ExpressionNode, ValueNode, ColumnNode
from django.db.models.sql.where import WhereNode, AND


class If(ExpressionNode):
    sql_template = 'CASE WHEN %(condition)s THEN %(true_value)s ELSE %(false_value)s END'

    def __init__(self, condition, true_value=True, false_value=None, output_type=None, **extra):
        super(If, self).__init__(None, None, False)
        self.condition = condition
        self.true_value = self._wrapped_value(true_value)
        self.false_value = self._wrapped_value(false_value)
        self.source = output_type
        self.extra = extra
        if 'sql_template' not in extra:
            self.extra['sql_template'] = self.sql_template

    def _wrapped_value(self, value):
        if not isinstance(value, ExpressionNode):
            if hasattr(value, 'as_sql'):
                return ColumnNode(value)
            else:
                return ValueNode(value)
        return value

    def prepare(self, query=None, allow_joins=True, reuse=None):
        where = WhereNode()
        clause, require_inner = query._add_q(self.condition, query.used_aliases)
        where.add(clause, AND)
        self.where = where
        self.true_value.prepare(query, allow_joins, reuse)
        self.false_value.prepare(query, allow_joins, reuse)
        return self

    def relabeled_clone(self, change_map):
        clone = copy.copy(self)
        clone.condition = self.condition.relabeled_clone(change_map)
        clone.true_value = self.true_value.relabeled_clone(change_map)
        clone.false_value = self.false_value.relabeled_clone(change_map)
        return clone

    def as_sql(self, compiler, connection):
        condition_sql, params = compiler.compile(self.where)
        self.extra['condition'] = condition_sql
        true_sql, true_params = compiler.compile(self.true_value)
        self.extra['true_value'] = true_sql
        params.extend(true_params)
        false_sql, false_params = compiler.compile(self.false_value)
        self.extra['false_value'] = false_sql
        params.extend(false_params)
        template = self.extra['sql_template']
        return template % self.extra, params
