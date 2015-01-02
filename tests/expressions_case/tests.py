from __future__ import unicode_literals

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from operator import attrgetter
from unittest import TestSuite
from uuid import UUID

from django.core.exceptions import FieldError
from django.db import models
from django.db.models import F, Q, Value, Min, Max
from django.db.models.expressions import SearchedCase, SimpleCase
from django.test import TestCase
from django.utils.six import binary_type, text_type, assertRaisesRegex

from .models import CaseTestModel, O2OCaseTestModel, FKCaseTestModel


class BaseCaseExpressionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        o = CaseTestModel.objects.create(integer=1, integer2=1, string='1')
        O2OCaseTestModel.objects.create(o2o=o, integer=1)
        FKCaseTestModel.objects.create(fk=o, integer=1)

        o = CaseTestModel.objects.create(integer=2, integer2=3, string='2')
        O2OCaseTestModel.objects.create(o2o=o, integer=2)
        FKCaseTestModel.objects.create(fk=o, integer=2)
        FKCaseTestModel.objects.create(fk=o, integer=3)

        o = CaseTestModel.objects.create(integer=3, integer2=4, string='3')
        O2OCaseTestModel.objects.create(o2o=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=4)

        o = CaseTestModel.objects.create(integer=2, integer2=2, string='2')
        O2OCaseTestModel.objects.create(o2o=o, integer=2)
        FKCaseTestModel.objects.create(fk=o, integer=2)
        FKCaseTestModel.objects.create(fk=o, integer=3)

        o = CaseTestModel.objects.create(integer=3, integer2=4, string='3')
        O2OCaseTestModel.objects.create(o2o=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=4)

        o = CaseTestModel.objects.create(integer=3, integer2=3, string='3')
        O2OCaseTestModel.objects.create(o2o=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=3)
        FKCaseTestModel.objects.create(fk=o, integer=4)

        o = CaseTestModel.objects.create(integer=4, integer2=5, string='4')
        O2OCaseTestModel.objects.create(o2o=o, integer=1)
        FKCaseTestModel.objects.create(fk=o, integer=5)


    def test_annotate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(test=self.create_expression(
                'integer', [(Value(1), Value('one')), (Value(2), Value('two'))],
                default=Value('other'),
                output_field=models.CharField())).order_by('pk'),
            [(1, 'one'), (2, 'two'), (3, 'other'), (2, 'two'), (3, 'other'), (3, 'other'), (4, 'other')],
            transform=attrgetter('integer', 'test'))

    def test_annotate_without_default(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(test=self.create_expression(
                'integer', [(Value(1), Value(1)), (Value(2), Value(2))],
                output_field=models.IntegerField())).order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'test'))

    def test_annotate_with_expression_as_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(f_test=self.create_expression(
                'integer',
                [(Value(1), F('integer') + 1),
                 (Value(2), F('integer') + 3)],
                default='integer')).order_by('pk'),
            [(1, 2), (2, 5), (3, 3), (2, 5), (3, 3), (3, 3), (4, 4)],
            transform=attrgetter('integer', 'f_test'))

    def test_annotate_with_expression_as_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(f_test=self.create_expression(
                'integer2',
                [(F('integer'), Value('equal')),
                 (F('integer') + 1, Value('+1'))],
                output_field=models.CharField())).order_by('pk'),
            [(1, 'equal'), (2, '+1'), (3, '+1'), (2, 'equal'), (3, '+1'), (3, 'equal'), (4, '+1')],
            transform=attrgetter('integer', 'f_test'))

    def test_annotate_with_join_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(join_test=self.create_expression(
                'integer',
                [(Value(1), F('o2o_rel__integer') + 1),
                 (Value(2), F('o2o_rel__integer') + 3)],
                default='o2o_rel__integer')).order_by('pk'),
            [(1, 2), (2, 5), (3, 3), (2, 5), (3, 3), (3, 3), (4, 1)],
            transform=attrgetter('integer', 'join_test'))

    def test_annotate_with_join_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(join_test=self.create_expression(
                'integer2',
                [(F('o2o_rel__integer'), Value('equal')),
                 (F('o2o_rel__integer') + 1, Value('+1'))],
                default=Value('other'),
                output_field=models.CharField())).order_by('pk'),
            [(1, 'equal'), (2, '+1'), (3, '+1'), (2, 'equal'), (3, '+1'), (3, 'equal'), (4, 'other')],
            transform=attrgetter('integer', 'join_test'))

    def test_annotate_with_join_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(join_test=self.create_expression(
                'o2o_rel__integer',
                [(Value(1), Value('one')),
                 (Value(2), Value('two')),
                 (Value(3), Value('three'))],
                default=Value('other'),
                output_field=models.CharField())).order_by('pk'),
            [(1, 'one'), (2, 'two'), (3, 'three'), (2, 'two'), (3, 'three'), (3, 'three'), (4, 'one')],
            transform=attrgetter('integer', 'join_test'))

    def test_annotate_with_annotation_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f_plus_1=F('integer') + 1,
                f_plus_3=F('integer') + 3
            ).annotate(
                f_test=self.create_expression(
                    'integer',
                    [(Value(1), 'f_plus_1'),
                     (Value(2), 'f_plus_3')],
                    default='integer')).order_by('pk'),
            [(1, 2), (2, 5), (3, 3), (2, 5), (3, 3), (3, 3), (4, 4)],
            transform=attrgetter('integer', 'f_test'))

    def test_annotate_with_annotation_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f_plus_1=F('integer') + 1
            ).annotate(
                f_test=self.create_expression(
                    'integer2',
                    [(F('integer'), Value('equal')),
                     (F('f_plus_1'), Value('+1'))],
                    output_field=models.CharField())).order_by('pk'),
            [(1, 'equal'), (2, '+1'), (3, '+1'), (2, 'equal'), (3, '+1'), (3, 'equal'), (4, '+1')],
            transform=attrgetter('integer', 'f_test'))

    def test_annotate_with_annotation_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f_minus_2=F('integer') - 2
            ).annotate(
                test=self.create_expression(
                    'f_minus_2',
                    [(Value(-1), Value('negative one')),
                     (Value(0), Value('zero')),
                     (Value(1), Value('one'))],
                    default=Value('other'),
                    output_field=models.CharField())).order_by('pk'),
            [(1, 'negative one'), (2, 'zero'), (3, 'one'), (2, 'zero'), (3, 'one'), (3, 'one'), (4, 'other')],
            transform=attrgetter('integer', 'test'))

    def test_annotate_with_aggregation_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                min=Min('fk_rel__integer'),
                max=Max('fk_rel__integer')
            ).annotate(
                test=self.create_expression(
                    'integer',
                    [(Value(2), 'min'),
                     (Value(3), 'max')])).order_by('pk'),
            [(1, None, 1, 1), (2, 2, 2, 3), (3, 4, 3, 4), (2, 2, 2, 3), (3, 4, 3, 4), (3, 4, 3, 4), (4, None, 5, 5)],
            transform=attrgetter('integer', 'test', 'min', 'max'))

    def test_annotate_with_aggregation_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                min=Min('fk_rel__integer'),
                max=Max('fk_rel__integer')
            ).annotate(
                test=self.create_expression(
                    'integer2',
                    [(F('min'), Value('min')),
                     (F('max'), Value('max'))],
                    output_field=models.CharField())).order_by('pk'),
            [(1, 1, 'min'), (2, 3, 'max'), (3, 4, 'max'), (2, 2, 'min'), (3, 4, 'max'), (3, 3, 'min'), (4, 5, 'min')],
            transform=attrgetter('integer', 'integer2', 'test'))

    def test_annotate_with_aggregation_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                max=Max('fk_rel__integer')
            ).annotate(
                test=self.create_expression(
                    'max',
                    [(Value(3), Value('max = 3')),
                     (Value(4), Value('max = 4'))],
                    output_field=models.CharField())).order_by('pk'),
            [(1, 1, None), (2, 3, 'max = 3'), (3, 4, 'max = 4'), (2, 3, 'max = 3'), (3, 4, 'max = 4'),
             (3, 4, 'max = 4'), (4, 5, None)],
            transform=attrgetter('integer', 'max', 'test'))

    def test_in_subquery(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(
                pk__in=CaseTestModel.objects.annotate(
                    test=self.create_expression(
                        'integer',
                        [(F('integer2'), 'pk'),
                         (Value(4), 'pk')],
                        output_field=models.IntegerField())
                ).values('test')).order_by('pk'),
            [(1, 1), (2, 2), (3, 3), (4, 5)],
            transform=attrgetter('integer', 'integer2'))

    def test_aggregate(self):
        self.assertEqual(
            CaseTestModel.objects.aggregate(
                one=models.Sum(self.create_expression(
                    'integer', [(Value(1), Value(1))],
                    output_field=models.IntegerField())),
                two=models.Sum(self.create_expression(
                    'integer', [(Value(2), Value(1))],
                    output_field=models.IntegerField())),
                three=models.Sum(self.create_expression(
                    'integer', [(Value(3), Value(1))],
                    output_field=models.IntegerField())),
                four=models.Sum(self.create_expression(
                    'integer', [(Value(4), Value(1))],
                    output_field=models.IntegerField()))),
            {'one': 1, 'two': 2, 'three': 3, 'four': 1})

    def test_aggregate_with_expression_as_value(self):
        self.assertEqual(
            CaseTestModel.objects.aggregate(
                one=models.Sum(self.create_expression(
                    'integer', [(Value(1), 'integer')])),
                two=models.Sum(self.create_expression(
                    'integer', [(Value(2), F('integer') - 1)])),
                three=models.Sum(self.create_expression(
                    'integer', [(Value(3), F('integer') + 1)]))),
            {'one': 1, 'two': 2, 'three': 12})

    def test_aggregate_with_expression_as_condition(self):
        self.assertEqual(
            CaseTestModel.objects.aggregate(
                equal=models.Sum(self.create_expression(
                    'integer2',
                    [(F('integer'), Value(1))],
                    output_field=models.IntegerField())),
                plus_one=models.Sum(self.create_expression(
                    'integer2',
                    [(F('integer') + 1, Value(1))],
                    output_field=models.IntegerField()))),
            {'equal': 3, 'plus_one': 4})

    def test_filter(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                'integer', [(Value(2), Value(3)), (Value(3), Value(4))],
                default=Value(1),
                output_field=models.IntegerField())).order_by('pk'),
            [(1, 1), (2, 3), (3, 4), (3, 4)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_without_default(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                'integer', [(Value(2), Value(3)), (Value(3), Value(4))],
                output_field=models.IntegerField())).order_by('pk'),
            [(2, 3), (3, 4), (3, 4)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_expression_as_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                'integer',
                [(Value(2), F('integer') + 1),
                 (Value(3), F('integer'))],
                default='integer')).order_by('pk'),
            [(1, 1), (2, 3), (3, 3)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_expression_as_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(string=self.create_expression(
                'integer2',
                [(F('integer'), Value('2')),
                 (F('integer') + 1, Value('3'))],
                output_field=models.CharField())).order_by('pk'),
            [(3, 4, '3'), (2, 2, '2'), (3, 4, '3')],
            transform=attrgetter('integer', 'integer2', 'string'))

    def test_filter_with_join_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                'integer',
                [(Value(2), F('o2o_rel__integer') + 1),
                 (Value(3), F('o2o_rel__integer'))],
                default='o2o_rel__integer')).order_by('pk'),
            [(1, 1), (2, 3), (3, 3)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_join_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer=self.create_expression(
                'integer2',
                [(F('o2o_rel__integer') + 1, Value(2)),
                 (F('o2o_rel__integer'), Value(3))],
                output_field=models.IntegerField())).order_by('pk'),
            [(2, 3), (3, 3)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_join_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                'o2o_rel__integer',
                [(Value(1), Value(1)),
                 (Value(2), Value(3)),
                 (Value(3), Value(4))],
                output_field=models.IntegerField())).order_by('pk'),
            [(1, 1), (2, 3), (3, 4), (3, 4)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_annotation_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f=F('integer'),
                f_plus_1=F('integer') + 1
            ).filter(
                integer2=self.create_expression(
                    'integer',
                    [(Value(2), 'f_plus_1'),
                     (Value(3), 'f')])).order_by('pk'),
            [(2, 3), (3, 3)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_annotation_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f_plus_1=F('integer') + 1
            ).filter(
                integer=self.create_expression(
                    'integer2',
                    [(F('integer'), Value(2)),
                     (F('f_plus_1'), Value(3))],
                    output_field=models.IntegerField())).order_by('pk'),
            [(3, 4), (2, 2), (3, 4)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_annotation_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                f_plus_1=F('integer') + 1
            ).filter(
                integer2=self.create_expression(
                    'f_plus_1',
                    [(Value(3), Value(3)),
                     (Value(4), Value(4))],
                    default=Value(1),
                    output_field=models.IntegerField())).order_by('pk'),
            [(1, 1), (2, 3), (3, 4), (3, 4)],
            transform=attrgetter('integer', 'integer2'))

    def test_filter_with_aggregation_in_value(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                min=Min('fk_rel__integer'),
                max=Max('fk_rel__integer')
            ).filter(
                integer2=self.create_expression(
                    'integer',
                    [(Value(2), 'min'),
                     (Value(3), 'max')])).order_by('pk'),
            [(3, 4, 3, 4), (2, 2, 2, 3), (3, 4, 3, 4)],
            transform=attrgetter('integer', 'integer2', 'min', 'max'))

    def test_filter_with_aggregation_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                min=Min('fk_rel__integer'),
                max=Max('fk_rel__integer')
            ).filter(
                integer=self.create_expression(
                    'integer2',
                    [(F('min'), Value(2)),
                     (F('max'), Value(3))])).order_by('pk'),
            [(3, 4, 3, 4), (2, 2, 2, 3), (3, 4, 3, 4)],
            transform=attrgetter('integer', 'integer2', 'min', 'max'))

    def test_filter_with_aggregation_in_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                max=Max('fk_rel__integer')
            ).filter(
                integer=self.create_expression(
                    'max',
                    [(Value(3), Value(2)),
                     (Value(4), Value(3))])).order_by('pk'),
            [(2, 3, 3), (3, 4, 4), (2, 2, 3), (3, 4, 4), (3, 3, 4)],
            transform=attrgetter('integer', 'integer2', 'max'))

    def test_update(self):
        CaseTestModel.objects.update(
            string=self.create_expression(
                'integer', [(Value(1), Value('one')), (Value(2), Value('two'))],
                default=Value('other')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 'one'), (2, 'two'), (3, 'other'), (2, 'two'), (3, 'other'), (3, 'other'), (4, 'other')],
            transform=attrgetter('integer', 'string'))

    def test_update_without_default(self):
        CaseTestModel.objects.update(
            integer2=self.create_expression(
                'integer', [(Value(1), Value(1)), (Value(2), Value(2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'integer2'))

    def test_update_with_expression_as_value(self):
        CaseTestModel.objects.update(
            integer=self.create_expression(
                'integer',
                [(Value(1), F('integer') + 1),
                 (Value(2), F('integer') + 3)],
                default='integer'))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [('1', 2), ('2', 5), ('3', 3), ('2', 5), ('3', 3), ('3', 3), ('4', 4)],
            transform=attrgetter('string', 'integer'))

    def test_update_with_expression_as_condition(self):
        CaseTestModel.objects.update(
            string=self.create_expression(
                'integer2',
                [(F('integer'), Value('equal')),
                 (F('integer') + 1, Value('+1'))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 'equal'), (2, '+1'), (3, '+1'), (2, 'equal'), (3, '+1'), (3, 'equal'), (4, '+1')],
            transform=attrgetter('integer', 'string'))

    def test_update_with_join_in_condition_raise_field_error(self):
        with assertRaisesRegex(self, FieldError, 'Joined field references are not permitted in this query'):
            CaseTestModel.objects.update(
                integer=self.create_expression(
                    'integer2',
                    [(F('o2o_rel__integer') + 1, Value(2)),
                     (F('o2o_rel__integer'), Value(3))],
                    output_field=models.IntegerField()))

    def test_update_with_join_in_predicate_raise_field_error(self):
        with assertRaisesRegex(self, FieldError, 'Joined field references are not permitted in this query'):
            CaseTestModel.objects.update(
                string=self.create_expression(
                    'o2o_rel__integer',
                    [(Value(1), Value('one')),
                     (Value(2), Value('two')),
                     (Value(3), Value('three'))],
                    default=Value('other'),
                    output_field=models.CharField()))

    def test_update_big_integer(self):
        CaseTestModel.objects.update(
            big_integer=self.create_expression(
                'integer',
                [(Value(1), Value(1)),
                 (Value(2), Value(2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'big_integer'))

    def test_update_binary(self):
        CaseTestModel.objects.update(
            binary=self.create_expression(
                'integer',
                # fails on postgresql with python 2.7 if output_field is not
                # set explicitly
                [(Value(1), Value(b'one', output_field=models.BinaryField())),
                 (Value(2), Value(b'two', output_field=models.BinaryField()))],
                default=Value(b'', output_field=models.BinaryField())))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, b'one'), (2, b'two'), (3, b''), (2, b'two'), (3, b''), (3, b''), (4, b'')],
            transform=lambda o: (o.integer, binary_type(o.binary)))

    def test_update_boolean(self):
        CaseTestModel.objects.update(
            boolean=self.create_expression(
                'integer',
                [(Value(1), Value(True)),
                 (Value(2), Value(True))],
                default=Value(False)))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, True), (2, True), (3, False), (2, True), (3, False), (3, False), (4, False)],
            transform=attrgetter('integer', 'boolean'))

    def test_update_comma_separated_integer(self):
        CaseTestModel.objects.update(
            comma_separated_integer=self.create_expression(
                'integer',
                [(Value(1), Value('1')),
                 (Value(2), Value('2,2'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1'), (2, '2,2'), (3, ''), (2, '2,2'), (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'comma_separated_integer'))

    def test_update_date(self):
        CaseTestModel.objects.update(
            date=self.create_expression(
                'integer',
                [(Value(1), Value(date(2015, 1, 1))),
                 (Value(2), Value(date(2015, 1, 2)))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, date(2015, 1, 1)), (2, date(2015, 1, 2)), (3, None), (2, date(2015, 1, 2)), (3, None), (3, None),
             (4, None)],
            transform=attrgetter('integer', 'date'))

    def test_update_date_time(self):
        CaseTestModel.objects.update(
            date_time=self.create_expression(
                'integer',
                [(Value(1), Value(datetime(2015, 1, 1))),
                 (Value(2), Value(datetime(2015, 1, 2)))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, datetime(2015, 1, 1)), (2, datetime(2015, 1, 2)), (3, None), (2, datetime(2015, 1, 2)), (3, None),
             (3, None), (4, None)],
            transform=attrgetter('integer', 'date_time'))

    def test_update_decimal(self):
        CaseTestModel.objects.update(
            decimal=self.create_expression(
                'integer',
                [(Value(1), Value(Decimal(1))),
                 (Value(2), Value(Decimal(2)))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, Decimal(1)), (2, Decimal(2)), (3, None), (2, Decimal(2)), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'decimal'))

    def test_update_duration(self):
        CaseTestModel.objects.update(
            duration=self.create_expression(
                'integer',
                # fails on sqlite if output_field is not set explicitly on all
                # Values containing timedeltas
                [(Value(1), Value(timedelta(1), output_field=models.DurationField())),
                 (Value(2), Value(timedelta(2), output_field=models.DurationField()))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, timedelta(1)), (2, timedelta(2)), (3, None), (2, timedelta(2)), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'duration'))

    def test_update_email(self):
        CaseTestModel.objects.update(
            email=self.create_expression(
                'integer',
                [(Value(1), Value('1@example.com')),
                 (Value(2), Value('2@example.com'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1@example.com'), (2, '2@example.com'), (3, ''), (2, '2@example.com'), (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'email'))

    def test_update_file(self):
        CaseTestModel.objects.update(
            file=self.create_expression(
                'integer',
                [(Value(1), Value('~/1')),
                 (Value(2), Value('~/2'))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '~/1'), (2, '~/2'), (3, ''), (2, '~/2'), (3, ''), (3, ''), (4, '')],
            transform=lambda o: (o.integer, text_type(o.file)))

    def test_update_file_path(self):
        CaseTestModel.objects.update(
            file_path=self.create_expression(
                'integer',
                [(Value(1), Value('~/1')),
                 (Value(2), Value('~/2'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '~/1'), (2, '~/2'), (3, ''), (2, '~/2'), (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'file_path'))

    def test_update_float(self):
        CaseTestModel.objects.update(
            float=self.create_expression(
                'integer',
                [(Value(1), Value(1.1)),
                 (Value(2), Value(2.2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1.1), (2, 2.2), (3, None), (2, 2.2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'float'))

    def test_update_image(self):
        CaseTestModel.objects.update(
            image=self.create_expression(
                'integer',
                [(Value(1), Value('~/1')),
                 (Value(2), Value('~/2'))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '~/1'), (2, '~/2'), (3, ''), (2, '~/2'), (3, ''), (3, ''), (4, '')],
            transform=lambda o: (o.integer, text_type(o.image)))

    def test_update_ip_address(self):
        CaseTestModel.objects.update(
            ip_address=self.create_expression(
                'integer',
                # fails on postgresql if output_field is not set explicitly
                [(Value(1), Value('1.1.1.1')),
                 (Value(2), Value('2.2.2.2'))],
                output_field=models.IPAddressField()))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1.1.1.1'), (2, '2.2.2.2'), (3, None), (2, '2.2.2.2'), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'ip_address'))

    def test_update_generic_ip_address(self):
        CaseTestModel.objects.update(
            generic_ip_address=self.create_expression(
                'integer',
                # fails on postgresql if output_field is not set explicitly
                [(Value(1), Value('1.1.1.1')),
                 (Value(2), Value('2.2.2.2'))],
                output_field=models.GenericIPAddressField()))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1.1.1.1'), (2, '2.2.2.2'), (3, None), (2, '2.2.2.2'), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'generic_ip_address'))

    def test_update_null_boolean(self):
        CaseTestModel.objects.update(
            null_boolean=self.create_expression(
                'integer',
                [(Value(1), Value(True)),
                 (Value(2), Value(False))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, True), (2, False), (3, None), (2, False), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'null_boolean'))

    def test_update_positive_integer(self):
        CaseTestModel.objects.update(
            positive_integer=self.create_expression(
                'integer',
                [(Value(1), Value(1)),
                 (Value(2), Value(2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'positive_integer'))

    def test_update_positive_small_integer(self):
        CaseTestModel.objects.update(
            positive_small_integer=self.create_expression(
                'integer',
                [(Value(1), Value(1)),
                 (Value(2), Value(2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'positive_small_integer'))

    def test_update_slug(self):
        CaseTestModel.objects.update(
            slug=self.create_expression(
                'integer',
                [(Value(1), Value('1')),
                 (Value(2), Value('2'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1'), (2, '2'), (3, ''), (2, '2'), (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'slug'))

    def test_update_small_integer(self):
        CaseTestModel.objects.update(
            small_integer=self.create_expression(
                'integer',
                [(Value(1), Value(1)),
                 (Value(2), Value(2))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 1), (2, 2), (3, None), (2, 2), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'small_integer'))

    def test_update_string(self):
        CaseTestModel.objects.filter(string__in=['1', '2']).update(
            string=self.create_expression(
                'integer',
                [(Value(1), Value('1', output_field=models.CharField())),
                 (Value(2), Value('2', output_field=models.CharField()))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(string__in=['1', '2']).order_by('pk'),
            [(1, '1'), (2, '2'), (2, '2')],
            transform=attrgetter('integer', 'string'))

    def test_update_text(self):
        CaseTestModel.objects.update(
            text=self.create_expression(
                'integer',
                [(Value(1), Value('1')),
                 (Value(2), Value('2'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, '1'), (2, '2'), (3, ''), (2, '2'), (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'text'))

    def test_update_time(self):
        CaseTestModel.objects.update(
            time=self.create_expression(
                'integer',
                # fails on sqlite if output_field is not set explicitly on all
                # Values containing times
                [(Value(1), Value(time(1), output_field=models.TimeField())),
                 (Value(2), Value(time(2), output_field=models.TimeField()))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, time(1)), (2, time(2)), (3, None), (2, time(2)), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'time'))

    def test_update_url(self):
        CaseTestModel.objects.update(
            url=self.create_expression(
                'integer',
                [(Value(1), Value('http://1.example.com/')),
                 (Value(2), Value('http://2.example.com/'))],
                default=Value('')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 'http://1.example.com/'), (2, 'http://2.example.com/'), (3, ''), (2, 'http://2.example.com/'),
             (3, ''), (3, ''), (4, '')],
            transform=attrgetter('integer', 'url'))

    def test_update_uuid(self):
        CaseTestModel.objects.update(
            uuid=self.create_expression(
                'integer',
                # fails on sqlite if output_field is not set explicitly on all
                # Values containing UUIDs
                [(Value(1), Value(UUID('11111111111111111111111111111111'), output_field=models.UUIDField())),
                 (Value(2), Value(UUID('22222222222222222222222222222222'), output_field=models.UUIDField()))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, UUID('11111111111111111111111111111111')), (2, UUID('22222222222222222222222222222222')), (3, None),
             (2, UUID('22222222222222222222222222222222')), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'uuid'))

    def test_update_fk(self):
        obj1, obj2 = CaseTestModel.objects.all()[:2]

        CaseTestModel.objects.update(
            fk=self.create_expression(
                'integer',
                [(Value(1), Value(obj1.pk)),
                 (Value(2), Value(obj2.pk))]))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, obj1.pk), (2, obj2.pk), (3, None), (2, obj2.pk), (3, None), (3, None), (4, None)],
            transform=attrgetter('integer', 'fk_id'))


class SimpleCaseExpressionTests(BaseCaseExpressionTests):
    def create_expression(self, *args, **kwargs):
        return SimpleCase(*args, **kwargs)

    def test_expression_as_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(f_test=self.create_expression(
                F('integer') - 2,
                [(Value(-1), Value('negative one')),
                 (Value(0), Value('zero')),
                 (Value(1), Value('one'))],
                default=Value('other'),
                output_field=models.CharField())).order_by('pk'),
            [(1, 'negative one'), (2, 'zero'), (3, 'one'), (2, 'zero'), (3, 'one'), (3, 'one'), (4, 'other')],
            transform=attrgetter('integer', 'f_test'))

    def test_filter_with_expression_as_predicate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.filter(integer2=self.create_expression(
                F('integer') + 1,
                [(Value(3), Value(3)),
                 (Value(4), Value(4))],
                default=Value(1),
                output_field=models.IntegerField())).order_by('pk'),
            [(1, 1), (2, 3), (3, 4), (3, 4)],
            transform=attrgetter('integer', 'integer2'))


class SearchedCaseExpressionTests(BaseCaseExpressionTests):
    def create_expression(self, predicate, cases=None, *args, **kwargs):
        """Creates an equivalent SearchedCase expression."""
        return SearchedCase(
            [(Q(**{predicate: condition}), value) for condition, value in cases],
            *args, **kwargs)

    def test_lookup_in_condition(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(
                test=SearchedCase([(Q(integer__lt=2), Value('less than 2')),
                                   (Q(integer__gt=2), Value('greater than 2'))],
                                  default=Value('equal to 2'),
                                  output_field=models.CharField())).order_by('pk'),
            [(1, 'less than 2'), (2, 'equal to 2'), (3, 'greater than 2'), (2, 'equal to 2'), (3, 'greater than 2'),
             (3, 'greater than 2'), (4, 'greater than 2')],
            transform=attrgetter('integer', 'test'))


def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in (SimpleCaseExpressionTests, SearchedCaseExpressionTests):
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
