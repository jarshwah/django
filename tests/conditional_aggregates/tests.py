from __future__ import unicode_literals

from django.db.models import (
    Avg, Sum, Count, Max, Min,
    Aggregate, F, ValueNode, Q,
    IntegerField, FloatField, DecimalField)
from django.db import connection
from django.db.models.functions import If
from django.test import TestCase
from django.test.utils import Approximate, CaptureQueriesContext

from .models import Author, Publisher, Book, Store


class BaseAggregateTestCase(TestCase):
    fixtures = ["aggregation.json"]

    def test_conditional_aggregates(self):
        age_distribution = Author.objects.values('age').annotate(
            over_30=Count(If(Q(age__gte=30))),
            under_30=Count(If(Q(age__lt=30)))
            ).aggregate(Sum('over_30'), Sum('under_30'))

        under_30 = Author.objects.filter(age__lt=30).count()
        over_30 = Author.objects.filter(age__gte=30).count()

        self.assertEqual(
            under_30,
            age_distribution['under_30__sum'])

        self.assertEqual(
            over_30,
            age_distribution['over_30__sum'])

    def test_conditional_aggregates_negated(self):
        age_distribution = Author.objects.values('age').annotate(
            under_30=Count(If(~Q(age__gte=30))),
            over_30=Count(If(~Q(age__lt=30)))
            ).aggregate(Sum('over_30'), Sum('under_30'))

        over_30 = Author.objects.filter(~Q(age__lt=30)).count()
        under_30 = Author.objects.filter(~Q(age__gte=30)).count()

        self.assertEqual(
            under_30,
            age_distribution['under_30__sum'])

        self.assertEqual(
            over_30,
            age_distribution['over_30__sum'])

    def test_f_expressions(self):
        age_distribution_sum = Author.objects.values('age').annotate(
            over_30=Sum(If(Q(age__gte=30), F('age'), 0), output_type=IntegerField()),
            under_30=Sum(If(Q(age__lt=30), F('age'), 0), output_type=IntegerField())
            ).aggregate(Sum('over_30'), Sum('under_30'))

        under_30_sum = Author.objects.filter(age__lt=30).aggregate(Sum('age'))
        over_30_sum = Author.objects.filter(age__gte=30).aggregate(Sum('age'))

        self.assertEqual(
            under_30_sum['age__sum'],
            age_distribution_sum['under_30__sum'])

        self.assertEqual(
            over_30_sum['age__sum'],
            age_distribution_sum['over_30__sum'])

    def test_m2m_conditions(self):
        m2m_distribution_max = Book.objects.annotate(
            over_30=Sum(
                If(Q(authors__age__gte=30), F('authors__age'), None),
                output_type=IntegerField()),
            under_30=Sum(
                If(Q(authors__age__lt=30), F('authors__age'), None),
                output_type=IntegerField())
            ).aggregate(Max('over_30'), Max('under_30'))

        under_30_max = Book.objects.filter(authors__age__lt=30).annotate(
            under_30=Sum('authors__age')).aggregate(max_age=Max('under_30'))

        over_30_max = Book.objects.filter(authors__age__gte=30).annotate(
            over_30=Sum('authors__age')).aggregate(max_age=Max('over_30'))

        self.assertEqual(
            m2m_distribution_max['under_30__max'],
            under_30_max['max_age'])

        self.assertEqual(
            m2m_distribution_max['over_30__max'],
            over_30_max['max_age'])

    def test_m2m_conditions_negated(self):
        m2m_distribution_max = Book.objects.annotate(
            under_30=Sum(
                If(~Q(authors__age__gte=30), F('authors__age'), None),
                output_type=IntegerField()),
            over_30=Sum(
                If(~Q(authors__age__lt=30), F('authors__age'), None),
                output_type=IntegerField())
            ).aggregate(Max('over_30'), Max('under_30'))

        over_30_max = Book.objects.filter(~Q(authors__age__lt=30)).annotate(
            under_30=Sum('authors__age')).aggregate(max_age=Max('under_30'))

        under_30_max = Book.objects.filter(~Q(authors__age__gte=30)).annotate(
            over_30=Sum('authors__age')).aggregate(max_age=Max('over_30'))

        self.assertEqual(
            m2m_distribution_max['under_30__max'],
            under_30_max['max_age'])

        self.assertEqual(
            m2m_distribution_max['over_30__max'],
            over_30_max['max_age'])

    def test_add_implementation(self):
        try:
            # Sum behaves like Count now
            def always_has_1_or_null(self, qn, connection):
                sql, params = qn.compile(self.where)
                self.extra['condition'] = sql
                self.extra['true_value'] = self.extra['false_value'] = '%s'
                params.extend([1, None])
                return self.sql_template % self.extra, params
            setattr(If, 'as_' + connection.vendor, always_has_1_or_null)

            age_distribution_sum = Author.objects.values('age').annotate(
                over_30=Sum(If(Q(age__gte=30), F('age'), 0), output_type=IntegerField()),
                under_30=Sum(If(Q(age__lt=30), F('age'), 0), output_type=IntegerField())
                ).aggregate(Sum('over_30'), Sum('under_30'))

            under_30_count = Author.objects.filter(age__lt=30).aggregate(Count('age'))
            over_30_count = Author.objects.filter(age__gte=30).aggregate(Count('age'))

            self.assertEqual(
                under_30_count['age__count'],
                age_distribution_sum['under_30__sum'])

            self.assertEqual(
                over_30_count['age__count'],
                age_distribution_sum['over_30__sum'])

            delattr(If, 'as_' + connection.vendor)

            # lower case sql is aesthetically pleasing
            def lower_case_template_super(self, qn, connection):
                self.extra['sql_template'] = self.extra['sql_template'].lower()
                return self.as_sql(qn, connection)
            setattr(If, 'as_' + connection.vendor, lower_case_template_super)

            qs = Author.objects.values('age').annotate(
                over_30=Sum(If(Q(age__gte=30), F('age'), 0), output_type=IntegerField()),
                under_30=Sum(If(Q(age__lt=30), F('age'), 0), output_type=IntegerField())
                )

            self.assertEqual(str(qs.query).count('case when'), 2)

        finally:
            delattr(If, 'as_' + connection.vendor)
