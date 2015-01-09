from __future__ import unicode_literals

from datetime import datetime
from operator import attrgetter

from django.db.models import F, Value
from django.test import TestCase

from .models import Article, Author


class OrderingTests(TestCase):
    def setUp(self):
        self.a1 = Article.objects.create(
            headline="Article 1", pub_date=datetime(2005, 7, 26)
        )
        self.a2 = Article.objects.create(
            headline="Article 2", pub_date=datetime(2005, 7, 27)
        )
        self.a3 = Article.objects.create(
            headline="Article 3", pub_date=datetime(2005, 7, 27)
        )
        self.a4 = Article.objects.create(
            headline="Article 4", pub_date=datetime(2005, 7, 28)
        )

    def test_default_ordering(self):
        """
        By default, Article.objects.all() orders by pub_date descending, then
        headline ascending.
        """
        self.assertQuerysetEqual(
            Article.objects.all(), [
                "Article 4",
                "Article 2",
                "Article 3",
                "Article 1",
            ],
            attrgetter("headline")
        )

        # Getting a single item should work too:
        self.assertEqual(Article.objects.all()[0], self.a4)

    def test_default_ordering_override(self):
        """
        Override ordering with order_by, which is in the same format as the
        ordering attribute in models.
        """
        self.assertQuerysetEqual(
            Article.objects.order_by("headline"), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )
        self.assertQuerysetEqual(
            Article.objects.order_by("pub_date", "-headline"), [
                "Article 1",
                "Article 3",
                "Article 2",
                "Article 4",
            ],
            attrgetter("headline")
        )

    def test_order_by_override(self):
        """
        Only the last order_by has any effect (since they each override any
        previous ordering).
        """
        self.assertQuerysetEqual(
            Article.objects.order_by("id"), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )
        self.assertQuerysetEqual(
            Article.objects.order_by("id").order_by("-headline"), [
                "Article 4",
                "Article 3",
                "Article 2",
                "Article 1",
            ],
            attrgetter("headline")
        )

    def test_stop_slicing(self):
        """
        Use the 'stop' part of slicing notation to limit the results.
        """
        self.assertQuerysetEqual(
            Article.objects.order_by("headline")[:2], [
                "Article 1",
                "Article 2",
            ],
            attrgetter("headline")
        )

    def test_stop_start_slicing(self):
        """
        Use the 'stop' and 'start' parts of slicing notation to offset the
        result list.
        """
        self.assertQuerysetEqual(
            Article.objects.order_by("headline")[1:3], [
                "Article 2",
                "Article 3",
            ],
            attrgetter("headline")
        )

    def test_random_ordering(self):
        """
        Use '?' to order randomly.
        """
        self.assertEqual(
            len(list(Article.objects.order_by("?"))), 4
        )

    def test_reversed_ordering(self):
        """
        Ordering can be reversed using the reverse() method on a queryset.
        This allows you to extract things like "the last two items" (reverse
        and then take the first two).
        """
        self.assertQuerysetEqual(
            Article.objects.all().reverse()[:2], [
                "Article 1",
                "Article 3",
            ],
            attrgetter("headline")
        )

    def test_extra_ordering(self):
        """
        Ordering can be based on fields included from an 'extra' clause
        """
        self.assertQuerysetEqual(
            Article.objects.extra(select={"foo": "pub_date"}, order_by=["foo", "headline"]), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )

    def test_extra_ordering_quoting(self):
        """
        If the extra clause uses an SQL keyword for a name, it will be
        protected by quoting.
        """
        self.assertQuerysetEqual(
            Article.objects.extra(select={"order": "pub_date"}, order_by=["order", "headline"]), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )

    def test_order_by_pk(self):
        """
        Ensure that 'pk' works as an ordering option in Meta.
        Refs #8291.
        """
        Author.objects.create(pk=1)
        Author.objects.create(pk=2)
        Author.objects.create(pk=3)
        Author.objects.create(pk=4)

        self.assertQuerysetEqual(
            Author.objects.all(), [
                4, 3, 2, 1
            ],
            attrgetter("pk")
        )

    def test_order_by_fk_attname(self):
        """
        Ensure that ordering by a foreign key by its attribute name prevents
        the query from inheriting it's related model ordering option.
        Refs #19195.
        """
        for i in range(1, 5):
            author = Author.objects.create(pk=i)
            article = getattr(self, "a%d" % (5 - i))
            article.author = author
            article.save(update_fields={'author'})

        self.assertQuerysetEqual(
            Article.objects.order_by('author_id'), [
                "Article 4",
                "Article 3",
                "Article 2",
                "Article 1",
            ],
            attrgetter("headline")
        )

    def test_order_by_f_expression(self):
        self.assertQuerysetEqual(
            Article.objects.order_by(F('headline')), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )

        self.assertQuerysetEqual(
            Article.objects.order_by(F('headline').asc()), [
                "Article 1",
                "Article 2",
                "Article 3",
                "Article 4",
            ],
            attrgetter("headline")
        )

        self.assertQuerysetEqual(
            Article.objects.order_by(F('headline').desc()), [
                "Article 4",
                "Article 3",
                "Article 2",
                "Article 1",
            ],
            attrgetter("headline")
        )
