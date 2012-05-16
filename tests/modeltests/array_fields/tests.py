from __future__ import absolute_import

from operator import attrgetter

from django.core.exceptions import FieldError
from django.test import TestCase

from .models import (Item, TaggedItem, CommentedItem, RatedItem,
                    MiscArraysItem)

import datetime
from time import time
import decimal

class ArrayFieldTestCase(TestCase):

    def test_CharArrayField(self):
        """test the CharArrayField and its lookups"""
        # test validation
        #ti = TaggedItem(title='title',
                        #tags=['loooooooonnnggggtag'])
        #with self.assertRaisesMessage('value too long for type character varying(10)'):
            #ti.save()


        # tests the python type of the field
        ti = TaggedItem(title='title',
                        tags=['tag'])
        self.assertIsInstance(ti.tags,set)

        # test if the type is also created when querying
        ti.save()
        pk = ti.pk
        ti = TaggedItem.objects.get(pk=pk)
        self.assertIsInstance(ti.tags,set)

        ## test lookups...
        for i in range(1,11):
            title = 'title%s' % i
            other_tag = i%2 and 'odd' or 'even'
            ti = TaggedItem(title=title,
                           tags=['tag%s' % i, other_tag])
            ti.save()

        # test 'has lookup'
        sq = TaggedItem.objects.filter(tags__has='tag1')
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title1')

        # test 'has_one lookup'
        sq = TaggedItem.objects.filter(tags__has_one=['even','tag1'])
        self.assertEqual(len(sq),6)
        found = set([ti.title for ti in sq])
        expected = set(['title1','title2','title4','title6','title8','title10'])
        self.assertEqual(len(found.difference(expected)),0)

        # test 'has_all lookup'
        sq = TaggedItem.objects.filter(tags__has_all=['even','tag1'])
        self.assertEqual(len(sq),0)
        sq = TaggedItem.objects.filter(tags__has_all=['even','tag2'])
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title2')

    def test_TextArrayField(self):
        """test TextArrayField and its lookups"""
        # tests the python type of the field. list is the default
        ci = CommentedItem(title='title',
                        comments=['comment'])
        self.assertIsInstance(ci.comments,list)

        # test if the type is also created when querying
        ci.save()
        pk = ci.pk
        ci = CommentedItem.objects.get(pk=pk)
        self.assertIsInstance(ci.comments,list)


        ## test lookups...
        for i in range(1,11):
            title = 'title%s' % i
            other_comment = i%2 and 'odd' or 'even'
            ci = CommentedItem(title=title,
                           comments=['comment%s' % i, other_comment])
            ci.save()

        # test 'has lookup'
        sq = CommentedItem.objects.filter(comments__has='comment1')
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title1')

        # test 'has_one lookup'
        sq = CommentedItem.objects.filter(comments__has_one=['even','comment1'])
        self.assertEqual(len(sq),6)
        found = set([ti.title for ti in sq])
        expected = set(['title1','title2','title4','title6','title8','title10'])
        self.assertEqual(len(found.difference(expected)),0)

        # test 'has_all lookup'
        sq = CommentedItem.objects.filter(comments__has_all=['even','comment1'])
        self.assertEqual(len(sq),0)
        sq = CommentedItem.objects.filter(comments__has_all=['even','comment2'])
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title2')

    def test_IntegerArrayField(self):
        """test IntegerArrayField and its lookups"""

        ## test lookups...
        for i in range(1,10):
            title = 'title%s' % i
            other_rate = i%2 and 1 or 2
            ti = RatedItem(title=title,
                           rates=[10+i, other_rate])
            ti.save()

        # test 'has' lookup
        sq = RatedItem.objects.filter(rates__has=11)
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title1')

        # test 'has_one' lookup
        sq = RatedItem.objects.filter(rates__has_one=[2,11])
        self.assertEqual(len(sq),5)
        found = set([ti.title for ti in sq])
        expected = set(['title1','title2','title4','title6','title8','title10'])
        self.assertEqual(len(found.difference(expected)),0)

        # test 'has_all' lookup
        sq = RatedItem.objects.filter(rates__has_all=[2,11])
        self.assertEqual(len(sq),0)
        sq = RatedItem.objects.filter(rates__has_all=[2,12])
        self.assertEqual(len(sq),1)
        self.assertEqual(sq[0].title,'title2')

    def test_misc_arrays(self):
        """Just instanciate, save, and retrieve a MiscArraysItem. Each
        of these fields should be full tested"""
        fields = dict(
            boolean=[True, False],
            #date=[datetime.date.today()],
            #date_time=[datetime.datetime.now()],
            #decimal=[decimal.Decimal(42.42)],
            email=['aa@bb.cc'],
            filepath=['/a/path'],
            floatt=[42.42],
            big_integer=[4242424242424242424],
            #ip_address=['42.42.42.42'],
            #generic_ip_address=['2001:0db8:85a3:0000:0000:8a2e:0370:7334'],
            null_boolean=[None,True,False],
            #positive_integer=[4242424242],
            #positive_small_integer=[42],
            slug=['a_slug'],
            small_integer=[-42],
            #time=[datetime.time()],
            url=['http://example.org'],
        )

        # Test empty fields
        mai = MiscArraysItem()
        mai.save()
        id_ = mai.pk
        mai = MiscArraysItem.objects.get(pk=id_)
        for fn in fields:
            attr = getattr(mai,fn)
            self.assertEqual(len(getattr(mai,fn)), 0)

        # Test with values
        mai = MiscArraysItem(**fields)
        mai.save()
        id_ = mai.pk
        mai = MiscArraysItem.objects.get(pk=id_)
        for fn in fields:
            self.assertEqual(getattr(mai,fn), fields[fn])

