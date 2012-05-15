"""
Tests for materialized base ineritance type.

In this type of inheritance, a model can be a root materialized view
(one per inheritance tree), a leaf (behaves just like a single table
model), an non-concrete branch (just a db view of its leaves) or a
concrete branch (a db view of its leaves + a concrete table)

When the root or a branch is queried, the resulting queryset is said
'mixed' i.e. it may yield any of its leaves.
"""
# TODO PG: - test foreign keys on base, branch and leaves,
#          - test proxy models on top of leaves (should work)
#            and on top of concrete branches(should not work yet)
#          - test multi-table inheritance on top of leaves and
#            concrete branches (none should work yet)
#          - test random insert of abstract models in the tree
#          - test a bunch of complex queries


from __future__ import absolute_import

from django.core.exceptions import FieldError, NonPersistantModel
from django.test import TestCase, skipUnlessDBFeature

from datetime import datetime

class ModelMaterializedViewInheritanceTests(TestCase):
    @skipUnlessDBFeature('support_materialized_view_base')
    def test_single_level_inheritance(self):
        # Imports must be skipped either
        from .models import DocumentBase, Comment, News
        c1 = Comment(title='a comment',
                     summary='a comment summary',
                     text='this is a comment',
                     commenter='me')
        n1 = News(title='a piece of news',
                  summary='a news summary',
                  date=datetime.now())
        n1.save()
        c1.save()
        dbs = [unicode(db) for db in DocumentBase.objects.all()]
        self.assertEqual(len(dbs),2)
        self.assertIn("Comment a comment", dbs)
        self.assertIn("News a piece of news",dbs)
        with self.assertRaises(NonPersistantModel):
            db = DocumentBase(title='a base',summary='a base summary')
            db.save()

    @skipUnlessDBFeature('support_materialized_view_base')
    def test_abstract_intemediate_views(self):
        from .models import (DocumentBase, TaggedDocument, FileDocument,
                         Comment, OtherTaggedStuff)
        fd1 = FileDocument(
            title="a file",
            summary="this is a file",
            path="/a/random/path",
            tags=['foo']
        )
        fd1.save()
        ots = OtherTaggedStuff(
            title="some stuff",
            summary='this is some stuff',
            spam='eggs',
            tags=['foo']
        )
        ots.save()
        c1 = Comment(title='a comment',
                     summary='a comment summary',
                     text='this is a comment',
                     commenter='me')
        c1.save()
        dbs = [unicode(db) for db in DocumentBase.objects.all()]
        self.assertEqual(len(dbs),3)
        self.assertIn("Comment a comment", dbs)
        self.assertIn("FileDocument a file",dbs)
        self.assertIn("OtherTaggedStuff some stuff",dbs)
        dbs = [unicode(db) for db in TaggedDocument.objects.all()]
        self.assertEqual(len(dbs),2)
        self.assertIn("FileDocument a file",dbs)
        self.assertIn("OtherTaggedStuff some stuff",dbs)
        with self.assertRaises(NonPersistantModel):
            db = TaggedDocument(title='a base',summary='a base summary',
                                tags=['foo','bar'])
            db.save()

    @skipUnlessDBFeature('support_materialized_view_base')
    def test_concrete_intemediate_views(self):
        from .models import (DocumentBase, TaggedDocument, FileDocument,
                Comment, OtherTaggedStuff, TextDocument, RatedTextDocument)
        # it is a branch, but it may also be saved...
        # that's why there is no assertRaise i this test
        td1 = TextDocument(
            title="a text",
            summary="this is a text",
            content="a random text written by my own personal inner monkey army. not too bad...",
            tags=['foo']
        )
        td1.save()
        rtd = RatedTextDocument(
            title="rate me",
            summary="rate me, my friend",
            content="rate me, rate me-e again, I'm not th only one, a-a-a-an ...",
            rate=42,
            tags=['foo']
        )
        rtd.save()
        c1 = Comment(title='a comment',
                     summary='a comment summary',
                     text='this is a comment',
                     commenter='me')
        c1.save()
        dbs = [unicode(db) for db in DocumentBase.objects.all()]
        self.assertEqual(len(dbs), 3)
        self.assertIn("Comment a comment", dbs)
        self.assertIn("TextDocument a text", dbs)
        self.assertIn("RatedTextDocument rate me", dbs)
        dbs = [unicode(db) for db in TextDocument.objects.all()]
        self.assertEqual(len(dbs),2)
        self.assertIn("TextDocument a text", dbs)
        self.assertIn("RatedTextDocument rate me", dbs)





