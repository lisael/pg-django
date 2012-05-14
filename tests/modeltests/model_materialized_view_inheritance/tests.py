from __future__ import absolute_import

from operator import attrgetter

from django.core.exceptions import FieldError
from django.test import TestCase, skipIfDBFeature, skipUnlessDBFeature

from .models import (DocumentBase, TaggedDocument, FileDocument, TextDocument,
                     Comment, RatedTextDocument, News)

from datetime import datetime

class ModelMaterializedViewInheritanceTests(TestCase):
    @skipUnlessDBFeature('support_materialized_view_base')
    def test_single_level_inheritance(self):
        c1 = Comment(title='a comment',
                     summary='a comment summary',
                     text='this is a comment',
                     commenter='me')
        n1 = News(title='a news',
                  summary='a news summary',
                  date=datetime.now())
        n1.save()
        c1.save()




