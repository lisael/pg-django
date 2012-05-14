from django.db import models
from django.db.models.fields import CharArrayField


class DocumentBase(models.Model):
    """A materialized base class. In database, this creates a view
    and a materialized view table containing all fields of all leaf
    children.

    This model is not instantiable. If one tries to instantiate it, a
    leaf model is returned (anyway it is possible to instanciate it,
    but only for internal use. The user shouldn't try to do it and this
    possibility is neither documented -RTFC- nor guaranteed in the future)

    It cannot be saved nor deleted directly. Only the leaves are.
    """
    title = models.CharField(max_length=200, db_index = True)
    summary = models.CharField(max_length=1000)

    class Meta:
        ## Tell pg_django to create a materialized view for this base class
        materialized_view = True

    def __unicode__(self):
        return u'%s %s' % (self.__class__.__name__, self.title)


class TaggedDocument(DocumentBase):
    """An intermediate view ( a non-concrete branch in the inheritance tree).
    This doesn't create a db table, but a view instead, based on DocumentBase
    materialized view.

    Instantiation works as for materialized views
    """
    tags = CharArrayField(max_length=60, db_index=True)

    class Meta:
        # Tell django this is an abstract intermediate view
        concrete = False



class FileDocument(TaggedDocument):
    """A leaf Model (it is a leaf in the materialized view based inheritance
    tree). Nothing to say, works just like a plain old single table django
    model
    """
    path = models.CharField(max_length=1000)


class TextDocument(TaggedDocument):
    """A concrete intermediate view ( a concrete branch in the inheritance
    tree). It behaves like an intermediate view and a leaf. In database, a
    view and a table are created.

    The view contains all objects plus its children models objects. The table
    stores only its objects

    It can be instanciated, but querying this model may return any of its
    children

    It can be saved and deleted.
    """
    content = models.TextField()

    class Meta:
        # Tell django it's a concrete intermediate view (this is the default.)
        concrete = True

class RatedTextDocument(TextDocument):
    """Another leaf. This one has a non-concrete and a concrete intermediate
    view"""
    rate = models.IntegerField()


class Comment(DocumentBase):
    """Yet another leaf. This one is a direct leaf (no intermediate view)"""
    text = models.CharField(max_length=1000)
    commenter = models.CharField(max_length=60)


class News(DocumentBase):
    """Yet another leaf. This one is a direct leaf (no intermediate view)"""
    date = models.DateTimeField()


