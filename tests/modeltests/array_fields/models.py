from django.db import models

TEST_SKIP_UNLESS_DB_FEATURES = ['support_arrays']

class Item(models.Model):
    title = models.CharField(max_length=50)

    class Meta:
        abstract = True

    def __unicode__(self):
        return '%s %s' % (self.__class__.__name__, self.title)


class TaggedItem(Item):
    # Arrayfields accept and require the same attributes as the underlying
    # Field type (here, model.CharField)
    # It also accept an attribute `itertype` telling the python type of the
    # field (default is list)
    tags = models.CharArrayField(max_length=10, itertype=set)


class CommentedItem(Item):
    comments = models.TextArrayField()


class RatedItem(Item):
    rates = models.IntegerArrayField()


class MiscArraysItem(Item):
    """Just to test arrays creation. No other test is done at the moment on
    these fields. Write deeper tests for those and remove them from here"""

    boolean = models.BooleanArrayField()
    #date = models.DateArrayField()
    #date_time = models.DateTimeArrayField()
    #decimal = models.DecimalArrayField(max_digits=5, decimal_places=2)
    email = models.EmailArrayField()
    filepath = models.FilePathArrayField()
    floatt = models.FloatArrayField()
    big_integer = models.BigIntegerArrayField()
    #ip_address = models.IPAddressArrayField()
    #generic_ip_address = models.GenericIPAddressArrayField()
    null_boolean = models.NullBooleanArrayField()
    #positive_integer = models.PositiveIntegerArrayField()
    #positive_small_integer = models.PositiveSmallIntegerArrayField()
    slug = models.SlugArrayField()
    small_integer = models.SmallIntegerArrayField()
    #time = models.TimeArrayField()
    url = models.URLArrayField()


