Pg-django
=========

Pg-django is a fork of `Django <https://www.djangoproject.com/>`_ that uses
`PostgreSQL <http://www.postgresql.org/>`_ as database backend.

It does not break Django's database agnosticism, but it provides postgres-only
new features. As such it aims to be a drop-in replacement of Django, and
a particular attention is ported at not breaking django features and APIs.

Features
========

All Django features and APIs. Pg-django is based on Django 1.4 code and it must
respect django features. To ensure this, we run django tests on our code every
days (or such), and no official release is possible if the tests fail.

This is not enough! But we can't do more... Maybe some bugs may appear at 
usage. In this case please send bug reports to pg-django, not Django!


Yet implemented
---------------

    - Add database CASCADE deletes
      
    - Add a new inheritance type introducing materialized views. This allow
      a more pythonic usage, and remove expensive DB JOINs. This also creates
      a new era, where a QuerySet can yield different Models instances

Not fully implemented
---------------------

    - New fields using database ARRAY type. For now only CharArrayFields and
      TextArrayFields are functional. It also introduce new filters lookups
      myfield__has, myfield__has_one and myfield__has_all. It is based on
      `Ecometrica's code <https://github.com/ecometrica/django-dbarray>`_
      for the fields code

More to come
------------

    - Add database DEFAULTs when possible (i.e. the django field's default is
      not a complex, user-defined python method)

    - Add a new type of models based on database views. The underling view
      may be defined using QuerySet.filter() like syntax or plain SQL for
      complex view definitions

    - Include a built-in full-text indexing facility, probably based on
      `SeSQL <https://bitbucket.org/liberation/sesql/overview>`_

    - ... any of your proposals that we approve.

License
=======

Our changes on Django 1.4 base code are distributed under the terms of
GNU-AGPL. However, we added a clause to allow Django project team to backport
our code modifications under the term of Django's original BSD license.

Web site
========

The project is self-hosted at `<http://trac.lisael.org/pg-django>`_

Note that the project's Github page is not the reference. As such, the github
repo mirrors the only official repo: `<git://git.lisael.org/git/pg-django>`_

Contributions
=============

Contributions are accepted if posted as a patch on a relevant ticket on `our
trac <http://trac.lisael.org/pg-django>`_. If your improvement idea fits no
existing ticket, please submit a new ticket before you start implementing it,
so we can discuss about it.

Other related projects
======================

By "related" I mean that these projects give similar features and that may be
sources of inspiration for Pg-django code. We may or may not have other types
of relation, this is not the point here.

    - `<http://readthedocs.org/docs/django-orm/en/latest/index.html>`_

Who are we?
===========

We? wadaya mean "we"? We is a single unemployed python/django web developer who
found an exciting way to spend his time, to explore and to torture Django
internals.

Misc and random thought
=======================

Most of pg-django features are actually portable to most django database backends
(however, materialized views inheritance, our killer feature if you ask me,
relies on PostgreSQL RULES, and I have no idea how to port this implementation
detail...). Though we don't have time and motivation to port ourselves, we accept
any contribution in this direction. If a feature becomes fully DB agnostic, it
is a good candidate to be backported in Django, and that's good for us.

We do aim to merge most of our code in Django tree, when possible. That's why
we need to keep full backward compatibility with django, and we must test our
code as much as humanly possible.

