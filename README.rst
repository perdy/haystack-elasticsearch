======================
Haystack-ElasticSearch
======================

:Version: 0.1.0
:Status: beta
:Author: José Antonio Perdiguero López

Haystack-ElasticSearch is a Django application that adds some specific features from ElasticSearch to django-haystack such as a mapping based on index/type hierarchy, each Django class is represented by a ElasticSearch type.

Quick start
===========

#. Install this package using pip::

    pip install haystack-elasticsearch


#. Add *haystack_elasticsearch* to your **INSTALLED_APPS** settings like this::

    INSTALLED_APPS = (
        ...
        'haystack_elasticsearch',
    )

#. Change your engine in haystack settings to *haystack_elasticsearch.backends.ElasticsearchSearchBackend*.
#. Replace *haystack fields* for *haystack_elasticsearch fields* in your indexes.
