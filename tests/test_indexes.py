from __future__ import unicode_literals
from django.core.exceptions import ImproperlyConfigured

from django.test import TestCase
from django.utils.datastructures import SortedDict
from haystack import indexes
from haystack.exceptions import NotHandled
from mock import patch, MagicMock

from haystack_elasticsearch.fields import *
from haystack_elasticsearch.indexes import ClassIndex, UnifiedIndex


class Dummy(object):
    pass


class DummyIndex(indexes.SearchIndex, indexes.Indexable):
    text = CharField(document=True, use_template=True, analyzer='snowball', term_vector='with_positions_offsets')
    char_field = CharField(null=True)
    int_field = IntegerField(index_fieldname='int_field')
    bool_field = BooleanField(index_fieldname='bool_field', indexed=False, stored=False)
    int_field_2 = IntegerField(index_fieldname='int_field',
                               indexed=True, stored=True, null=True, use_template=True, faceted=True)
    bool_field_2 = BooleanField(index_fieldname='bool_field',
                                indexed=False, stored=False, null=False, use_template=False, faceted=False)
    multivalue_field = MultiValueField()
    multivalue_field_2 = MultiValueField(index_fieldname='multivalue_field')
    facet_field = FacetCharField(facet_for='char_field')
    facet_field_2 = FacetIntegerField()

    def prepare_char_field(self, obj):
        return 'char'

    def prepare_int_field(self, obj):
        return 1

    def prepare_bool_field(self, obj):
        return True

    def prepare_multivalue_field(self, obj):
        return list(range(10))

    def get_model(self):
        return Dummy


class ClassIndexTestCase(TestCase):
    def setUp(self):
        self.dummy = DummyIndex()
        self.index = ClassIndex(self.dummy)

    @patch('haystack_elasticsearch.indexes.settings')
    def test_init(self, settings):
        dummy = DummyIndex()
        settings.HAYSTACK_DOCUMENT_FIELD = 'test'

        index = ClassIndex(dummy)

        self.assertEqual(index.index, dummy)
        self.assertEqual(index.fields, SortedDict())
        self.assertFalse(index._built)
        self.assertEqual(index.document_field, 'test')
        self.assertEqual(index._fieldnames, {})
        self.assertEqual(index._facet_fieldnames, {})

    def test_reset(self):
        self.index.reset()

        self.assertEqual(self.index.fields, SortedDict())
        self.assertFalse(self.index._built)
        self.assertEqual(self.index._fieldnames, {})
        self.assertEqual(self.index._facet_fieldnames, {})

    @patch.object(ClassIndex, 'collect_fields', return_value=True)
    def test_build(self, class_index):
        self.index.build()

        self.assertTrue(self.index._built)

    def test_collect_fields(self):
        self.index.collect_fields()

    def test_get_model(self):
        model = self.index.get_model()

        self.assertEqual(model, Dummy)

    def test_get_index_fieldname_exists(self):
        field = self.index.get_index_fieldname('int_field')

        self.assertEqual(field, 'int_field')

    def test_get_index_fieldname_not_exists(self):
        field_not_exists = self.index.get_index_fieldname('not_exists')

        self.assertEqual(field_not_exists, 'not_exists')

    def test_get_facet_fieldname_faceted(self):
        facet_field = self.index.get_facet_fieldname('facet_field')

        self.assertEqual(facet_field, 'char_field')

    def test_get_facet_fieldname_not_faceted(self):
        facet_field_2 = self.index.get_facet_fieldname('facet_field_2')

        self.assertEqual(facet_field_2, 'facet_field_2')

    def test_get_facet_fieldname_normal_field(self):
        int_field_2 = self.index.get_facet_fieldname('int_field_2')

        self.assertEqual(int_field_2, 'int_field_2')

    def test_get_facet_fieldname_not_exists(self):
        field_not_exists = self.index.get_facet_fieldname('not_exists')

        self.assertEqual(field_not_exists, 'not_exists')

    def tearDown(self):
        pass


class UnifiedIndexTestCase(TestCase):
    def setUp(self):
        self.index = UnifiedIndex()

    def test_init(self):
        index = UnifiedIndex()

        self.assertEqual(index.indexes, {})
        self.assertFalse(index._built)
        self.assertEqual(index.excluded_indexes, [])
        self.assertEqual(index.excluded_indexes_ids, {})

    def test_reset(self):
        self.index.reset()

        self.assertEqual(self.index.indexes, {})
        self.assertFalse(self.index._built)
        self.assertEqual(self.index.excluded_indexes, [])
        self.assertEqual(self.index.excluded_indexes_ids, {})

    @patch('haystack_elasticsearch.indexes.inspect')
    @patch('haystack_elasticsearch.indexes.settings')
    def test_collect_indexes(self, settings, inspect):
        settings.INSTALLED_APPS = ('test', )
        item = MagicMock()
        item.haystack_use_for_indexing = True
        item.get_model.return_value = Dummy
        inspect.getmembers.return_value = [('Dummy', item)]

        self.index.collect_indexes()

    @patch('haystack_elasticsearch.indexes.inspect')
    @patch('haystack_elasticsearch.indexes.settings')
    def test_collect_indexes_excluded_index(self, settings, inspect):
        settings.INSTALLED_APPS = ('test', )
        item = MagicMock()
        item.haystack_use_for_indexing = True
        item.get_model.return_value = Dummy
        inspect.getmembers.return_value = [('Dummy', item)]

        self.index.excluded_indexes = 'test.search_indexes.Dummy'
        self.index.collect_indexes()

    @patch('haystack_elasticsearch.indexes.inspect')
    @patch('haystack_elasticsearch.indexes.settings')
    def test_collect_indexes_excluded_index_id(self, settings, inspect):
        settings.INSTALLED_APPS = ('test', )
        item = MagicMock()
        item.haystack_use_for_indexing = True
        item.get_model.return_value = Dummy
        inspect.getmembers.return_value = [('Dummy', item)]

        self.index.excluded_indexes_ids['Dummy'] = id(item)
        self.index.collect_indexes()

    @patch('haystack_elasticsearch.indexes.utils')
    @patch('haystack_elasticsearch.indexes.settings')
    def test_collect_indexes_import_error(self, settings, utils):
        settings.INSTALLED_APPS = ('test', )

        utils.import_search_indexes.side_effect = ImportError

        self.assertRaises(ImportError, self.index.collect_indexes)

    def test_build(self):
        indexes_ = [ClassIndex(DummyIndex())]
        self.index.build(indexes_)

        self.assertTrue(self.index._built)

    def test_build_collect_indexes(self):
        indexes_ = [ClassIndex(DummyIndex())]
        with patch.object(UnifiedIndex, 'collect_indexes', return_value=indexes_):
            self.index.build()

        self.assertTrue(self.index._built)

    def test_build_repeated_index(self):
        indexes_ = [ClassIndex(DummyIndex()), ClassIndex(DummyIndex())]
        with patch.object(UnifiedIndex, 'collect_indexes', return_value=indexes_):
            self.assertRaises(ImproperlyConfigured, self.index.build)

        self.assertFalse(self.index._built)

    @patch.object(UnifiedIndex, 'build')
    def test_get_indexed_models(self, unified_index):
        class_index = ClassIndex(DummyIndex)
        self.index.indexes[Dummy] = class_index

        indexed_models = self.index.get_indexed_models()

        self.assertEqual(indexed_models, [Dummy])

    @patch.object(UnifiedIndex, 'build')
    def test_get_index(self, unified_index):
        class_index = ClassIndex(DummyIndex)
        self.index.indexes[Dummy] = class_index

        index_returned = self.index.get_index(Dummy)

        self.assertEqual(index_returned, DummyIndex)

    @patch.object(UnifiedIndex, 'build')
    def test_get_index_model_not_exists(self, unified_index):
        self.assertRaises(NotHandled, self.index.get_index, Dummy)

    @patch.object(UnifiedIndex, 'build')
    def test_all_searchfields(self, unified_index):
        fields = {'foo': None, 'bar': None}
        class_index = MagicMock()
        class_index.index = DummyIndex
        class_index.fields = fields
        self.index.indexes[Dummy] = class_index

        search_fields = self.index.all_searchfields()
        returned_index = search_fields[DummyIndex]

        self.assertIn(DummyIndex, search_fields.keys())
        self.assertDictEqual(returned_index, fields)

    def tearDown(self):
        pass