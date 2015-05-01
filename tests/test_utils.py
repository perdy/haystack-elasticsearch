from __future__ import unicode_literals
from functools import partial

from django.test import TestCase
from mock import patch, MagicMock

from haystack_elasticsearch import utils


def side_effect_list(returns, *args):
    result = returns.pop(0)
    if isinstance(result, Exception):
        raise result
    return result


class CheckAnalyzersTestCase(TestCase):
    def setUp(self):
        pass

    def test_indexes_different_analyzers(self):
        unified_index = MagicMock()
        index = MagicMock()
        foo_field = MagicMock()
        foo_field.analyzer = 'foo_analyzer'
        bar_field = MagicMock()
        bar_field.analyzer = 'bar_analyzer'
        index.fields = {
            'foo': foo_field,
            'bar': bar_field,
        }
        unified_index.collect_indexes.return_value = [index]

        utils.check_analyzers(unified_index)

    def tearDown(self):
        pass


class ImportSearchIndexesTestCase(TestCase):
    def setUp(self):
        pass

    @patch('haystack_elasticsearch.utils.warnings')
    @patch('haystack_elasticsearch.utils.importlib')
    def test_import_module_failed(self, importlib, warnings):
        importlib.import_module.side_effect = ImportError

        search_indexes_module = utils.import_search_indexes('Test')

        self.assertIsNone(search_indexes_module)

    @patch('haystack_elasticsearch.utils.module_has_submodule', return_value=True)
    @patch('haystack_elasticsearch.utils.warnings')
    @patch('haystack_elasticsearch.utils.importlib')
    def test_import_module_failed_search_indexes(self, importlib, warnings, module_has_submodule):
        returns = ['Test', ImportError()]
        importlib.import_module.side_effect = partial(side_effect_list, returns)

        self.assertRaises(ImportError, utils.import_search_indexes, 'Test')

    @patch('haystack_elasticsearch.utils.module_has_submodule', return_value=False)
    @patch('haystack_elasticsearch.utils.warnings')
    @patch('haystack_elasticsearch.utils.importlib')
    def test_import_module_has_not_search_indexes(self, importlib, warnings, module_has_submodule):
        returns = ['Test', ImportError()]
        importlib.import_module.side_effect = partial(side_effect_list, returns)

        search_indexes_module = utils.import_search_indexes('Test')

        self.assertIsNone(search_indexes_module)

    def tearDown(self):
        pass