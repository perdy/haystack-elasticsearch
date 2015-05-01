from __future__ import unicode_literals

from django.test import TestCase


def side_effect_list(returns, *args):
    result = returns.pop(0)
    if isinstance(result, Exception):
        raise result
    return result


class ElasticsearchBackendTestCase(TestCase):
    def setUp(self):
        pass

    def test_setup(self):
        pass

    def test_build_schema(self):
        pass

    def test_update(self):
        pass

    def test_remove(self):
        pass

    def test_clear(self):
        pass

    def test_build_search_kwargs(self):
        pass

    def test_process_results(self):
        pass

    def test_search(self):
        pass

    def test_more_like_this(self):
        pass

    def tearDown(self):
        pass
