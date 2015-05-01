import importlib
import warnings

from django.utils.module_loading import module_has_submodule
from haystack.exceptions import SearchFieldError


def check_analyzers(unified_index):
    """Check in all indexes if uses of a field have the same analyzer.

    :param unified_index: Unified index object from Haystack.
    :type unified_index: haystack.utils.UnifiedIndex
    :raise: haystack.exceptions.SearchFieldError
    """
    indexes = unified_index.collect_indexes()

    for index in indexes:
        analyzers_mapping = {}
        for fieldname, field_object in index.fields.items():
            if hasattr(field_object, 'analyzer'):
                if fieldname in analyzers_mapping:
                    if field_object.analyzer != analyzers_mapping[fieldname]:
                        raise SearchFieldError(
                            "All uses of the '{}' field need to use the same 'analyzer'. Index '{}'".format(fieldname,
                                                                                                            index))
                else:
                    analyzers_mapping[fieldname] = field_object.analyzer


def import_search_indexes(app):
    """Import search_indexes module of an application.

    :param app: Application.
    :type app: str
    :return: search_indexes module.
    :rtype: callable
    :raise: ImportError if app has search_indexes module and cannot be imported.
    """
    mod = None
    try:
        mod = importlib.import_module(app)
        search_index_module = importlib.import_module("%s.search_indexes" % app)
    except ImportError:
        if mod is None:
            warnings.warn('Installed app %s is not an importable Python module and will be ignored' % app)
            search_index_module = None
        else:
            if module_has_submodule(mod, 'search_indexes'):
                raise
            else:
                search_index_module = None
    return search_index_module