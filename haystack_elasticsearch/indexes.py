import copy
import importlib
import inspect
import warnings

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.datastructures import SortedDict
from django.utils.module_loading import module_has_submodule
from haystack.exceptions import SearchFieldError, NotHandled


class ClassIndex(object):
    def __init__(self, index):
        self.index = index
        self.fields = SortedDict()
        self._built = False
        self.document_field = getattr(settings, 'HAYSTACK_DOCUMENT_FIELD', 'text')
        self._fieldnames = {}
        self._facet_fieldnames = {}

    def reset(self):
        self.fields = SortedDict()
        self._built = False
        self._fieldnames = {}
        self._facet_fieldnames = {}

    def build(self):
        self.reset()
        self.collect_fields()

        self._built = True

    def collect_fields(self):
        for fieldname, field_object in self.index.fields.items():
            if field_object.document is True:
                if field_object.index_fieldname != self.document_field:
                    raise SearchFieldError(
                        "All 'SearchIndex' classes must use the same '%s' fieldname for the 'document=True' field. Offending index is '%s'." % (
                            self.document_field, self.index))

            # Stow the index_fieldname so we don't have to get it the hard way again.
            if fieldname in self._fieldnames and field_object.index_fieldname != self._fieldnames[fieldname]:
                # We've already seen this field in the list. Raise an exception if index_fieldname differs.
                raise SearchFieldError(
                    "All uses of the '%s' field need to use the same 'index_fieldname' attribute." % fieldname)

            self._fieldnames[fieldname] = field_object.index_fieldname

            # Stow the facet_fieldname so we don't have to look that up either.
            if hasattr(field_object, 'facet_for'):
                if field_object.facet_for:
                    self._facet_fieldnames[field_object.facet_for] = fieldname
                else:
                    self._facet_fieldnames[field_object.instance_name] = fieldname

            # Copy the field in so we've got a unified schema.
            if not field_object.index_fieldname in self.fields:
                self.fields[field_object.index_fieldname] = field_object
                self.fields[field_object.index_fieldname] = copy.copy(field_object)
            else:
                # If the field types are different, we can mostly
                # safely ignore this. The exception is ``MultiValueField``,
                # in which case we'll use it instead, copying over the
                # values.
                if field_object.is_multivalued == True:
                    old_field = self.fields[field_object.index_fieldname]
                    self.fields[field_object.index_fieldname] = field_object
                    self.fields[field_object.index_fieldname] = copy.copy(field_object)

                    # Switch it so we don't have to dupe the remaining
                    # checks.
                    field_object = old_field

                # We've already got this field in the list. Ensure that
                # what we hand back is a superset of all options that
                # affect the schema.
                if field_object.indexed is True:
                    self.fields[field_object.index_fieldname].indexed = True

                if field_object.stored is True:
                    self.fields[field_object.index_fieldname].stored = True

                if field_object.faceted is True:
                    self.fields[field_object.index_fieldname].faceted = True

                if field_object.use_template is True:
                    self.fields[field_object.index_fieldname].use_template = True

                if field_object.null is True:
                    self.fields[field_object.index_fieldname].null = True

    def get_model(self):
        return self.index.get_model()

    def get_index_fieldname(self, field):
        if not self._built:
            self.build()

        return self._fieldnames.get(field) or field

    def get_facet_fieldname(self, field):
        if not self._built:
            self.build()

        for fieldname, field_object in self.fields.items():
            if fieldname != field:
                continue

            if hasattr(field_object, 'facet_for'):
                if field_object.facet_for:
                    return field_object.facet_for
                else:
                    return field_object.instance_name
            else:
                return self._facet_fieldnames.get(field) or field

        return field


class UnifiedIndex(object):
    # Used to collect all the indexes into a cohesive whole.
    def __init__(self, excluded_indexes=None):
        self.indexes = {}
        self._built = False
        self.excluded_indexes = excluded_indexes or []
        self.excluded_indexes_ids = {}

    def collect_indexes(self):
        indexes = []

        for app in settings.INSTALLED_APPS:
            try:
                mod = importlib.import_module(app)
            except ImportError:
                warnings.warn('Installed app %s is not an importable Python module and will be ignored' % app)
                continue

            try:
                search_index_module = importlib.import_module("%s.search_indexes" % app)
            except ImportError:
                if module_has_submodule(mod, 'search_indexes'):
                    raise

                continue

            for item_name, item in inspect.getmembers(search_index_module, inspect.isclass):
                if getattr(item, 'haystack_use_for_indexing', False) and getattr(item, 'get_model', None):
                    # We've got an index. Check if we should be ignoring it.
                    class_path = "%s.search_indexes.%s" % (app, item_name)

                    if class_path in self.excluded_indexes or self.excluded_indexes_ids.get(item_name) == id(item):
                        self.excluded_indexes_ids[str(item_name)] = id(item)
                        continue

                    indexes.append(ClassIndex(item()))

        return indexes

    def reset(self):
        self.indexes = {}
        self._built = False

    def build(self, indexes=None):
        self.reset()

        if indexes is None:
            indexes = self.collect_indexes()

        for index in indexes:
            model = index.get_model()

            if model in self.indexes:
                raise ImproperlyConfigured(
                    "Model '%s' has more than one 'SearchIndex`` handling it. "
                    "Please exclude either '%s' or '%s' using the 'EXCLUDED_INDEXES' "
                    "setting defined in 'settings.HAYSTACK_CONNECTIONS'." % (
                        model, self.indexes[model], index
                    )
                )

            self.indexes[model] = index
            index.build()

        self._built = True

    def get_indexed_models(self):
        if not self._built:
            self.build()

        return list(self.indexes.keys())

    def get_index(self, model_klass):
        if not self._built:
            self.build()

        if model_klass not in self.indexes:
            raise NotHandled('The model %s is not registered' % model_klass.__class__)

        return self.indexes[model_klass].index

    def all_searchfields(self):
        if not self._built:
            self.build()

        return {index.index: index.fields for index in self.indexes.itervalues()}