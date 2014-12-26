import warnings
import datetime

from django.conf import settings
from django.db.models.loading import get_model
import haystack
from haystack.backends import log_query
from haystack.backends.elasticsearch_backend import (ElasticsearchSearchBackend as HaystackBackend,
                                                     ElasticsearchSearchEngine as HaystackEngine,
                                                     DEFAULT_FIELD_MAPPING,
                                                     FIELD_MAPPINGS)
from haystack.constants import DJANGO_ID, ID, DEFAULT_OPERATOR
from haystack.constants import DJANGO_CT
from haystack.exceptions import MissingDependency
from haystack.models import SearchResult
from haystack.utils import get_model_ct, get_identifier

from haystack_elasticsearch.indexes import UnifiedIndex
from haystack_elasticsearch.utils import check_analyzers


try:
    import elasticsearch
    from elasticsearch.helpers import bulk_index
    from elasticsearch.exceptions import NotFoundError
except ImportError:
    raise MissingDependency(
        "The 'elasticsearch' backend requires the installation of 'elasticsearch'. Please refer to the documentation.")

import logging

logger = logging.getLogger(__name__)


class ElasticsearchSearchBackend(HaystackBackend):
    """
    Extends the Haystack ElasticSearch backend to allow configuration of index
    mappings and field-by-field analyzers.
    """
    DEFAULT_ANALYZER = "snowball"

    def __init__(self, connection_alias, **connection_options):
        super(ElasticsearchSearchBackend, self).__init__(connection_alias, **connection_options)
        user_settings = getattr(settings, 'ELASTICSEARCH_INDEX_SETTINGS', None)
        user_analyzer = getattr(settings, 'ELASTICSEARCH_DEFAULT_ANALYZER', None)
        if user_settings:
            setattr(self, 'DEFAULT_SETTINGS', user_settings)
        if user_analyzer:
            setattr(self, 'DEFAULT_ANALYZER', user_analyzer)

    def setup(self):
        """
        Defers loading until needed.
        """
        # Get the existing mapping & cache it. We'll compare it
        # during the ``update`` & if it doesn't match, we'll put the new
        # mapping.
        try:
            self.existing_mapping = self.conn.indices.get_mapping(index=self.index_name)
        except NotFoundError:
            pass
        except Exception:
            if not self.silently_fail:
                raise

        unified_index = haystack.connections[self.connection_alias].get_unified_index()
        check_analyzers(unified_index)
        current_mapping = self.build_schema(unified_index.indexes)

        if current_mapping != self.existing_mapping:
            try:
                # Make sure the index is there first.
                self.conn.indices.create(index=self.index_name, body=self.DEFAULT_SETTINGS, ignore=400)
                for type_name, type_body in current_mapping.iteritems():
                    self.conn.indices.put_mapping(index=self.index_name, doc_type=type_name, body=type_body)
                self.existing_mapping = current_mapping
            except Exception:
                if not self.silently_fail:
                    raise

        self.setup_complete = True

    def build_schema(self, indexes):
        schema = {}

        for model, index in indexes.iteritems():
            mapping_properties = {
                DJANGO_CT: {'type': 'string', 'index': 'not_analyzed', 'include_in_all': False},
                DJANGO_ID: {'type': 'string', 'index': 'not_analyzed', 'include_in_all': False},
            }
            for field_name, field_class in index.fields.items():
                field_mapping = FIELD_MAPPINGS.get(field_class.field_type, DEFAULT_FIELD_MAPPING).copy()
                if field_class.boost != 1.0:
                    field_mapping['boost'] = field_class.boost

                if field_class.stored is True:
                    field_mapping['store'] = True

                # Do this last to override `text` fields.
                if field_mapping['type'] == 'string':
                    if field_class.indexed is False or hasattr(field_class, 'facet_for'):
                        field_mapping['index'] = 'not_analyzed'
                        del field_mapping['analyzer']

                    elif field_class.field_type not in ('ngram', 'edge_ngram'):

                        # Check analyzer attribute
                        if not hasattr(field_class, 'analyzer') or field_class.analyzer is None:
                            logger.warning("Set default analyzer for field {}".format(field_name))
                        field_mapping['index'] = 'analyzed'
                        field_mapping['analyzer'] = getattr(field_class, 'analyzer', self.DEFAULT_ANALYZER)

                        # Check term_vector attribute
                        if hasattr(field_class, 'term_vector') and field_class.term_vector is not None:
                            field_mapping['term_vector'] = field_class.term_vector

                mapping_properties[field_class.index_fieldname] = field_mapping

            mapping_type = {
                'properties': mapping_properties,
                '_boost': {'name': 'boost', 'null_value': 1.0},
            }

            schema[get_model_ct(model)] = mapping_type

        return schema

    def update(self, index, iterable, commit=True):
        if not self.setup_complete:
            try:
                self.setup()
            except elasticsearch.TransportError as e:
                if not self.silently_fail:
                    raise

                self.log.error("Failed to add documents to Elasticsearch: %s", e)
                return

        prepped_docs = []

        for obj in iterable:
            try:
                prepped_data = index.full_prepare(obj)
                final_data = {}

                # Convert the data to make sure it's happy.
                for key, value in prepped_data.items():
                    final_data[key] = self._from_python(value)
                final_data['_id'] = final_data[ID]

                prepped_docs.append(final_data)
            except elasticsearch.TransportError as e:
                if not self.silently_fail:
                    raise

                # We'll log the object identifier but won't include the actual object
                # to avoid the possibility of that generating encoding errors while
                # processing the log message:
                self.log.error(u"%s while preparing object for update" % e.__class__.__name__, exc_info=True, extra={
                    "data": {
                        "index": index,
                        "object": get_identifier(obj)
                    }
                })

        bulk_index(self.conn, prepped_docs, index=self.index_name, doc_type=get_model_ct(index.get_model()))

        if commit:
            self.conn.indices.refresh(index=self.index_name)

    def remove(self, obj_or_string, commit=True):
        doc_id = get_identifier(obj_or_string)
        try:
            doc_type = get_model_ct(obj_or_string)
        except:
            doc_type = ''

        if not self.setup_complete:
            try:
                self.setup()
            except elasticsearch.TransportError as e:
                if not self.silently_fail:
                    raise

                self.log.error("Failed to remove document '%s' from Elasticsearch: %s", doc_id, e)
                return

        try:
            self.conn.delete(index=self.index_name, doc_type=doc_type, id=doc_id, ignore=404)

            if commit:
                self.conn.indices.refresh(index=self.index_name)
        except elasticsearch.TransportError as e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to remove document '%s' from Elasticsearch: %s", doc_id, e)

    def clear(self, models=None, commit=True):
        # We actually don't want to do this here, as mappings could be
        # very different.
        # if not self.setup_complete:
        # self.setup()
        if models is None:
            models = []

        models_to_delete = ['{}:{}'.format(DJANGO_CT, get_model_ct(model)) for model in models]

        try:
            if not models:
                self.conn.indices.delete(index=self.index_name, ignore=404)
                self.setup_complete = False
                self.existing_mapping = {}
            else:
                # Delete by query in Elasticsearch asssumes you're dealing with
                # a ``query`` root object. :/
                query = {'query': {'query_string': {'query': " OR ".join(models_to_delete)}}}
                self.conn.delete_by_query(index=self.index_name, doc_type='', body=query)
        except elasticsearch.TransportError as e:
            if not self.silently_fail:
                raise

            if len(models):
                self.log.error("Failed to clear Elasticsearch index of models '%s': %s", ','.join(models_to_delete), e)
            else:
                self.log.error("Failed to clear Elasticsearch index: %s", e)

    def build_search_kwargs(self, query_string, sort_by=None, start_offset=0, end_offset=None,
                            fields='', highlight=False, facets=None,
                            date_facets=None, query_facets=None,
                            narrow_queries=None, spelling_query=None,
                            within=None, dwithin=None, distance_point=None,
                            models=None, limit_to_registered_models=None,
                            result_class=None):
        if query_string == '*:*':
            kwargs = {
                'query': {
                    "match_all": {}
                },
            }
        else:
            kwargs = {
                'query': {
                    'query_string': {
                        'default_operator': DEFAULT_OPERATOR,
                        'query': query_string,
                        'analyze_wildcard': True,
                        'auto_generate_phrase_queries': True,
                    },
                },
            }

        if limit_to_registered_models is None:
            limit_to_registered_models = getattr(settings, 'HAYSTACK_LIMIT_TO_REGISTERED_MODELS', True)

        if models and len(models):
            model_choices = sorted(get_model_ct(model) for model in models)
        elif limit_to_registered_models:
            # Using narrow queries, limit the results to only models handled
            # with the current routers.
            model_choices = self.build_models_list()
        else:
            model_choices = []

        kwargs['models'] = model_choices

        # so far, no filters
        filters = []

        if fields:
            if isinstance(fields, (list, set)):
                fields = " ".join(fields)

            kwargs['fields'] = fields

        if sort_by is not None:
            order_list = []
            for field, direction in sort_by:
                if field == 'distance' and distance_point:
                    # Do the geo-enabled sort.
                    lng, lat = distance_point['point'].get_coords()
                    sort_kwargs = {
                        "_geo_distance": {
                            distance_point['field']: [lng, lat],
                            "order": direction,
                            "unit": "km"
                        }
                    }
                else:
                    if field == 'distance':
                        warnings.warn("In order to sort by distance, you must call the '.distance(...)' method.")

                    # Regular sorting.
                    sort_kwargs = {field: {'order': direction}}

                order_list.append(sort_kwargs)

            kwargs['sort'] = order_list

        # From/size offsets don't seem to work right in Elasticsearch's DSL. :/
        # if start_offset is not None:
        # kwargs['from'] = start_offset

        # if end_offset is not None:
        # kwargs['size'] = end_offset - start_offset

        if highlight is True:
            kwargs['highlight'] = {
                'fields': {
                    '_all': {'store': 'yes'},
                }
            }

        if self.include_spelling:
            kwargs['suggest'] = {
                'suggest': {
                    'text': spelling_query or query_string,
                    'term': {
                        # Using content_field here will result in suggestions of stemmed words.
                        'field': '_all',
                    },
                },
            }

        if narrow_queries is None:
            narrow_queries = set()

        if facets is not None:
            kwargs.setdefault('facets', {})

            for facet_fieldname, extra_options in facets.items():
                facet_options = {
                    'terms': {
                        'field': facet_fieldname,
                        'size': 100,
                    },
                }
                # Special cases for options applied at the facet level (not the terms level).
                if extra_options.pop('global_scope', False):
                    # Renamed "global_scope" since "global" is a python keyword.
                    facet_options['global'] = True
                if 'facet_filter' in extra_options:
                    facet_options['facet_filter'] = extra_options.pop('facet_filter')
                facet_options['terms'].update(extra_options)
                kwargs['facets'][facet_fieldname] = facet_options

        if date_facets is not None:
            kwargs.setdefault('facets', {})

            for facet_fieldname, value in date_facets.items():
                # Need to detect on gap_by & only add amount if it's more than one.
                interval = value.get('gap_by').lower()

                # Need to detect on amount (can't be applied on months or years).
                if value.get('gap_amount', 1) != 1 and interval not in ('month', 'year'):
                    # Just the first character is valid for use.
                    interval = "%s%s" % (value['gap_amount'], interval[:1])

                kwargs['facets'][facet_fieldname] = {
                    'date_histogram': {
                        'field': facet_fieldname,
                        'interval': interval,
                    },
                    'facet_filter': {
                        "range": {
                            facet_fieldname: {
                                'from': self._from_python(value.get('start_date')),
                                'to': self._from_python(value.get('end_date')),
                            }
                        }
                    }
                }

        if query_facets is not None:
            kwargs.setdefault('facets', {})

            for facet_fieldname, value in query_facets:
                kwargs['facets'][facet_fieldname] = {
                    'query': {
                        'query_string': {
                            'query': value,
                        }
                    },
                }

        if len(model_choices) > 0:
            filters.append({"terms": {DJANGO_CT: model_choices}})

        for q in narrow_queries:
            filters.append({
                'fquery': {
                    'query': {
                        'query_string': {
                            'query': q
                        },
                    },
                    '_cache': True,
                }
            })

        if within is not None:
            from haystack.utils.geo import generate_bounding_box

            ((south, west), (north, east)) = generate_bounding_box(within['point_1'], within['point_2'])
            within_filter = {
                "geo_bounding_box": {
                    within['field']: {
                        "top_left": {
                            "lat": north,
                            "lon": west
                        },
                        "bottom_right": {
                            "lat": south,
                            "lon": east
                        }
                    }
                },
            }
            filters.append(within_filter)

        if dwithin is not None:
            lng, lat = dwithin['point'].get_coords()
            dwithin_filter = {
                "geo_distance": {
                    "distance": dwithin['distance'].km,
                    dwithin['field']: {
                        "lat": lat,
                        "lon": lng
                    }
                }
            }
            filters.append(dwithin_filter)

        # if we want to filter, change the query type to filteres
        if filters:
            kwargs["query"] = {"filtered": {"query": kwargs.pop("query")}}
            if len(filters) == 1:
                kwargs['query']['filtered']["filter"] = filters[0]
            else:
                kwargs['query']['filtered']["filter"] = {"bool": {"must": filters}}

        return kwargs

    def _process_results(self, raw_results, highlight=False,
                         result_class=None, distance_point=None,
                         geo_sort=False):
        from haystack import connections

        results = []
        hits = raw_results.get('hits', {}).get('total', 0)
        facets = {}
        spelling_suggestion = None

        if result_class is None:
            result_class = SearchResult

        if self.include_spelling and 'suggest' in raw_results:
            raw_suggest = raw_results['suggest'].get('suggest')
            if raw_suggest:
                spelling_suggestion = ' '.join(
                    [word['text'] if len(word['options']) == 0 else word['options'][0]['text'] for word in raw_suggest])

        if 'facets' in raw_results:
            facets = {
                'fields': {},
                'dates': {},
                'queries': {},
            }

            for facet_fieldname, facet_info in raw_results['facets'].items():
                if facet_info.get('_type', 'terms') == 'terms':
                    facets['fields'][facet_fieldname] = [(individual['term'], individual['count']) for individual in
                                                         facet_info['terms']]
                elif facet_info.get('_type', 'terms') == 'date_histogram':
                    # Elasticsearch provides UTC timestamps with an extra three
                    # decimals of precision, which datetime barfs on.
                    facets['dates'][facet_fieldname] = [
                        (datetime.datetime.utcfromtimestamp(individual['time'] / 1000), individual['count']) for
                        individual in facet_info['entries']]
                elif facet_info.get('_type', 'terms') == 'query':
                    facets['queries'][facet_fieldname] = facet_info['count']

        unified_index = connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        for raw_result in raw_results.get('hits', {}).get('hits', []):
            source = raw_result['_source']
            app_label, model_name = source[DJANGO_CT].split('.')
            additional_fields = {}
            model = get_model(app_label, model_name)

            if model and model in indexed_models:
                for key, value in source.items():
                    index = unified_index.get_index(model)
                    string_key = str(key)

                    if string_key in index.fields and hasattr(index.fields[string_key], 'convert'):
                        additional_fields[string_key] = index.fields[string_key].convert(value)
                    else:
                        additional_fields[string_key] = self._to_python(value)

                del (additional_fields[DJANGO_CT])
                del (additional_fields[DJANGO_ID])

                if 'highlight' in raw_result:
                    additional_fields['highlighted'] = raw_result['highlight']

                if distance_point:
                    additional_fields['_point_of_origin'] = distance_point

                    if geo_sort and raw_result.get('sort'):
                        from haystack.utils.geo import Distance

                        additional_fields['_distance'] = Distance(km=float(raw_result['sort'][0]))
                    else:
                        additional_fields['_distance'] = None

                result = result_class(app_label, model_name, source[DJANGO_ID], raw_result['_score'],
                                      **additional_fields)
                results.append(result)
            else:
                hits -= 1

        return {
            'results': results,
            'hits': hits,
            'facets': facets,
            'spelling_suggestion': spelling_suggestion,
        }

    @log_query
    def search(self, query_string, **kwargs):
        if len(query_string) == 0:
            return {
                'results': [],
                'hits': 0,
            }

        if not self.setup_complete:
            self.setup()

        search_kwargs = self.build_search_kwargs(query_string, **kwargs)
        search_kwargs['from'] = kwargs.get('start_offset', 0)

        order_fields = set()
        for order in search_kwargs.get('sort', []):
            for key in order.keys():
                order_fields.add(key)

        geo_sort = '_geo_distance' in order_fields

        end_offset = kwargs.get('end_offset')
        start_offset = kwargs.get('start_offset', 0)
        if end_offset is not None and end_offset > start_offset:
            search_kwargs['size'] = end_offset - start_offset

        models = search_kwargs.pop('models', None)
        doc_type = ','.join(models) if models else ''

        try:
            raw_results = self.conn.search(body=search_kwargs,
                                           index=self.index_name,
                                           doc_type=doc_type)
        except elasticsearch.TransportError as e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to query Elasticsearch using '%s': %s", query_string, e)
            raw_results = {}

        return self._process_results(raw_results,
                                     highlight=kwargs.get('highlight'),
                                     result_class=kwargs.get('result_class', SearchResult),
                                     distance_point=kwargs.get('distance_point'), geo_sort=geo_sort)

    def more_like_this(self, model_instance, additional_query_string=None,
                       start_offset=0, end_offset=None, models=None,
                       limit_to_registered_models=None, result_class=None, **kwargs):
        from haystack import connections

        if not self.setup_complete:
            self.setup()

        # Deferred models will have a different class ("RealClass_Deferred_fieldname")
        # which won't be in our registry:
        model_klass = model_instance._meta.concrete_model

        index = connections[self.connection_alias].get_unified_index().get_index(model_klass)
        field_name = index.get_content_field()
        params = {}

        if start_offset is not None:
            params['search_from'] = start_offset

        if end_offset is not None:
            params['search_size'] = end_offset - start_offset

        doc_id = get_identifier(model_instance)

        try:
            raw_results = self.conn.mlt(index=self.index_name, doc_type='', id=doc_id, mlt_fields=[field_name],
                                        **params)
        except elasticsearch.TransportError as e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to fetch More Like This from Elasticsearch for document '%s': %s", doc_id, e)
            raw_results = {}

        return self._process_results(raw_results, result_class=result_class)


class ElasticsearchSearchEngine(HaystackEngine):
    backend = ElasticsearchSearchBackend
    unified_index = UnifiedIndex
