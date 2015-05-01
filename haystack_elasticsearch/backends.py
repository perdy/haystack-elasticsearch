import json
import warnings
import datetime

from django.conf import settings
from django.db.models.loading import get_model
from django.utils import six
import haystack
from haystack.backends import log_query
from haystack.backends.elasticsearch_backend import (ElasticsearchSearchBackend as HaystackBackend,
                                                     ElasticsearchSearchEngine as HaystackEngine,
                                                     ElasticsearchSearchQuery as HaystackQuery,
                                                     DEFAULT_FIELD_MAPPING,
                                                     FIELD_MAPPINGS)
from haystack.constants import DJANGO_ID, ID, DEFAULT_OPERATOR, DJANGO_CT
from haystack.exceptions import MissingDependency, NotHandled
from haystack.inputs import Exact, Raw, Clean, PythonData, BaseInput
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
        """Get the existing mapping & cache it. We'll compare it during the ``update`` and if it doesn't match,
        we'll put the new mapping.
        """
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
        """Build Elasticsearch schema.

        :param indexes: Dictionary of model -> index.
        :type indexes: dict
        :return: Schema.
        :rtype: dict
        """
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
                    if field_class.indexed is False or hasattr(field_class, 'facet_for') or getattr(field_class, 'is_multivalued', False):
                        field_mapping['index'] = 'not_analyzed'
                        try:
                            del field_mapping['analyzer']
                            del field_mapping['term_vector']
                        except:
                            pass

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
        """Update an index with a collection.

        :param index: Index to be updated.
        :type index: Index
        :param iterable: Objects to update the index.
        :type iterable: iterable
        :param commit: Commit changes.
        :type commit: bool
        """
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

        doc_type = get_model_ct(index.get_model())
        bulk_index(self.conn, prepped_docs, index=self.index_name, doc_type=doc_type)

        if commit:
            self.conn.indices.refresh(index=self.index_name)

    def remove(self, obj_or_string, commit=True):
        """Remove an object from an index.

        :param obj_or_string: Object to be removed.
        :param commit: Commit changes.
        :type commit: bool
        """
        doc_id = get_identifier(obj_or_string)
        try:
            doc_type = get_model_ct(obj_or_string)
        except:
            try:
                doc_type = obj_or_string.rsplit('.', 1)[0]
            except:
                doc_type = '*'

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
        """Clear an index.

        :param models: Models to be cleared.
        :type models: list
        :param commit: Commit changes.
        :type commit: bool
        """

        if models is None:
            models = []
            doc_type = ''
        else:
            doc_type = ','.join([get_model_ct(model) for model in models])

        try:
            if not models:
                self.conn.indices.delete(index=self.index_name, ignore=404)
                self.setup_complete = False
                self.existing_mapping = {}
            else:
                # Delete by query in Elasticsearch asssumes you're dealing with
                # a ``query`` root object. :/
                query = {'query': {'query_string': {'query': '*'}}}
                self.conn.delete_by_query(index=self.index_name, doc_type=doc_type, body=query)
        except elasticsearch.TransportError as e:
            if not self.silently_fail:
                raise

            if len(models):
                self.log.error("Failed to clear Elasticsearch index of models '%s': %s", doc_type, e)
            else:
                self.log.error("Failed to clear Elasticsearch index: %s", e)

    def build_search_kwargs(self, query_string, sort_by=None, start_offset=0, end_offset=None,
                            fields='', highlight=False, facets=None,
                            date_facets=None, query_facets=None,
                            narrow_queries=None, spelling_query=None,
                            within=None, dwithin=None, distance_point=None,
                            models=None, limit_to_registered_models=None,
                            result_class=None):
        """Build all kwargs necessaries to perform the query.

        :param query_string: Query string.
        :type query_string: str
        :param sort_by:
        :param start_offset: If query is partially done, this parameters will represents where the slice begins.
        :type start_offset: int
        :param end_offset: If query is partially done, this parameters will represents where the slice ends.
        :type end_offset: int
        :param fields: Fields that will be searched for.
        :type fields: str
        :param highlight:
        :param facets:
        :param date_facets:
        :param query_facets:
        :param narrow_queries:
        :param spelling_query:
        :param within:
        :param dwithin:
        :param distance_point:
        :param models: List of models over the query will be performed.
        :type models: list
        :param limit_to_registered_models:
        :param result_class: Class used for search results.
        :type result_class: object
        :return: Search kwargs.
        :rtype: dict
        """
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

    def _process_results_facets_section(self, raw_results):
        """Process facets section from raw results.

        :param raw_results: Result returned from ElasticSearch API.
        :type raw_results: dict
        :return: Facets section.
        :rtype: dict
        """
        # Initialize return value
        facets = {}

        # Do processing
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

        return facets

    def _process_results_results_section(self, distance_point, geo_sort, raw_results, result_class):
        """Process results section from raw results.

        :param raw_results: Result returned from ElasticSearch API.
        :type raw_results: dict
        :return: Results section.
        :rtype: dict
        """
        from haystack import connections
        if distance_point and geo_sort:
            from haystack.utils.geo import Distance

        # Get result class
        if result_class is None:
            result_class = SearchResult

        # Initialize return values
        hits = raw_results.get('hits', {}).get('total', 0)
        results = []

        # Get unified index
        unified_index = connections[self.connection_alias].get_unified_index()

        # Do processing
        for raw_result in raw_results.get('hits', {}).get('hits', []):
            try:
                source = raw_result['_source']
                app_label, model_name = source[DJANGO_CT].split('.')
                additional_fields = {}
                model = get_model(app_label, model_name)
                index = unified_index.get_index(model)

                for key, value in [(k, v) for k, v in source.items() if k != DJANGO_CT and k != DJANGO_ID]:
                    string_key = str(key)

                    try:
                        additional_fields[string_key] = index.fields[string_key].convert(value)
                    except (KeyError, NameError):
                        additional_fields[string_key] = self._to_python(value)

                try:
                    additional_fields['highlighted'] = raw_result['highlight']
                except KeyError:
                    pass

                if distance_point:
                    additional_fields['_point_of_origin'] = distance_point

                    if geo_sort:
                        try:
                            additional_fields['_distance'] = Distance(km=float(raw_result['sort'][0]))
                        except KeyError:
                            additional_fields['_distance'] = None

                result = result_class(app_label, model_name, source[DJANGO_ID], raw_result['_score'],
                                      **additional_fields)
                results.append(result)
            except NotHandled:
                hits -= 1
            except KeyError:
                hits -= 1
                logger.warning("Hit has no source field")

        return results, hits

    def _process_results_suggest_section(self, raw_results):
        """Process suggest section from raw results.

        :param raw_results: Result returned from ElasticSearch API.
        :type raw_results: dict
        :return: Suggestion section.
        :rtype: dict
        """
        # Initialize return value
        spelling_suggestion = None

        # Do processing
        if self.include_spelling and 'suggest' in raw_results:
            raw_suggest = raw_results['suggest'].get('suggest')
            if raw_suggest:
                spelling_suggestion = ' '.join(
                    [word['text'] if len(word['options']) == 0 else word['options'][0]['text'] for word in raw_suggest])

        return spelling_suggestion

    def _process_results(self, raw_results, highlight=False, result_class=None, distance_point=None, geo_sort=False):
        """Process results from a raw dictionary obtained in a query to Elasticsearch API

        :param raw_results: Raw results.
        :type raw_results: dict
        :param highlight: Indicates if highlighted.
        :type highlight: bool
        :param result_class: Class used for a result value.
        :type result_class: object
        :param distance_point: Initial distance.
        :type distance_point: object
        :param geo_sort: Indicates if sorted using geo.
        :type geo_sort: bool
        :return: Processed results.
        :rtype: dict
        """

        spelling_suggestion = self._process_results_suggest_section(raw_results)

        facets = self._process_results_facets_section(raw_results)

        results, hits = self._process_results_results_section(distance_point, geo_sort, raw_results, result_class)

        return {
            'results': results,
            'hits': hits,
            'facets': facets,
            'spelling_suggestion': spelling_suggestion,
        }

    @log_query
    def search(self, query_string, **kwargs):
        """Do a search in Elasticsearch.

        :param query_string: The string that will be used for querying.
        :type query_string: str
        :param kwargs: Search parameters.
        :type kwargs: dict
        :return: Search results.
        """
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
            raw_results = self.conn.search(body=search_kwargs, index=self.index_name, doc_type=doc_type, _source=True)
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
        """Do a 'more like this' search in Elasticsearch.

        :param model_instance: Model that will be used as a base for 'more like this'.
        :type model_instance: object
        :param additional_query_string:
        :param start_offset: If query is partially done, this parameters will represents where the slice begins.
        :type start_offset: int
        :param end_offset: If query is partially done, this parameters will represents where the slice ends.
        :type end_offset: int
        :param models: Models to be searched.
        :type models: list
        :param limit_to_registered_models:
        :param result_class: Class used for a result value.
        :type result_class: object
        :param kwargs: Search parameters.
        :type kwargs: dict
        :return: Search results.
        """
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


class ElasticsearchSearchQuery(HaystackQuery):
    """
    Provides a way to specify search parameters and lazily load results.

    This implementation changes how Query fragment is constructed, applying changes related to multi-type.
    """
    def build_query_fragment(self, field, filter_type, value):
        """Construct the query fragment based on the field that is been search for.

        :param field: Field to search.
        :type field: str
        :param filter_type: Filter type (contains, gt, lt...)
        :type filter_type: str
        :param value: Value to search.
        :type value: str
        :return: Query fragment.
        :rtype: str
        """
        from haystack import connections

        if not hasattr(value, 'input_type_name'):
            # Handle when we've got a ``ValuesListQuerySet``...
            if hasattr(value, 'values_list'):
                value = list(value)

            if isinstance(value, six.string_types):
                # It's not an ``InputType``. Assume ``Clean``.
                value = Clean(value)
            else:
                value = PythonData(value)

        # Prepare the query using the InputType.
        prepared_value = value.prepare(self)

        if not isinstance(prepared_value, (set, list, tuple)):
            # Then convert whatever we get back to what elasticsearch wants if needed.
            prepared_value = self.backend._from_python(prepared_value)

        # 'content' is a special reserved word, much like 'pk' in
        # Django's ORM layer. It indicates 'no special field'.
        if field == 'content':
            index_fieldnames = {}
        else:
            index_fieldnames = connections[self._using].get_unified_index().get_index_fieldname(field)

        filter_types = {
            'contains': u'%s',
            'startswith': u'%s*',
            'exact': u'%s',
            'gt': u'{%s TO *}',
            'gte': u'[%s TO *]',
            'lt': u'{* TO %s}',
            'lte': u'[* TO %s]',
        }

        if value.post_process is False:
            query_frag = prepared_value
        else:
            if filter_type in ['contains', 'startswith']:
                if value.input_type_name == 'exact':
                    query_frag = prepared_value
                else:
                    # Iterate over terms & incorportate the converted form of each into the query.
                    terms = []

                    if isinstance(prepared_value, six.string_types):
                        for possible_value in prepared_value.split(' '):
                            term = filter_types[filter_type] % self.backend._from_python(possible_value)
                            terms.append(u'"%s"' % term)
                    elif isinstance(prepared_value, bool):
                        term = filter_types[filter_type] % six.text_type(prepared_value).lower()
                        terms.append(u'"%s"' % term)
                    else:
                        terms.append(filter_types[filter_type] % prepared_value)

                    if len(terms) == 1:
                        query_frag = terms[0]
                    else:
                        query_frag = u"(%s)" % " AND ".join(terms)
            elif filter_type == 'in':
                in_options = []

                for possible_value in prepared_value:
                    if isinstance(possible_value, six.string_types):
                        in_options.append(u'"%s"' % self.backend._from_python(possible_value))
                    elif isinstance(possible_value, bool):
                        term = filter_types[filter_type] % six.text_type(possible_value).lower()
                        in_options.append(u'"%s"' % term)
                    else:
                        in_options.append(u'%s' % possible_value)

                query_frag = u"(%s)" % " OR ".join(in_options)
            elif filter_type == 'range':
                start = self.backend._from_python(prepared_value[0])
                end = self.backend._from_python(prepared_value[1])
                query_frag = u'[%s TO %s]' % (start, end)
            elif filter_type == 'exact':
                if value.input_type_name == 'exact':
                    query_frag = prepared_value
                else:
                    prepared_value = Exact(prepared_value).prepare(self)
                    query_frag = filter_types[filter_type] % prepared_value
            else:
                if value.input_type_name == 'exact':
                    prepared_value = Exact(prepared_value).prepare(self)

                query_frag = filter_types[filter_type] % prepared_value

        if len(query_frag) and not isinstance(value, Raw):
            if not query_frag.startswith('(') and not query_frag.endswith(')'):
                query_frag = '(%s)' % str(query_frag)

        field_names = set(index_fieldnames.values())
        if field_names:
            multiple_query_frag = ' OR '.join([u'%s:%s' % (field_name, query_frag) for field_name in field_names])
            result = "(%s)" % multiple_query_frag
        else:
            result = query_frag

        return result


class ElasticsearchSearchEngine(HaystackEngine):
    backend = ElasticsearchSearchBackend
    query = ElasticsearchSearchQuery
    unified_index = UnifiedIndex
