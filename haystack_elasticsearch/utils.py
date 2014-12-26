from haystack.exceptions import SearchFieldError


def check_analyzers(unified_index):
    """Check in all indexes if uses of a field have the same analyzer.

    :param unified_index: Unified index object from Haystack.
    :type unified_index: haystack.utils.UnifiedIndex
    :raise: haystack.exceptions.SearchFieldError
    """
    indexes = unified_index.collect_indexes()
    analyzers_mapping = {}

    for index in indexes:
        for fieldname, field_object in index.fields.items():
            if hasattr(field_object, 'analyzer'):
                if fieldname in analyzers_mapping:
                    if field_object.analyzer != analyzers_mapping[fieldname]:
                        raise SearchFieldError(
                            "All uses of the '{}' field need to use the same 'analyzer'. Index '{}'".format(fieldname,
                                                                                                            index))
                else:
                    analyzers_mapping[fieldname] = field_object.analyzer