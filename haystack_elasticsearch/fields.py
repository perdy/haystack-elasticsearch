from haystack.exceptions import SearchFieldError
from haystack.fields import (CharField as BaseCharField,
                             LocationField as BaseLocationField,
                             NgramField as BaseNgramField,
                             EdgeNgramField as BaseEdgeNgramField,
                             IntegerField as BaseIntegerField,
                             FloatField as BaseFloatField,
                             DecimalField as BaseDecimalField,
                             BooleanField as BaseBooleanField,
                             DateField as BaseDateField,
                             DateTimeField as BaseDateTimeField,
                             MultiValueField as BaseMultiValueField,
                             FacetField as BaseFacetField)


class ConfigurableFieldMixin(object):
    """
    A mixin which allows custom settings on a per field basis.
    """
    def __init__(self, **kwargs):
        super(ConfigurableFieldMixin, self).__init__(**kwargs)


class ElasticField(ConfigurableFieldMixin):
    pass


class ElasticCharField(ElasticField):
    """
    Field that has analyzer attribute.
    """
    def __init__(self, analyzer=None, term_vector=None, **kwargs):
        self.analyzer = analyzer
        self.term_vector = term_vector

        super(ElasticCharField, self).__init__(**kwargs)


class CharField(ElasticCharField, BaseCharField):
    pass


class LocationField(ElasticField, BaseLocationField):
    pass


class NgramField(ElasticField, BaseNgramField):
    pass


class EdgeNgramField(ElasticField, BaseEdgeNgramField):
    pass


class IntegerField(ElasticField, BaseIntegerField):
    pass


class FloatField(ElasticField, BaseFloatField):
    pass


class DecimalField(ElasticField, BaseDecimalField):
    pass


class BooleanField(ElasticField, BaseBooleanField):
    pass


class DateField(ElasticField, BaseDateField):
    pass


class DateTimeField(ElasticField, BaseDateTimeField):
    pass


class MultiValueField(ElasticField, BaseMultiValueField):
    pass


class FacetField(ElasticField, BaseFacetField):
    pass


class BaseFacetCharField(ElasticCharField, BaseFacetField):
    pass


class FacetCharField(BaseFacetCharField, CharField):
    pass


class FacetIntegerField(FacetField, IntegerField):
    pass


class FacetFloatField(FacetField, FloatField):
    pass


class FacetDecimalField(FacetField, DecimalField):
    pass


class FacetBooleanField(FacetField, BooleanField):
    pass


class FacetDateField(FacetField, DateField):
    pass


class FacetDateTimeField(FacetField, DateTimeField):
    pass


class FacetMultiValueField(FacetField, MultiValueField):
    pass
