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


class ElasticCharFieldMixin(ConfigurableFieldMixin):
    """
    Field that has analyzer attribute.
    """
    def __init__(self, analyzer=None, term_vector=None, **kwargs):
        self.analyzer = analyzer
        self.term_vector = term_vector

        super(ElasticCharFieldMixin, self).__init__(**kwargs)


class CharField(ElasticCharFieldMixin, BaseCharField):
    pass


class LocationField(ConfigurableFieldMixin, BaseLocationField):
    pass


class NgramField(ConfigurableFieldMixin, BaseNgramField):
    pass


class EdgeNgramField(ConfigurableFieldMixin, BaseEdgeNgramField):
    pass


class IntegerField(ConfigurableFieldMixin, BaseIntegerField):
    pass


class FloatField(ConfigurableFieldMixin, BaseFloatField):
    pass


class DecimalField(ConfigurableFieldMixin, BaseDecimalField):
    pass


class BooleanField(ConfigurableFieldMixin, BaseBooleanField):
    pass


class DateField(ConfigurableFieldMixin, BaseDateField):
    pass


class DateTimeField(ConfigurableFieldMixin, BaseDateTimeField):
    pass


class MultiValueField(ConfigurableFieldMixin, BaseMultiValueField):
    pass


class FacetField(ConfigurableFieldMixin, BaseFacetField):
    pass


class BaseFacetCharField(ElasticCharFieldMixin, BaseFacetField):
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
