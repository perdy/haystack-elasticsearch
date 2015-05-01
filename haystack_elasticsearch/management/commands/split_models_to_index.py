from optparse import make_option

from haystack import connections

from django.core.management.base import BaseCommand
from django.contrib.contenttypes.models import ContentType
from builtins import reduce


def collect_indexes(initial, weighted_list, limit):
    final = initial
    count = 0
    while count < limit:
        count += weighted_list[final][1]
        final += 1
    return final-1


class Command(BaseCommand):
    """
     Example:
     ['Quote.fixedforwardquote',
     'trades.forwarddeal',
     'settlements.bankaccountentry',
     'trades.spotdeal',
     'trades.nondeliverableforwarddeal',
     'trades.drawdown',
     'settlements.beneficiarypayment',
     'trades.limitorder',
     'clients.client',
     'trades.syntheticforwarddeal',
     'Quote.windowforwardquote',
     'risk.margincall',
     'Quote.quote',
     'settlements.bankaccount']

     >> python manage.py split_models_to_index 2 1

     >> Quote.fixedforwardquote, trades.forwarddeal, settlements.bankaccountentry, trades.spotdeal, trades.nondeliverableforwarddeal, trades.drawdown, settlements.beneficiarypayment

     >> python manage.py split_models_to_index 2 2

     >> trades.limitorder, clients.client, trades.syntheticforwarddeal, Quote.windowforwardquote, risk.margincall, Quote.quote, settlements.bankaccount

    """
    help = "Prints the models to be indexes by N processes" \
           "Usage: python manage.py split_models_to_index <partition_number> <part_to_return>" \
           "Example: python manage.py split_models_to_index 2 1"

    option_list = BaseCommand.option_list + (
        make_option('--using',
                    action='store',
                    dest='using',
                    default='default',
                    help='The Haystack backend to use'))

    def handle(self, *args, **options):

        if len(args) != 2:
            raise AttributeError("Invalid number of arguments. ")

        partition_number = int(args[0])
        part_to_return = int(args[1])

        backend = options.get('using')
        unified_index = connections[backend].get_unified_index()
        models_to_index = [model for model in connections['default'].get_unified_index().get_indexed_models()]
        weighted_models_to_index = [(model_to_index, unified_index.indexes[model_to_index].index_queryset().count())
                                    for model_to_index in models_to_index]
        total_count = reduce(lambda x, y: x + y[1], weighted_models_to_index, 0)
        weighted_models_to_index = sorted(weighted_models_to_index, key=lambda x: x[1])
        content_types_to_index = {}
        for indexed_model in models_to_index:
            content_type = ContentType.objects.get_for_model(indexed_model)
            content_types_to_index[indexed_model] = "{}.{}".format(content_type.app_label, content_type.model)

        limit = (total_count / partition_number)

