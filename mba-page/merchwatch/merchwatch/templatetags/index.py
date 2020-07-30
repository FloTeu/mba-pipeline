from django import template
register = template.Library()

@register.filter
def index(indexable, i):
    return indexable[i]

@register.filter
def get_item_and_first(dictionary, key):
    return dictionary.get(key)[0]


@register.filter
def get_item_and_second(dictionary, key):
    return dictionary.get(key)[1]