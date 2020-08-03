from django import template
from urllib.parse import urlencode
import random
register = template.Library()

@register.simple_tag
def define(val=None):
  return val

@register.simple_tag()
def multiply(item1, item2, *args, **kwargs):
    return item1 * item2
'''
@register.simple_tag(takes_context=True)
def url_replace(context, **kwargs):
    query = context['request'].GET.copy()
    query.update(kwargs)
    return query.urlencode()
'''

@register.simple_tag
def url_replace(request, field, value):
    dict_ = request.GET.copy()

    dict_[field] = value

    return dict_.urlencode()

@register.simple_tag
def random_int(a, b=None):
    if b is None:
        a, b = 0, a
    return random.randint(a, b)