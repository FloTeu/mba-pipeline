from django import template
register = template.Library()

@register.simple_tag
def define(val=None):
  return val

@register.simple_tag()
def multiply(item1, item2, *args, **kwargs):
    return item1 * item2