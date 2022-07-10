from mwfunctions.pydantic.base_classes import EnumBase

class RequestMethod(EnumBase):
    POST="post"
    GET="get"
    PATCH="patch"