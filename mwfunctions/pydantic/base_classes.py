from pydantic import BaseModel
from datetime import date, datetime
from enum import Enum
import json

def dumper(obj):
    if type(obj) in [datetime, date]:
        return str(obj)
    else:
        return obj

class MWBaseModel(BaseModel):

    def dict(self, json_serializable=False, *args, **kwargs):
        output_dict = super(MWBaseModel, self).dict(*args, **kwargs)
        if json_serializable:
            return json.loads(json.dumps(output_dict, default=dumper))
        else:
            return output_dict

    '''
    ### Dict functions
    '''
    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __repr__(self):
        return repr(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __delitem__(self, key):
        del self.__dict__[key]

    def clear(self):
        return self.__dict__.clear()

    def copy(self):
        return self.__dict__.copy()

    def has_key(self, k):
        return k in self.__dict__

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def pop(self, *args):
        return self.__dict__.pop(*args)

    def __cmp__(self, dict_):
        return self.__cmp__(self.__dict__, dict_)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __unicode__(self):
        return unicode(repr(self.__dict__))

    class Config:
        use_enum_values = True


class EnumBase(Enum):
    @classmethod
    def to_list(cls):
        return list(map(lambda c: c.value, cls))

    def __eq__(self, other):
        """
            make string comparison with enum object possible.
            e.g.:
                class CategoryEnum(EnumBase)
                    EXAMPLE_CATEGORY = "diy"

                "diy" == CategoryEnum.EXAMPLE_CATEGORY returns True
                old case:
                    super().__eq__("diy") would return False
        """
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)

    def __hash__(self):
        return super.__hash__(self)

    def __str__(self):
        # str(EnumClass) should return its inner value and not "EnumClass.key_field"
        return self.value

class TextLanguage(str, EnumBase):
    # https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
    GERMAN="de"
    ENGLISH="en"
    ITALIAN="it"
    SPANISH="es"
    FRENCH="fr"
    JAPANESE="ja"

class Marketplace(str, EnumBase):
    DE="de"
    COM="com"
    UK="co.uk" # TODO: check if "." leads to problem in BQ, FS, storage or some other place
    ES="es"
    IT="it"
    FR="fr"
    JP="co.jp"

    @classmethod
    def get_marketplaces_to_crawl(cls):
        return [m for m in cls.to_list() if m in [cls.DE, cls.COM]]

    def get_default_language(self):
        Marketplace2DefaultTextLanguage_dict = {
            Marketplace.DE: TextLanguage.GERMAN,
            Marketplace.COM: TextLanguage.ENGLISH,
            Marketplace.UK: TextLanguage.ENGLISH,
            Marketplace.ES: TextLanguage.SPANISH,
            Marketplace.IT: TextLanguage.ITALIAN,
            Marketplace.FR: TextLanguage.FRENCH,
            Marketplace.JP: TextLanguage.JAPANESE
        }
        return Marketplace2DefaultTextLanguage_dict[self]




Marketplace2DefaultTextLanguage_dict = {
    Marketplace.DE: TextLanguage.GERMAN,
    Marketplace.COM: TextLanguage.ENGLISH,
    Marketplace.UK: TextLanguage.ENGLISH,
    Marketplace.ES: TextLanguage.SPANISH,
    Marketplace.IT: TextLanguage.ITALIAN,
    Marketplace.FR: TextLanguage.FRENCH,
    Marketplace.JP: TextLanguage.JAPANESE
}