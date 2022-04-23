from mwfunctions.pydantic.base_classes import EnumBase, Marketplace
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt


class MWRootCollectionType(str, EnumBase):
    SHIRTS = "shirts"

    def get_pydantic_class(self):
        MWRootCollection2PydanticClass = {MWRootCollectionType.SHIRTS: FSMBAShirt}
        return MWRootCollection2PydanticClass[self]


class MWRootCollection(object):
    def __init__(self, marketplace: Marketplace, root_col_type: MWRootCollectionType):
        self.marketplace: Marketplace = marketplace
        self.root_col_type: MWRootCollectionType = root_col_type
        self.root_col = self.get_root_col_str(marketplace, root_col_type)

    @staticmethod
    def get_root_col_str(marketplace: Marketplace, root_col_type: MWRootCollectionType):
        return f"{marketplace}_{root_col_type}"

    @staticmethod
    def root_col_str2marketplace_and_root_col_type(root_col: str) -> (Marketplace, MWRootCollectionType):
        # TODO return enum object and not str
        return root_col.split("_")[0], "_".join(root_col.split("_")[1:len(root_col.split("_"))])

    @classmethod
    def to_list(cls, return_str=False):
        # returns all possible variation of MWRootCollection which have format {marketplace}_{root_col_type}
        marketplaces = list(map(lambda c: c, Marketplace))
        root_col_types = list(map(lambda c: c, MWRootCollectionType))
        if return_str:
            return [cls(marketplace, root_col_type).get_root_col() for marketplace in marketplaces for root_col_type in
                    root_col_types]
        else:
            return [cls(marketplace, root_col_type) for marketplace in marketplaces for root_col_type in root_col_types]

    def __str__(self):
        return self.root_col

    def get_root_col(self) -> str:
        return self.root_col