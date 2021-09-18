from pydantic import Field
from mwfunctions.pydantic.base_classes import MWBaseModel

class ProjectorMetaDataShortLists(MWBaseModel):
    bsr_short_x:str = Field(description="Comma seperated string of dates related to bsr")
    bsr_short_y:str = Field(description="Comma seperated string of bsr")
    prices_short_x:str = Field(description="Comma seperated string of dates related to price")
    prices_short_y:str = Field(description="Comma seperated string of prices")
