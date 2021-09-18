
from pydantic import BaseModel, Field
from mwfunctions.pydantic.base_classes import MWBaseModel

class BQPlotDataRaw(MWBaseModel):
    plot_x:str = Field(description="Comma seperated string of dates related to bsr")
    plot_y:str = Field(description="Comma seperated string of bsr")
    plot_x_price:str = Field(default=None, description="Comma seperated string of dates related to price")
    plot_y_price:str = Field(default=None, description="Comma seperated string of prices")