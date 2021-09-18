from typing import Union, Dict, List
from pydantic import BaseModel, Field
from mwfunctions.pydantic.base_classes import MWBaseModel

# class FSWatchItemPlotDataNumber(MWBaseModel):
#     plot_y:

class FSWatchItemSubCollectionPlotDataYear(MWBaseModel):
    bsr: Dict[str, Union[int, float]] # str is date_str
    prices: Dict[str, Union[int, float]] # str is date_str
    year: int

class FSWatchItemSubCollectionDict(MWBaseModel):
    """ Example:
            {
                "plot_data":
                    {
                        "year":
                            {"bsr": {"2020-09-20": 480549, ...},
                            "price": {"2020-09-20": 13.99, ...}
                            }
                    }
            }
    """
    plot_data: Dict[str, FSWatchItemSubCollectionPlotDataYear] # str/key is year

class FSWatchItemShortenedPlotData(MWBaseModel):
    """ Example:
            {
                "bsr_short":
                    {"2020-09-20": 480549, ...}
                "prices_short":
                    {"2020-09-20": 480549, ...}
            }
    """
    bsr_short: Dict[str, Union[int, float]] # str is date_str
    prices_short: Dict[str, Union[int, float]] # str is date_str