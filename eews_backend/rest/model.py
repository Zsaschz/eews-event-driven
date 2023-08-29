from typing import List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict

class Location(BaseModel):
    coordinates: list[float] | None = [None, None]
    type: Literal['Point']

class StationModel(BaseModel):
    name: str = Field()
    description: Optional[str] = Field()
    location: Location

class MSeedData(BaseModel):
    network: Optional[str] = Field()
    station: Optional[str] = Field()
    location: Optional[str] = Field()
    channel: Optional[str] = Field()
    starttime: Optional[str] = Field()
    endtime: Optional[str] = Field()
    sampling_rate: Optional[float] = Field()
    delta: Optional[float] = Field()
    npts: Optional[int] = Field()
    calib: Optional[float] = Field()
    data: Optional[str] = Field()


class MSeed(BaseModel):
    name: str = Field()
    traces: Optional[List[MSeedData]] = Field()
