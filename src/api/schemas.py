from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional, List

# --- TaxiTrip Schemas ---
class TaxiTripBase(BaseModel):
    VendorID: Optional[int] = None
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[float] = None
    trip_distance: Optional[float] = None
    RatecodeID: Optional[float] = None
    store_and_fwd_flag: Optional[str] = None
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    Airport_fee: Optional[float] = None
    cbd_congestion_fee: Optional[float] = None

class TaxiTripCreate(TaxiTripBase):
    pass

class TaxiTripUpdate(TaxiTripBase):
    pass

class TaxiTrip(TaxiTripBase):
    id: int

    model_config = ConfigDict(from_attributes=True)

class TaxiTripList(BaseModel):
    total: int
    trips: List[TaxiTrip]

# --- ImportLog Schemas ---
class ImportLogBase(BaseModel):
    file_name: str
    import_date: Optional[datetime] = None
    rows_imported: Optional[int] = None

class ImportLogSchema(ImportLogBase):
    model_config = ConfigDict(from_attributes=True)

# --- Statistics Schema ---
class Statistics(BaseModel):
    total_trips: int
    earliest_pickup: Optional[datetime] = None
    latest_dropoff: Optional[datetime] = None
    average_trip_distance: Optional[float] = None
    average_fare_amount: Optional[float] = None

# --- Pipeline Response Schema ---
class PipelineResponse(BaseModel):
    status: str
    message: str
    downloaded_files: List[str]
    imported_files_count: int
    migrated_tables: List[str]