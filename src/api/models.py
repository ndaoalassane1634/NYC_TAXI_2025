from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from sqlalchemy.sql import func
from .database import Base

class YellowTaxiTrip(Base):
    __tablename__ = "yellow_taxi_trips"

    id = Column(Integer, primary_key=True, index=True)
    VendorID = Column(BigInteger)
    tpep_pickup_datetime = Column(DateTime)
    tpep_dropoff_datetime = Column(DateTime)
    passenger_count = Column(Float)
    trip_distance = Column(Float)
    RatecodeID = Column(Float)
    store_and_fwd_flag = Column(String)
    PULocationID = Column(BigInteger)
    DOLocationID = Column(BigInteger)
    payment_type = Column(BigInteger)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    Airport_fee = Column(Float)
    cbd_congestion_fee = Column(Float)

class ImportLog(Base):
    __tablename__ = "import_log"

    file_name = Column(String, primary_key=True, index=True)
    import_date = Column(DateTime, default=func.now())
    rows_imported = Column(BigInteger)
