from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
from .models import YellowTaxiTrip, ImportLog
from .schemas import TaxiTripCreate, TaxiTripUpdate, TaxiTrip, ImportLogBase, ImportLogSchema, Statistics


class TaxiTripService:

    @staticmethod
    def get_trip(db: Session, trip_id: int):
        return db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()

    @staticmethod
    def get_trips(db: Session, skip: int = 0, limit: int = 100):
        trips = db.query(YellowTaxiTrip).offset(skip).limit(limit).all()
        total = db.query(func.count(YellowTaxiTrip.id)).scalar()
        return trips, total

    @staticmethod
    def create_trip(db: Session, trip: TaxiTripCreate):
        db_trip = YellowTaxiTrip(**trip.dict())
        db.add(db_trip)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def update_trip(db: Session, trip_id: int, trip: TaxiTripUpdate):
        db_trip = db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()
        if not db_trip:
            return None
        for key, value in trip.dict(exclude_unset=True).items():
            setattr(db_trip, key, value)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def delete_trip(db: Session, trip_id: int):
        db_trip = db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()
        if not db_trip:
            return False
        db.delete(db_trip)
        db.commit()
        return True

    @staticmethod
    def get_statistics(db: Session):
        total_trips = db.query(func.count(YellowTaxiTrip.id)).scalar()
        earliest_pickup = db.query(func.min(YellowTaxiTrip.tpep_pickup_datetime)).scalar()
        latest_dropoff = db.query(func.max(YellowTaxiTrip.tpep_dropoff_datetime)).scalar()
        average_trip_distance = db.query(func.avg(YellowTaxiTrip.trip_distance)).scalar()
        average_fare_amount = db.query(func.avg(YellowTaxiTrip.fare_amount)).scalar()

        return Statistics(
            total_trips=total_trips or 0,
            earliest_pickup=earliest_pickup,
            latest_dropoff=latest_dropoff,
            average_trip_distance=float(average_trip_distance or 0),
            average_fare_amount=float(average_fare_amount or 0)
        )

class ImportLogService:

    @staticmethod
    def create_log(db: Session, log: ImportLogBase):
        db_log = ImportLog(**log.dict())
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        return db_log

    @staticmethod
    def get_log(db: Session, file_name: str):
        return db.query(ImportLog).filter(ImportLog.file_name == file_name).first()

    @staticmethod
    def get_logs(db: Session, skip: int = 0, limit: int = 100):
        logs = db.query(ImportLog).offset(skip).limit(limit).all()
        total = db.query(func.count(ImportLog.file_name)).scalar()
        return logs, total

    @staticmethod
    def delete_log(db: Session, file_name: str):
        db_log = db.query(ImportLog).filter(ImportLog.file_name == file_name).first()
        if not db_log:
            return False
        db.delete(db_log)
        db.commit()
        return True