from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime
import subprocess
import os

from .schemas import TaxiTrip, TaxiTripCreate, TaxiTripUpdate, TaxiTripList, ImportLogSchema, ImportLogBase, Statistics, PipelineResponse
from .services import TaxiTripService, ImportLogService
from .database import get_db

router = APIRouter()


# ---------------- CRUD Trajets ---------------- #

@router.get("/trips", response_model=TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return TaxiTripList(total=total, trips=trips)


@router.get("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def read_trip(trip_id: int, db: Session = Depends(get_db)):
    db_trip = TaxiTripService.get_trip(db, trip_id=trip_id)
    if not db_trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return db_trip


@router.post("/trips", response_model=TaxiTrip, tags=["Trips"])
def create_trip(trip: TaxiTripCreate, db: Session = Depends(get_db)):
    return TaxiTripService.create_trip(db, trip)


@router.put("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, trip: TaxiTripUpdate, db: Session = Depends(get_db)):
    updated_trip = TaxiTripService.update_trip(db, trip_id, trip)
    if not updated_trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated_trip


@router.delete("/trips/{trip_id}", response_model=dict, tags=["Trips"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    success = TaxiTripService.delete_trip(db, trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"status": "deleted"}


# ---------------- CRUD ImportLog ---------------- #

@router.get("/import_logs", response_model=List[ImportLogSchema], tags=["ImportLogs"])
def get_import_logs(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    logs, total = ImportLogService.get_logs(db, skip=skip, limit=limit)
    return logs


@router.get("/import_logs/{file_name:path}", response_model=ImportLogSchema, tags=["ImportLogs"])
def read_import_log(file_name: str, db: Session = Depends(get_db)):
    db_log = ImportLogService.get_log(db, file_name=file_name)
    if not db_log:
        raise HTTPException(status_code=404, detail="ImportLog not found")
    return db_log


@router.post("/import_logs", response_model=ImportLogSchema, tags=["ImportLogs"])
def create_import_log(log: ImportLogBase, db: Session = Depends(get_db)):
    return ImportLogService.create_log(db, log)


@router.delete("/import_logs/{file_name:path}", response_model=dict, tags=["ImportLogs"])
def delete_import_log(file_name: str, db: Session = Depends(get_db)):
    success = ImportLogService.delete_log(db, file_name)
    if not success:
        raise HTTPException(status_code=404, detail="ImportLog not found")
    return {"status": "deleted"}


# ---------------- Statistics ---------------- #

@router.get("/statistics", response_model=Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    return TaxiTripService.get_statistics(db)


# ---------------- Pipeline ---------------- #

def run_pipeline_scripts(db: Session):
    # Define the base directory for the scripts
    script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    downloaded_files = []
    imported_files_count = 0
    migrated_tables = []

    try:
        # 1. Run load_raw_data.py
        print("Running load_raw_data.py...")
        download_command = ["python", os.path.join(script_dir, "load_raw_data.py")]
        download_result = subprocess.run(download_command, capture_output=True, text=True, check=True)
        print("load_raw_data.py stdout:", download_result.stdout)
        print("load_raw_data.py stderr:", download_result.stderr)
        # Parse downloaded files from stdout (this is a simple example, might need more robust parsing)
        for line in download_result.stdout.splitlines():
            if "téléchargé dans" in line:
                downloaded_files.append(line.split(" ")[1])

        # 2. Run import_to_duckdb.py
        print("Running import_to_duckdb.py...")
        import_duckdb_command = ["python", os.path.join(script_dir, "import_to_duckdb.py")]
        import_duckdb_result = subprocess.run(import_duckdb_command, capture_output=True, text=True, check=True)
        print("import_to_duckdb.py stdout:", import_duckdb_result.stdout)
        print("import_to_duckdb.py stderr:", import_duckdb_result.stderr)
        # Parse imported files count from stdout
        for line in import_duckdb_result.stdout.splitlines():
            if "Total fichiers importés :" in line:
                imported_files_count = int(line.split(":")[1].strip())

        # 3. Run migrate_to_postgres.py
        print("Running migrate_to_postgres.py...")
        migrate_postgres_command = ["python", os.path.join(script_dir, "migrate_to_postgres.py")]
        migrate_postgres_result = subprocess.run(migrate_postgres_command, capture_output=True, text=True, check=True)
        print("migrate_to_postgres.py stdout:", migrate_postgres_result.stdout)
        print("migrate_to_postgres.py stderr:", migrate_postgres_result.stderr)
        # Parse migrated tables from stdout
        for line in migrate_postgres_result.stdout.splitlines():
            if "Migration de la table :" in line:
                migrated_tables.append(line.split(":")[1].strip())

        # Log the import process in the database
        for file_name in downloaded_files:
            # This is a simplified logging, in a real scenario, you'd get rows_imported from import_to_duckdb.py output
            log_entry = ImportLogBase(file_name=file_name, import_date=datetime.utcnow(), rows_imported=0) # rows_imported is a placeholder
            ImportLogService.create_log(db, log_entry)

        return {
            "status": "success",
            "message": "Pipeline executed successfully.",
            "downloaded_files": downloaded_files,
            "imported_files_count": imported_files_count,
            "migrated_tables": migrated_tables
        }
    except subprocess.CalledProcessError as e:
        print(f"Pipeline script failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {e.stderr}")
    except Exception as e:
        print(f"An unexpected error occurred during pipeline execution: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")


@router.post("/pipeline/run", response_model=PipelineResponse, tags=["Pipeline"])
def run_pipeline_endpoint(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Triggers the data pipeline to download raw data, import it into DuckDB, and migrate to PostgreSQL.
    The process runs in the background.
    """
    background_tasks.add_task(run_pipeline_scripts, db)
    return {"status": "pending", "message": "Pipeline execution started in the background.", "downloaded_files": [], "imported_files_count": 0, "migrated_tables": []}
