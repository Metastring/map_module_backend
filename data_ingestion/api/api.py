from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from database.database import get_db
from data_ingestion.service.service import data_ingestion_service


class data_ingestion_api:
    router = APIRouter()

    ########################## Upload file (XLSX or CSV) ##########################

    @staticmethod
    @router.post("/create-table-and-insert1", summary="Create a table and insert data into it",description=(
            "Create a table and insert data into it from an uploaded Excel (.xlsx) "
            "or CSV (.csv) file. The provided table_name will be used as the table "
            "name in the database."
        ),
    )
    async def create_table_and_insert1(
        table_name: str, file: UploadFile = File(...), db: Session = Depends(get_db)
    ):
        filename = file.filename or ""
        if not (filename.endswith(".xlsx") or filename.endswith(".csv")):
            raise HTTPException(
                status_code=400,
                detail="Only XLSX and CSV files are allowed",
            )
        try:
            message = await data_ingestion_service.create_table_and_insert1(
                table_name, file, db
            )
            return {"message": message}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))