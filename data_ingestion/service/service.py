import io

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text

from data_ingestion.dao.dao import data_ingestion_dao
from utils.config import db_schema


SCHEMA = db_schema

# Mapping from table_name (slug) to dataset_master.title
DATASET_TITLE_MAPPING = {
    "economic_census_of_india": "Economic Census of India (1990–2013)",
    "pmgsy_rural_roads_administrative_data": "PMGSY Rural Roads Administrative Data",
    "socio-economic_and_caste_census_(secc)_aggregated_data": (
        "Socio-Economic and Caste Census (SECC) Aggregated Data"
    ),
}


class data_ingestion_service:
    @staticmethod
    async def create_table_and_insert1(table_name: str, file, db: Session) -> str:
        """Read an uploaded Excel/CSV file, create a table dynamically, and insert its data.

        The provided table_name is used directly as the database table name, and a dataset_id
        column is added based on the matching entry in the dataset_master table.
        """
        # Read the file contents
        contents = await file.read()
        filename = file.filename or ""

        try:
            # Decide how to load the file based on extension
            if filename.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(contents))
            elif filename.endswith(".csv"):
                # pandas can read CSV from a BytesIO buffer
                df = pd.read_csv(io.BytesIO(contents))
            else:
                raise ValueError("Unsupported file type. Only XLSX and CSV are allowed.")

            # Resolve dataset title from table_name
            dataset_title = DATASET_TITLE_MAPPING.get(table_name)
            if not dataset_title:
                raise ValueError(
                    f"Unknown dataset mapping for table_name '{table_name}'."
                )

            # Fetch dataset_id from dataset_master based on title
            engine = db.get_bind()
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT dataset_id FROM {SCHEMA}.dataset_master "
                        "WHERE title = :title"
                    ),
                    {"title": dataset_title},
                ).first()

            if not result:
                raise ValueError(
                    f"Dataset with title '{dataset_title}' not found in dataset_master."
                )

            dataset_id = result[0]

            # Add dataset_id column to the DataFrame so the created table also has this column
            df["dataset_id"] = int(dataset_id)

            # Create table dynamically (includes dataset_id column)
            data_ingestion_dao.create_table1(table_name, df, db)

            # Insert data into the newly created table, including dataset_id
            data_ingestion_dao.insert_data_dynamic1(table_name, df, db)

            return (
                f"Table '{table_name}' created and data inserted successfully with "
                f"dataset_id={dataset_id}."
            )
        except Exception as e:
            # Re-raise to let the API layer convert this to an HTTP error
            raise e