import io

import pandas as pd
from sqlalchemy.orm import Session

from data_ingestion.dao.dao import data_ingestion_dao


class data_ingestion_service:
    @staticmethod
    async def create_table_and_insert1(table_name: str, file, db: Session) -> str:
        """Read an uploaded Excel/CSV file, create a table dynamically, and insert its data.

        The provided table_name is used directly as the database table name.
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

            # Create table dynamically
            data_ingestion_dao.create_table1(table_name, df, db)

            # Insert data into the newly created table
            data_ingestion_dao.insert_data_dynamic1(table_name, df, db)

            return f"Table '{table_name}' created and data inserted successfully."
        except Exception as e:
            # Re-raise to let the API layer convert this to an HTTP error
            raise e