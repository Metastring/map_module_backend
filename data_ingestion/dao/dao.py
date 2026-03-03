from sqlalchemy import Column, Float, Integer, MetaData, String, Table
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from utils.config import db_schema


SCHEMA = db_schema


class data_ingestion_dao:
    @staticmethod
    def create_table1(table_name: str, df, db: Session):
        """
        Dynamically create a table in the database.

        :param table_name: Name of the table to be created.
        :param df: DataFrame to infer schema from.
        :param db: Database session.
        """
        # Get the database engine
        engine = db.get_bind()

        # Create a new MetaData object using configured schema
        metadata = MetaData(schema=SCHEMA)

        # Create columns dynamically based on the DataFrame's columns and dtypes
        columns = []
        for col_name, dtype in df.dtypes.items():
            if dtype in ["int64", "int32"]:
                columns.append(Column(col_name, Integer))
            elif dtype in ["float64", "float32"]:
                columns.append(Column(col_name, Float))
            else:
                columns.append(Column(col_name, String))

        # Define the table
        table = Table(table_name, metadata, *columns)

        # Create the table in the database
        metadata.create_all(engine)

    @staticmethod
    def insert_data_dynamic1(table_name: str, df, db: Session):
        """
        Insert data into a dynamically created table.

        :param table_name: Name of the table.
        :param df: DataFrame containing data to insert.
        :param db: Database session.
        """
        try:
            # Convert DataFrame to dictionaries
            data = df.to_dict(orient="records")

            # Dynamically map to the table using configured schema
            engine = db.get_bind()
            metadata = MetaData(schema=SCHEMA)
            table = Table(table_name, metadata, autoload_with=engine)

            # Insert data into the table
            with db.begin():
                db.execute(table.insert(), data)
        except SQLAlchemyError as e:
            db.rollback()
            raise e