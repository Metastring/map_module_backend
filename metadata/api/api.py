import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from metadata.models.model import MetadataFilterInput, MetadataType, MetadataInput
from metadata.service.service import MetadataService
from database.database import get_db
import logging
import uuid
from typing import Optional, List
import configparser

config = configparser.ConfigParser()
encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
read_success = False
for encoding in encodings_to_try:
    try:
        if config.read('secure.ini', encoding=encoding):
            read_success = True
            break
    except (UnicodeDecodeError, UnicodeError):
        continue
    except Exception:
        continue

if not read_success:
    raise ValueError("Error reading secure.ini: Could not decode file with any supported encoding. Please ensure the file is saved as UTF-8.")

logger = logging.getLogger(__name__)

class MetadataAPI:
    version = "/v1"
    router = APIRouter()

def get_context(db: Session = Depends(get_db)):
    return {"db": db}


@strawberry.type
class Mutation:

    @strawberry.mutation
    @staticmethod
    def create(info, metadata_data: MetadataInput) -> MetadataType:
        db: Session = info.context["db"]  # Get database session from context
        logger.info(f"Creating new metadata with data: {metadata_data}")
        try:
            new_metadata = MetadataService.create_gql(metadata_data, db)
            if new_metadata:
                return new_metadata
        except Exception as e:
            logger.error(f"Error in create_metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

@strawberry.type
class Query:

    @strawberry.field
    @staticmethod
    def get(info, geoserver_name: str) -> MetadataType:
        db: Session = info.context["db"]  # Get database session from context
        logger.info(f"Fetching metadata for geoserver_name: {geoserver_name}")
        try:
            metadata = MetadataService.get_by_geoserver_name(geoserver_name, db)
            if metadata:
                return metadata
            else:
                raise HTTPException(status_code=404, detail=f"No metadata found for geoserver_name {geoserver_name}")
        except HTTPException:
            # Re-raise HTTPException as-is
            raise
        except Exception as e:
            logger.error(f"Error in get_metadata query: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")
    
    @strawberry.field
    @staticmethod
    def get_any(info, filters: Optional[MetadataFilterInput] = None) -> List[MetadataType]:
        db: Session = info.context["db"]  # Get database session from context
        logger.info(f"Fetching metadata with filters: {filters}")
        try:
            metadata_list = MetadataService.get_filtered(filters, db)
            # Return empty list if no records found (for "get all" queries)
            return metadata_list if metadata_list else []
        except HTTPException:
            # Re-raise HTTPException as-is
            raise
        except Exception as e:
            logger.error(f"Error in get_filtered query: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

# Register the GraphQL router with API key verification
schema = strawberry.Schema(query=Query, mutation=Mutation)
metadata_app = GraphQLRouter(schema, context_getter=get_context)

