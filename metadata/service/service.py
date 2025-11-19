from fastapi import HTTPException
from metadata.dao.dao import MetadataDAO
from metadata.models.model import MetadataFilterInput, MetadataType
from typing import List, Optional
from metadata.models.schema import Metadata
import logging
import uuid

logger = logging.getLogger(__name__)

class MetadataService:

    @staticmethod
    def create_gql(metadata_data, db):
        try:
            metadata_dict = vars(metadata_data)
            logger.info(f"Creating metadata with data: {metadata_dict}")
            # Create metadata instance
            metadata = Metadata(id=uuid.uuid4(), **metadata_dict)
            # Save to DB
            return MetadataDAO.save_metadata_gql(metadata, db)
        except Exception as e:
            logger.error(f"Error creating metadata record: {str(e)}")
            raise HTTPException(status_code=500, detail="Error creating metadata record")
    
    @staticmethod
    def get_by_geoserver_name(geoserver_name: str, db):
        try:
            logger.info(f"Fetching metadata for geoserver_name: {geoserver_name}")
            result = MetadataDAO.get_by_geoserver_name(geoserver_name, db)
            if result:
                return result
            else:
                logger.error("No metadata found for the given geoserver_name.")
                raise HTTPException(status_code=404, detail="Metadata not found")
        except Exception as e:
            logger.error(f"Error in fetching metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")
        
    @staticmethod
    def get_filtered(filters: Optional[MetadataFilterInput], db) -> List[MetadataType]:
        try:
            logger.info(f"Fetching metadata with filters: {filters}")
            result = MetadataDAO.get_filtered(filters, db)
            if result:
                return result
            else:
                logger.error("No metadata found for the given filters.")
                raise HTTPException(status_code=404, detail="No metadata found")
        except Exception as e:
            logger.error(f"Error in fetching filtered metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")
    
    @staticmethod
    def get_by_geoserver_names(geoserver_names: List[str], db) -> List[Metadata]:
        """
        Batch fetch metadata by multiple geoserver names.
        Returns a list of metadata records (empty list if none found, no exception).
        """
        try:
            if not geoserver_names:
                return []
            logger.info(f"Batch fetching metadata for {len(geoserver_names)} geoserver names")
            return MetadataDAO.get_by_geoserver_names(geoserver_names, db)
        except Exception as e:
            logger.error(f"Error in batch fetching metadata: {str(e)}")
            # Return empty list instead of raising exception for batch operations
            return []

