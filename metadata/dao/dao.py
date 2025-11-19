from fastapi import HTTPException
from metadata.models.schema import Metadata
from typing import List, Optional
from metadata.models.model import MetadataFilterInput
import logging

logger = logging.getLogger(__name__)


class MetadataDAO:

    @staticmethod
    def save_metadata_gql(metadata, db):
        try:
            logger.info("Adding metadata to DB")
            db.add(metadata)
            db.commit()
            db.refresh(metadata)
            logger.info("Metadata added successfully")
            return metadata
        except Exception as e:
            logger.error(f"Error in saving metadata: {str(e)}")
            db.rollback()
            raise e
    
    @staticmethod
    def get_by_geoserver_name(geoserver_name: str, db):
        try:
            logger.info(f"Fetching metadata for geoserver_name: {geoserver_name}")
            return db.query(Metadata).filter(Metadata.geoserver_name == geoserver_name).first()
        except Exception as e:
            logger.error(f"Error in retrieving metadata: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error retrieving metadata")
    
    @staticmethod
    def get_by_geoserver_names(geoserver_names: List[str], db) -> List[Metadata]:
        """
        Batch fetch metadata by multiple geoserver names.
        Returns a list of metadata records matching any of the provided names.
        """
        try:
            if not geoserver_names:
                return []
            logger.info(f"Batch fetching metadata for {len(geoserver_names)} geoserver names")
            return db.query(Metadata).filter(Metadata.geoserver_name.in_(geoserver_names)).all()
        except Exception as e:
            logger.error(f"Error in batch retrieving metadata: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error batch retrieving metadata")
        
    @staticmethod
    def get_filtered(filters: Optional[MetadataFilterInput], db) -> List[Metadata]:
        try:
            logger.info(f"Fetching metadata with filters: {filters}")
            query = db.query(Metadata)

            if filters:
                if filters.id:
                    query = query.filter(Metadata.id == filters.id)
                if filters.geoserver_name:
                    query = query.filter(Metadata.geoserver_name == filters.geoserver_name)
                if filters.name_of_dataset:
                    query = query.filter(Metadata.name_of_dataset == filters.name_of_dataset)
                if filters.theme:
                    query = query.filter(Metadata.theme == filters.theme)
                if filters.keywords:
                    # Check if any keyword in the filter list exists in the database array
                    query = query.filter(Metadata.keywords.overlap(filters.keywords))
                if filters.purpose_of_creating_data:
                    query = query.filter(Metadata.purpose_of_creating_data.ilike(f"%{filters.purpose_of_creating_data}%"))
                if filters.access_constraints:
                    query = query.filter(Metadata.access_constraints.ilike(f"%{filters.access_constraints}%"))
                if filters.use_constraints:
                    query = query.filter(Metadata.use_constraints.ilike(f"%{filters.use_constraints}%"))
                if filters.data_type:
                    query = query.filter(Metadata.data_type == filters.data_type)
                if filters.contact_person:
                    query = query.filter(Metadata.contact_person == filters.contact_person)
                if filters.organization:
                    query = query.filter(Metadata.organization == filters.organization)
                if filters.mailing_address:
                    query = query.filter(Metadata.mailing_address.ilike(f"%{filters.mailing_address}%"))
                if filters.city_locality_country:
                    query = query.filter(Metadata.city_locality_country == filters.city_locality_country)
                if filters.country:
                    query = query.filter(Metadata.country == filters.country)
                if filters.contact_email:
                    query = query.filter(Metadata.contact_email == filters.contact_email)
                if filters.created_on:
                    query = query.filter(Metadata.created_on == filters.created_on)
                if filters.updated_on:
                    query = query.filter(Metadata.updated_on == filters.updated_on)

            return query.all()

        except Exception as e:
            logger.error(f"Error in retrieving filtered metadata: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error retrieving filtered metadata")

