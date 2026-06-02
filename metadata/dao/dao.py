from fastapi import HTTPException
from metadata.models.schema import MapLayerInfo, DatasetMaster, DatasetContact
from typing import List, Optional
from metadata.models.model import MetadataFilterInput
from sqlalchemy.orm import joinedload, contains_eager
import logging

logger = logging.getLogger(__name__)

_LOAD_OPTS = [
    joinedload(MapLayerInfo.dataset_master).joinedload(DatasetMaster.contacts)
]


class MetadataDAO:

    @staticmethod
    def save_map_layer(map_layer: MapLayerInfo, db):
        try:
            db.add(map_layer)
            db.commit()
            db.refresh(map_layer)
            return map_layer
        except Exception as e:
            logger.error(f"Error saving map_layer_info: {str(e)}")
            db.rollback()
            raise e

    @staticmethod
    def get_by_geoserver_name(geoserver_name: str, db) -> Optional[MapLayerInfo]:
        try:
            logger.info(f"Fetching map layer for geoserver_name: {geoserver_name}")
            return (
                db.query(MapLayerInfo)
                .options(*_LOAD_OPTS)
                .filter(MapLayerInfo.geoserver_name == geoserver_name)
                .first()
            )
        except Exception as e:
            logger.error(f"Error retrieving map layer: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error retrieving metadata")

    @staticmethod
    def get_by_geoserver_names(geoserver_names: List[str], db) -> List[MapLayerInfo]:
        try:
            if not geoserver_names:
                return []
            logger.info(f"Batch fetching map layers for {len(geoserver_names)} geoserver names")
            return (
                db.query(MapLayerInfo)
                .options(*_LOAD_OPTS)
                .filter(MapLayerInfo.geoserver_name.in_(geoserver_names))
                .all()
            )
        except Exception as e:
            logger.error(f"Error batch retrieving map layers: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error batch retrieving metadata")

    @staticmethod
    def get_filtered(filters: Optional[MetadataFilterInput], db) -> List[MapLayerInfo]:
        try:
            logger.info(f"Fetching map layers with filters: {filters}")

            query = (
                db.query(MapLayerInfo)
                .join(MapLayerInfo.dataset_master)
                .outerjoin(DatasetMaster.contacts)
                .options(
                    contains_eager(MapLayerInfo.dataset_master)
                    .contains_eager(DatasetMaster.contacts)
                )
            )

            if filters:
                if filters.id:
                    query = query.filter(MapLayerInfo.id == filters.id)
                if filters.geoserver_name:
                    query = query.filter(MapLayerInfo.geoserver_name == filters.geoserver_name)
                if filters.name_of_dataset:
                    query = query.filter(DatasetMaster.title == filters.name_of_dataset)
                if filters.theme:
                    query = query.filter(DatasetMaster.theme == filters.theme)
                if filters.keywords:
                    for kw in filters.keywords:
                        query = query.filter(DatasetMaster.keywords.ilike(f"%{kw}%"))
                if filters.purpose_of_creating_data:
                    query = query.filter(
                        DatasetMaster.description.ilike(f"%{filters.purpose_of_creating_data}%")
                    )
                if filters.access_constraints:
                    query = query.filter(
                        DatasetMaster.access_constraints.ilike(f"%{filters.access_constraints}%")
                    )
                if filters.use_constraints:
                    query = query.filter(
                        DatasetMaster.use_constraints.ilike(f"%{filters.use_constraints}%")
                    )
                if filters.data_type:
                    query = query.filter(DatasetMaster.dataset_type == filters.data_type)
                if filters.contact_person:
                    query = query.filter(DatasetContact.name == filters.contact_person)
                if filters.organization:
                    query = query.filter(DatasetContact.organization == filters.organization)
                if filters.mailing_address:
                    query = query.filter(
                        DatasetContact.address.ilike(f"%{filters.mailing_address}%")
                    )
                if filters.city_locality_country:
                    query = query.filter(DatasetContact.city == filters.city_locality_country)
                if filters.country:
                    query = query.filter(DatasetContact.country == filters.country)
                if filters.contact_email:
                    query = query.filter(DatasetContact.email == filters.contact_email)
                if filters.created_on:
                    query = query.filter(DatasetMaster.created_at == filters.created_on)
                if filters.updated_on:
                    query = query.filter(DatasetMaster.updated_at == filters.updated_on)

            return query.distinct().all()

        except Exception as e:
            logger.error(f"Error retrieving filtered map layers: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=400, detail="Error retrieving filtered metadata")
