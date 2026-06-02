from fastapi import HTTPException
from metadata.dao.dao import MetadataDAO
from metadata.models.model import MetadataFilterInput, MetadataType
from metadata.models.schema import DatasetMaster, DatasetContact, MapLayerInfo
from typing import List, Optional
import logging
import uuid

logger = logging.getLogger(__name__)


class MetadataService:

    @staticmethod
    def create_gql(metadata_data, db) -> MapLayerInfo:
        try:
            d = vars(metadata_data)
            logger.info(f"Creating map layer info with data: {d}")

            # Build dataset_master row from the flat input
            keywords_list = d.get("keywords")
            keywords_str = "; ".join(keywords_list) if keywords_list else None

            dataset_master = DatasetMaster(
                title=d.get("name_of_dataset"),
                description=d.get("purpose_of_creating_data"),
                keywords=keywords_str,
                dataset_type=d.get("data_type"),
                theme=d.get("theme"),
                access_constraints=d.get("access_constraints"),
                use_constraints=d.get("use_constraints"),
                is_active=True,
            )
            db.add(dataset_master)
            db.flush()  # get dataset_id without committing

            # Build map_layer_info row
            map_layer = MapLayerInfo(
                id=uuid.uuid4(),
                dataset_id=dataset_master.dataset_id,
                geoserver_name=d.get("geoserver_name"),
            )
            db.add(map_layer)

            # Build dataset_contacts row if contact info provided
            has_contact = any(
                d.get(f) for f in ("contact_person", "contact_email", "organization")
            )
            if has_contact:
                contact = DatasetContact(
                    dataset_id=dataset_master.dataset_id,
                    name=d.get("contact_person"),
                    email=d.get("contact_email"),
                    organization=d.get("organization"),
                    address=d.get("mailing_address"),
                    city=d.get("city_locality_country"),
                    country=d.get("country"),
                )
                db.add(contact)

            db.commit()
            db.refresh(map_layer)

            # Reload with relationships so properties work
            return MetadataDAO.get_by_geoserver_name(d.get("geoserver_name"), db)

        except Exception as e:
            logger.error(f"Error creating map layer record: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Error creating metadata record")

    @staticmethod
    def get_by_geoserver_name(geoserver_name: str, db) -> MapLayerInfo:
        try:
            result = MetadataDAO.get_by_geoserver_name(geoserver_name, db)
            if result:
                return result
            raise HTTPException(status_code=404, detail="Metadata not found")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @staticmethod
    def get_filtered(filters: Optional[MetadataFilterInput], db) -> List[MapLayerInfo]:
        try:
            result = MetadataDAO.get_filtered(filters, db)
            return result if result else []
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching filtered metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    @staticmethod
    def get_by_geoserver_names(geoserver_names: List[str], db) -> List[MapLayerInfo]:
        try:
            if not geoserver_names:
                return []
            return MetadataDAO.get_by_geoserver_names(geoserver_names, db)
        except Exception as e:
            logger.error(f"Error batch fetching metadata: {str(e)}")
            return []
