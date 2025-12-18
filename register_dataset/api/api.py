import logging
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session
from typing import List, Optional
import json
from database.database import get_db
from register_dataset.model.model import (
    RegisterDatasetRequest,
    RegisterDatasetResponse,
    RegisterDatasetFormData,
    StyleConfigForColumn,
)
from register_dataset.service.service import RegisterDatasetService
from geoserver.dao import GeoServerDAO
from geoserver.service import GeoServerService
from geoserver.admin.service import GeoServerAdminService
from geoserver.admin.dao import GeoServerAdminDAO
from styles.service.style_service import StyleService
from utils.config import (
    geoserver_host,
    geoserver_port,
    geoserver_username,
    geoserver_password,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["register-dataset"])

# Initialize GeoServer services
geo_dao = GeoServerDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password,
)
geo_service = GeoServerService(geo_dao)

geo_admin_dao = GeoServerAdminDAO(
    base_url=f"http://{geoserver_host}:{geoserver_port}/geoserver/rest",
    username=geoserver_username,
    password=geoserver_password,
)
geo_admin_service = GeoServerAdminService(geo_admin_dao)


def get_register_service(db: Session = Depends(get_db)) -> RegisterDatasetService:
    """Dependency to get RegisterDatasetService instance."""
    style_service = StyleService(db, geo_dao, geo_service)
    return RegisterDatasetService(
        db=db,
        geo_service=geo_service,
        geo_admin_service=geo_admin_service,
        style_service=style_service,
    )


@router.post(
    "/register",
    response_model=RegisterDatasetResponse,
    summary="Register Complete Dataset",
    description="""
    Register a complete dataset with a single API call. This endpoint:
    1. Uploads XLSX or CSV file and creates PostGIS table (calls create-table-and-insert1)
    2. Configures GeoServer layer (SRS, bounding boxes, tile caching)
    3. Creates metadata entry
    4. Generates styles for multiple columns
    
    This is a comprehensive endpoint that orchestrates multiple operations.
    """,
)
async def register_dataset(
    # File upload
    file: UploadFile = File(..., description="XLSX or CSV file to upload"),
    # Form data as JSON string (will be parsed into RegisterDatasetFormData)
    form_data_json: str = Form(..., description="JSON string containing form data"),
    # Service dependency
    service: RegisterDatasetService = Depends(get_register_service),
):
    """
    Register a complete dataset with upload, metadata, and styling.
    """
    try:
        # Parse form_data JSON string into RegisterDatasetFormData
        try:
            form_data_dict = json.loads(form_data_json)
            form_data = RegisterDatasetFormData(**form_data_dict)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON in form_data: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error parsing form_data: {str(e)}"
            )
        
        # Parse tags
        tags_list = None
        if form_data.tags:
            tags_list = [t.strip() for t in form_data.tags.split(",") if t.strip()]

        # Parse keywords
        keywords_list = None
        if form_data.keywords:
            keywords_list = [k.strip() for k in form_data.keywords.split(",") if k.strip()]

        # Parse style configs
        try:
            style_configs_data = json.loads(form_data.style_configs_json)
            style_configs = [
                StyleConfigForColumn(**config) for config in style_configs_data
            ]
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON in style_configs_json: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error parsing style configurations: {str(e)}"
            )

        # Parse data_source
        from styles.models.model import DataSource as DataSourceEnum
        try:
            data_source_enum = DataSourceEnum(form_data.data_source.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data_source: {form_data.data_source}. Must be 'postgis' or 'geoserver'"
            )

        # Validate file extension (actual file type detection happens in service layer)
        if not file.filename:
            raise HTTPException(
                status_code=400,
                detail="File must have a filename"
            )
        
        file_extension = file.filename.lower()
        if not (file_extension.endswith(".csv") or file_extension.endswith(".xlsx") or file_extension.endswith(".xls")):
            raise HTTPException(
                status_code=400,
                detail="Only CSV, XLSX, and XLS files are allowed"
            )

        # Build request
        request = RegisterDatasetRequest(
            table_name=form_data.table_name,
            db_schema=form_data.db_schema,  # Uses alias 'schema' for JSON serialization
            uploaded_by=form_data.uploaded_by,
            layer_name=form_data.layer_name,
            tags=tags_list,
            workspace=form_data.workspace,
            store_name=form_data.store_name,
            name_of_dataset=form_data.name_of_dataset,
            theme=form_data.theme,
            keywords=keywords_list,
            purpose_of_creating_data=form_data.purpose_of_creating_data,
            access_constraints=form_data.access_constraints,
            use_constraints=form_data.use_constraints,
            data_type=form_data.data_type,
            contact_person=form_data.contact_person,
            organization=form_data.organization,
            mailing_address=form_data.mailing_address,
            city_locality_country=form_data.city_locality_country,
            country=form_data.country,
            contact_email=form_data.contact_email,
            style_configs=style_configs,
            data_source=data_source_enum,
            publish_styles_to_geoserver=form_data.publish_styles_to_geoserver,
            attach_styles_to_layer=form_data.attach_styles_to_layer,
            user_id=form_data.user_id,
            user_email=form_data.user_email,
        )

        # Call service
        result = await service.register_dataset(request, file)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in register_dataset endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

