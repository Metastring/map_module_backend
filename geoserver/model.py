from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
import re

class UploadRequest(BaseModel):
    resource_type: str = Field(..., description="Type of resource (e.g., 'shapefile', 'style', 'dataset', 'postgis')")
    workspace: str = Field(..., description="Target workspace in GeoServer")
    store_name: Optional[str] = Field(None, description="Name of the datastore (for shapefiles/datasets)")
    file_path: str = Field(..., description="Path to the file to upload")
    style_name: Optional[str] = Field(None, description="Name of the style (for style uploads)")

    @validator('resource_type')
    def validate_resource_type(cls, v):
        allowed_types = ['shapefile', 'style', 'dataset', 'postgis']
        if v not in allowed_types:
            raise ValueError(f'resource_type must be one of {allowed_types}')
        return v

    @validator('workspace')
    def validate_workspace(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('workspace must contain only letters, numbers, underscores, and hyphens')
        return v

class PostGISRequest(BaseModel):
    workspace: str = Field(..., description="Target workspace in GeoServer")
    store_name: str = Field(..., description="Name of the PostGIS datastore")
    database: str = Field(..., description="PostgreSQL database name")
    host: str = Field(..., description="Database host")
    port: int = Field(5432, ge=1, le=65535, description="Database port (1-65535)")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    db_schema: str = Field("public", description="Database schema")
    description: Optional[str] = Field(None, description="Optional description for the datastore")
    enabled: bool = Field(True, description="Whether the datastore is enabled")

    @validator('workspace')
    def validate_workspace(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('workspace must contain only letters, numbers, underscores, and hyphens')
        return v

    @validator('store_name')
    def validate_store_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('store_name must contain only letters, numbers, underscores, and hyphens')
        return v

    @validator('database')
    def validate_database(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('database must contain only letters, numbers, underscores, and hyphens')
        return v

class CreateLayerRequest(BaseModel):
    workspace: str = Field(..., description="Target workspace in GeoServer")
    store_name: str = Field(..., description="Name of the datastore")
    table_name: str = Field(..., description="Name of the table in the database")
    layer_name: Optional[str] = Field(None, description="Name for the layer (if not provided, uses table_name)")
    title: Optional[str] = Field(None, description="Display title for the layer")
    description: Optional[str] = Field(None, description="Description for the layer")
    enabled: bool = Field(True, description="Whether the layer is enabled")
    default_style: Optional[str] = Field(None, description="Default style for the layer")

    @validator('workspace')
    def validate_workspace(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('workspace must contain only letters, numbers, underscores, and hyphens')
        return v

    @validator('store_name')
    def validate_store_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('store_name must contain only letters, numbers, underscores, and hyphens')
        return v

    @validator('table_name')
    def validate_table_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('table_name must contain only letters, numbers, underscores, and hyphens')
        return v

class CreateWorkspaceRequest(BaseModel):
    workspace_name: str = Field(..., description="Name of the workspace to create")
    isolated: bool = Field(False, description="Whether the workspace is isolated")

    @validator('workspace_name')
    def validate_workspace_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('workspace_name must contain only letters, numbers, underscores, and hyphens')
        return v

class UpdateRequest(BaseModel):
    new_name: Optional[str] = Field(None, description="New name for the resource")
    new_file_path: Optional[str] = Field(None, description="New file path for the resource (if applicable)")

    @validator('new_name')
    def validate_new_name(cls, v):
        if v is not None and not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('new_name must contain only letters, numbers, underscores, and hyphens')
        return v

class TableInfo(BaseModel):
    table_name: str = Field(..., description="Name of the table")
    table_type: str = Field(..., description="Type of the table")
    table_schema: str = Field(..., description="Schema containing the table")

class SchemaTablesResponse(BaseModel):
    tables: List[TableInfo] = Field(..., description="List of tables in the schema")
    db_schema: str = Field(..., description="Schema name")
    workspace: str = Field(..., description="Workspace name")
    datastore: str = Field(..., description="Datastore name")


