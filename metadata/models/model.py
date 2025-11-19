from typing import Optional, List
from datetime import datetime
from uuid import UUID
import strawberry


@strawberry.type
class MetadataType:
    id: UUID
    dataset_id: Optional[UUID]
    geoserver_name: str
    name_of_dataset: str
    theme: Optional[str]
    keywords: Optional[List[str]]
    purpose_of_creating_data: Optional[str]
    access_constraints: Optional[str]
    use_constraints: Optional[str]
    data_type: Optional[str]
    contact_person: Optional[str]
    organization: Optional[str]
    mailing_address: Optional[str]
    city_locality_country: Optional[str]
    country: Optional[str]
    contact_email: Optional[str]
    created_on: datetime
    updated_on: Optional[datetime]


@strawberry.input
class MetadataInput:
    dataset_id: Optional[UUID] = None
    geoserver_name: str
    name_of_dataset: str
    theme: Optional[str] = None
    keywords: Optional[List[str]] = None
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None


@strawberry.input
class MetadataFilterInput:
    id: Optional[UUID] = None
    dataset_id: Optional[UUID] = None
    geoserver_name: Optional[str] = None
    name_of_dataset: Optional[str] = None
    theme: Optional[str] = None
    keywords: Optional[List[str]] = None
    purpose_of_creating_data: Optional[str] = None
    access_constraints: Optional[str] = None
    use_constraints: Optional[str] = None
    data_type: Optional[str] = None
    contact_person: Optional[str] = None
    organization: Optional[str] = None
    mailing_address: Optional[str] = None
    city_locality_country: Optional[str] = None
    country: Optional[str] = None
    contact_email: Optional[str] = None
    created_on: Optional[datetime] = None
    updated_on: Optional[datetime] = None

