import re
from typing import Optional
from pydantic import BaseModel, Field, validator


class UpdateRequest(BaseModel):
    new_name: Optional[str] = Field(None, description="New name for the resource")
    new_file_path: Optional[str] = Field(None, description="New file path for the resource (if applicable)")

    @validator('new_name')
    def validate_new_name(cls, v):
        if v is not None and not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError(
                'new_name must contain only letters, numbers, underscores, and hyphens'
            )
        return v

