from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CustomerCreate(BaseModel):
    store_id: int
    first_name: str
    last_name: str
    email: Optional[str] = None
    address_id: int
    active: bool = True

    class Config:
        json_schema_extra = {
            "example": {
                "store_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
                "address_id": 5,
                "active": True
            }
        }

class CustomerUpdate(BaseModel):
    store_id: Optional[int] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    address_id: Optional[int] = None
    active: Optional[bool] = None

class CustomerResponse(BaseModel):
    customer_id: int
    store_id: int
    first_name: str
    last_name: str
    email: Optional[str]
    address_id: int
    active: bool
    create_date: datetime
    last_update: datetime