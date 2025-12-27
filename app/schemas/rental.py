from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class RentalCreate(BaseModel):
    rental_date: datetime
    inventory_id: int
    customer_id: int
    staff_id: int

    class Config:
        json_schema_extra = {
            "example": {
                "rental_date": "2024-12-02T10:00:00",
                "inventory_id": 1,
                "customer_id": 1,
                "staff_id": 1
            }
        }

class RentalResponse(BaseModel):
    rental_id: int
    rental_date: datetime
    inventory_id: int
    customer_id: int
    return_date: Optional[datetime]
    staff_id: int
    last_update: datetime