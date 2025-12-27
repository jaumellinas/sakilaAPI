from pydantic import BaseModel, Field

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., min_length=5, max_length=100)
    password: str = Field(
        ...,
        min_length=8,
        max_length=72,
        description="Password must be between 8 and 72 characters"
    )

class UserResponse(BaseModel):
    user_id: int
    username: str
    email: str
    disabled: bool