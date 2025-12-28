from pydantic import BaseModel, Field

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., min_length=5, max_length=100)
    password: str = Field(
        ...,
        min_length=8,
        max_length=72,
        description="La contraseña tiene que tener entre 8 y 72 caracteres"
    )

class UserResponse(BaseModel):
    user_id: int
    username: str
    email: str
    disabled: bool