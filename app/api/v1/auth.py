from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.core.security import (
    authenticate_user,
    create_access_token,
    get_password_hash,
    Token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)
from app.db.database import get_db_connection
from app.schemas.user import UserCreate
import pymysql

router = APIRouter(prefix="/api/v1/auth", tags=["Autenticación"])

@router.post("/register",
             status_code=status.HTTP_201_CREATED,
             name="Registrar usuario",
             summary="Registra un usuario de la API en la base de datos",
             description="Registra el usuario, el e-mail y la contraseña de un usuario en la base de datos y le da acceso a la misma a través de un token"
             )
async def register(user: UserCreate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            hashed_password = get_password_hash(user.password)
            query = """
                INSERT INTO user (username, email, hashed_password)
                VALUES (%s, %s, %s)
            """
            cursor.execute(
                query,
                (user.username, user.email, hashed_password)
            )
            conn.commit()
            return {"message": "Usuario creado correctamente"}
        except pymysql.IntegrityError:
            raise HTTPException(
                status_code=400,
                detail="El usuario o el e-mail ya existen en la base de datos"
            )
        finally:
            cursor.close()

@router.post("/token",
             response_model=Token,
             name="Autenticar usuario",
             summary="Autentica un usuario de la API y le devuelve su token",
             description="Autentica un usuario de la API comprobando si su usuario y su contraseña existen en la base de datos. Si es así, se le devuelve su token de autenticación"
             )
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(
        form_data.username,
        form_data.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    access_token = create_access_token(
        data={"sub": form_data.username},
        expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }