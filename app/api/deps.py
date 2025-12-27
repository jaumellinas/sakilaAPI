from app.core.security import get_current_user, User
from fastapi import Depends

async def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> User:
    if current_user.disabled:
        raise Exception("Usuario desactivado")
    return current_user