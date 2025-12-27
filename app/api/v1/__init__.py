from fastapi import APIRouter
from app.api.v1 import customers, rentals, auth

router = APIRouter()
router.include_router(customers.router)
router.include_router(rentals.router)
router.include_router(auth.router)