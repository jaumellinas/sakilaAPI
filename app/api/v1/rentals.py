from fastapi import APIRouter, HTTPException, status, Query, Depends
from typing import List
import pymysql
from app.schemas.rental import RentalCreate, RentalResponse
from app.db.database import get_db_connection
from app.core.security import User, get_current_user

router = APIRouter(
    prefix="/api/v1/rentals",
    tags=["Reservas"]
)

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=RentalResponse,
    name="Crear reserva",
    summary="Registra una nueva reserva",
    description="Crea una nueva reserva linkeando a un cliente con un ítem del inventario"
)
async def create_rental(
    rental: RentalCreate,
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            query = """
                INSERT INTO rental
                (rental_date, inventory_id, customer_id, staff_id)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (
                rental.rental_date,
                rental.inventory_id,
                rental.customer_id,
                rental.staff_id
            ))
            conn.commit()

            rental_id = cursor.lastrowid
            cursor.execute(
                "SELECT * FROM rental WHERE rental_id = %s",
                (rental_id,)
            )
            row = cursor.fetchone()

            return RentalResponse(
                rental_id=row[0],
                rental_date=row[1],
                inventory_id=row[2],
                customer_id=row[3],
                return_date=row[4],
                staff_id=row[5],
                last_update=row[6]
            )
        except pymysql.IntegrityError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Integrity error: {str(e)}"
            )
        finally:
            cursor.close()

@router.get(
    "",
    response_model=List[RentalResponse],
    name="Devuelve todas las reservas",
    summary="Devuelve todas las reservas",
    description="Devuelve una lista con todas las reservas que existen en la base de datos"
)
async def get_rentals(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM rental ORDER BY rental_date DESC "
                "LIMIT %s OFFSET %s",
                (limit, skip)
            )
            rows = cursor.fetchall()

            rentals = []
            for row in rows:
                rentals.append(RentalResponse(
                    rental_id=row[0],
                    rental_date=row[1],
                    inventory_id=row[2],
                    customer_id=row[3],
                    return_date=row[4],
                    staff_id=row[5],
                    last_update=row[6]
                ))

            return rentals
        finally:
            cursor.close()

@router.get(
    "/{rental_id}",
    response_model=RentalResponse,
    name="Listar reserva concreta",
    summary="Listar reserva concreta por ID",
    description="Devuelve información detallada sobre una reserva específica, filtrando dicha reserva por su ID"
)
async def get_rental(
    rental_id: int,
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM rental WHERE rental_id = %s",
                (rental_id,)
            )
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="Rental not found"
                )

            return RentalResponse(
                rental_id=row[0],
                rental_date=row[1],
                inventory_id=row[2],
                customer_id=row[3],
                return_date=row[4],
                staff_id=row[5],
                last_update=row[6]
            )
        finally:
            cursor.close()

@router.put(
    "/{rental_id}/return",
    response_model=RentalResponse,
    name="Devolver reserva",
    summary="Marca reserva como devuelta",
    description="Actualiza una reserva para setear su fecha y hora de reserva a la hora actual"
)
async def return_rental(
    rental_id: int,
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT rental_id, return_date FROM rental "
                "WHERE rental_id = %s",
                (rental_id,)
            )
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="Rental not found"
                )

            if row[1] is not None:
                raise HTTPException(
                    status_code=400,
                    detail="Rental already returned"
                )

            cursor.execute(
                "UPDATE rental SET return_date = NOW(), "
                "last_update = NOW() WHERE rental_id = %s",
                (rental_id,)
            )
            conn.commit()

            cursor.execute(
                "SELECT * FROM rental WHERE rental_id = %s",
                (rental_id,)
            )
            row = cursor.fetchone()

            return RentalResponse(
                rental_id=row[0],
                rental_date=row[1],
                inventory_id=row[2],
                customer_id=row[3],
                return_date=row[4],
                staff_id=row[5],
                last_update=row[6]
            )
        finally:
            cursor.close()

@router.get(
    "/customer/{customer_id}",
    response_model=List[RentalResponse],
    name="Obtener reservas por cliente",
    summary="Devuelve todas las reservas hechas por un cliente concreto",
    description="Devuelve todas las reservas de la base de datos hechas por un cliente en específico, filtrando el cliente a través de su ID"
)
async def get_customer_rentals(
    customer_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT customer_id FROM customer WHERE customer_id = %s",
                (customer_id,)
            )
            if not cursor.fetchone():
                raise HTTPException(
                    status_code=404,
                    detail="Customer not found"
                )

            cursor.execute(
                "SELECT * FROM rental WHERE customer_id = %s "
                "ORDER BY rental_date DESC LIMIT %s OFFSET %s",
                (customer_id, limit, skip)
            )
            rows = cursor.fetchall()

            rentals = []
            for row in rows:
                rentals.append(RentalResponse(
                    rental_id=row[0],
                    rental_date=row[1],
                    inventory_id=row[2],
                    customer_id=row[3],
                    return_date=row[4],
                    staff_id=row[5],
                    last_update=row[6]
                ))

            return rentals
        finally:
            cursor.close()