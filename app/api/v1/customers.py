from fastapi import APIRouter, HTTPException, status, Query, Depends
from typing import List
import pymysql
from app.schemas.customer import (
    CustomerCreate,
    CustomerUpdate,
    CustomerResponse,
)
from app.db.database import get_db_connection
from app.core.security import User, get_current_user

router = APIRouter(
    prefix="/api/v1/customers",
    tags=["Clientes"]
)

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=CustomerResponse,
    name="Crear cliente",
    summary="Crea un nuevo cliente",
    description="Registra un nuevo cliente en la base de datos con los siguientes datos asociados: tienda, nombre, e-mail y dirección"
)
async def create_customer(
    customer: CustomerCreate,
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            query = """
                INSERT INTO customer
                (store_id, first_name, last_name, email,
                 address_id, active, create_date)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(query, (
                customer.store_id,
                customer.first_name,
                customer.last_name,
                customer.email,
                customer.address_id,
                customer.active
            ))
            conn.commit()

            customer_id = cursor.lastrowid
            cursor.execute(
                "SELECT * FROM customer WHERE customer_id = %s",
                (customer_id,)
            )
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=500,
                    detail="Error al recuperar cliente creado"
                )

            return CustomerResponse(
                customer_id=row[0],
                store_id=row[1],
                first_name=row[2],
                last_name=row[3],
                email=row[4],
                address_id=row[5],
                active=bool(row[6]),
                create_date=row[7],
                last_update=row[8]
            )
        except pymysql.IntegrityError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error de integridad: {str(e)}"
            )
        finally:
            cursor.close()

@router.get(
    "",
    response_model=List[CustomerResponse],
    name="Listar clientes",
    summary="Devuelve todos los clientes",
    description="Devuelve un .json con todos los datos de los clientes registrados en la base de datos en ese momento"
)
async def get_customers(
    skip: int = Query(0, ge=0),
    limit: int = Query(None, ge=1),
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            if limit is None:
                cursor.execute(
                    "SELECT * FROM customer ORDER BY customer_id "
                    "OFFSET %s",
                    (skip,)
                )
            else:
                cursor.execute(
                    "SELECT * FROM customer ORDER BY customer_id "
                    "LIMIT %s OFFSET %s",
                    (limit, skip)
                )
            rows = cursor.fetchall()

            customers = []
            for row in rows:
                customers.append(CustomerResponse(
                    customer_id=row[0],
                    store_id=row[1],
                    first_name=row[2],
                    last_name=row[3],
                    email=row[4],
                    address_id=row[5],
                    active=bool(row[6]),
                    create_date=row[7],
                    last_update=row[8]
                ))

            return customers
        finally:
            cursor.close()

@router.get(
    "/{customer_id}",
    response_model=CustomerResponse,
    name="Listar cliente concreto",
    summary="Listar cliente concreto por ID",
    description="Devuelve toda la información almacenada en la base de datos sobre un cliente en específico, filtrando el mismo en la base de datos a través de el ID"
)
async def get_customer(
    customer_id: int,
    current_user: User = Depends(get_current_user)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM customer WHERE customer_id = %s",
                (customer_id,)
            )
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="Cliente no encontrado"
                )

            return CustomerResponse(
                customer_id=row[0],
                store_id=row[1],
                first_name=row[2],
                last_name=row[3],
                email=row[4],
                address_id=row[5],
                active=bool(row[6]),
                create_date=row[7],
                last_update=row[8]
            )
        finally:
            cursor.close()

@router.put(
    "/{customer_id}",
    response_model=CustomerResponse,
    name="Modificar datos de un cliente",
    summary="Modificar datos de un cliente concreto",
    description="Modifica uno o más campos de un cliente concreto ya existente a través de su ID"
)
async def update_customer(
    customer_id: int,
    customer: CustomerUpdate,
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
                    detail="Cliente no encontrado"
                )

            updates = []
            values = []

            if customer.store_id is not None:
                updates.append("store_id = %s")
                values.append(customer.store_id)
            if customer.first_name is not None:
                updates.append("first_name = %s")
                values.append(customer.first_name)
            if customer.last_name is not None:
                updates.append("last_name = %s")
                values.append(customer.last_name)
            if customer.email is not None:
                updates.append("email = %s")
                values.append(customer.email)
            if customer.address_id is not None:
                updates.append("address_id = %s")
                values.append(customer.address_id)
            if customer.active is not None:
                updates.append("active = %s")
                values.append(customer.active)

            if not updates:
                raise HTTPException(
                    status_code=400,
                    detail="No hay cambios a aplicar"
                )

            updates.append("last_update = NOW()")
            values.append(customer_id)

            query = (
                f"UPDATE customer SET {', '.join(updates)} "
                f"WHERE customer_id = %s"
            )
            cursor.execute(query, tuple(values))
            conn.commit()

            cursor.execute(
                "SELECT * FROM customer WHERE customer_id = %s",
                (customer_id,)
            )
            row = cursor.fetchone()

            return CustomerResponse(
                customer_id=row[0],
                store_id=row[1],
                first_name=row[2],
                last_name=row[3],
                email=row[4],
                address_id=row[5],
                active=bool(row[6]),
                create_date=row[7],
                last_update=row[8]
            )
        finally:
            cursor.close()

@router.delete(
    "/{customer_id}",
    name="Eliminar cliente",
    summary="Eliminar cliente concreto",
    description="Elimina un cliente de la base de datos. Dicho cliente no puede ser eliminado si tiene reservas activas en el momento"
)
async def delete_customer(
    customer_id: int,
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
                    detail="Cliente no encontrado"
                )

            cursor.execute(
                "DELETE FROM customer WHERE customer_id = %s",
                (customer_id,)
            )
            conn.commit()

            return None
        except pymysql.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="No se puede eliminar un cliente con reservas activas"
            )
        finally:
            cursor.close()