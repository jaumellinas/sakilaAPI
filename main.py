from fastapi import FastAPI, HTTPException, status, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import pymysql
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()
pymysql.install_as_MySQLdb()


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


DB_CONFIG = {
    "host": os.getenv("MARIADB_HOST", "localhost"),
    "port": int(os.getenv("MARIADB_PORT", "3306")),
    "user": os.getenv("MARIADB_USER", "root"),
    "password": os.getenv("MARIADB_PASSWORD", ""),
    "database": os.getenv("MARIADB_DATABASE", "sakila"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.Cursor
}

app = FastAPI(
    title="sakilaAPI",
    description="API RESTful para manejar clientes y reservas en la base de datos de películas 'sakila'.",
    version="1.0.0",
)


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        yield conn
    except pymysql.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()


@app.post(
    "/api/v1/customers",
    status_code=status.HTTP_201_CREATED,
    response_model=CustomerResponse,
    name="Crear cliente",
    summary="Crea un nuevo cliente",
    description="Registra un nuevo cliente en la base de datos con los siguientes datos asociados: tienda, nombre, e-mail y dirección",
    tags=["Clientes"]
)
async def create_customer(customer: CustomerCreate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            query = """
                    INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW()) \
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
            cursor.execute("SELECT * FROM customer WHERE customer_id = %s", (customer_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=500, detail="Failed to retrieve created customer")

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
            raise HTTPException(status_code=400, detail=f"Integrity error: {str(e)}")
        finally:
            cursor.close()


@app.get(
    "/api/v1/customers",
    response_model=List[CustomerResponse],
    name="Listar clientes",
    summary="Devuelve todos los clientes",
    description="Devuelve un .json con todos los datos de los clientes registrados en la base de datos en ese momento",
    tags=["Clientes"]
)
async def get_customers(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM customer ORDER BY customer_id LIMIT %s OFFSET %s",
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


@app.get(
    "/api/v1/customers/{customer_id}",
    response_model=CustomerResponse,
    name="Listar cliente concreto",
    summary="Listar cliente concreto por ID",
    description="Devuelve toda la información almacenada en la base de datos sobre un cliente en específico, filtrando el mismo en la base de datos a través de el ID",
    tags=["Clientes"]
)
async def get_customer(customer_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM customer WHERE customer_id = %s", (customer_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Customer not found")

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


@app.put(
    "/api/v1/customers/{customer_id}",
    response_model=CustomerResponse,
    name="Modificar datos de un cliente",
    summary="Modificar datos de un cliente concreto",
    description="Modifica uno o más campos de un cliente concreto ya existente a través de su ID",
    tags=["Clientes"]
)
async def update_customer(customer_id: int, customer: CustomerUpdate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Customer not found")

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
                raise HTTPException(status_code=400, detail="No data to update")

            updates.append("last_update = NOW()")
            values.append(customer_id)

            query = f"UPDATE customer SET {', '.join(updates)} WHERE customer_id = %s"
            cursor.execute(query, tuple(values))
            conn.commit()

            cursor.execute("SELECT * FROM customer WHERE customer_id = %s", (customer_id,))
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

@app.delete(
    "/api/v1/customers/{customer_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    name="Eliminar cliente",
    summary="Eliminar cliente concreto",
    description="Elimina un cliente de la base de datos. Dicho cliente no puede ser eliminado si tiene reservas activas en el momento",
    tags=["Clientes"]
)
async def delete_customer(customer_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Customer not found")

            cursor.execute("DELETE FROM customer WHERE customer_id = %s", (customer_id,))
            conn.commit()

            return None
        except pymysql.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail="Cannot delete customer with existing rentals"
            )
        finally:
            cursor.close()


@app.post(
    "/api/v1/rentals",
    status_code=status.HTTP_201_CREATED,
    response_model=RentalResponse,
    name="Crear reserva",
    summary="Registra una nueva reserva",
    description="Crea una nueva reserva linkeando a un cliente con un ítem del inventario",
    tags=["Reservas"]
)
async def create_rental(rental: RentalCreate):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            query = """
                    INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
                    VALUES (%s, %s, %s, %s) \
                    """
            cursor.execute(query, (
                rental.rental_date,
                rental.inventory_id,
                rental.customer_id,
                rental.staff_id
            ))
            conn.commit()

            rental_id = cursor.lastrowid
            cursor.execute("SELECT * FROM rental WHERE rental_id = %s", (rental_id,))
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
            raise HTTPException(status_code=400, detail=f"Integrity error: {str(e)}")
        finally:
            cursor.close()


@app.get(
    "/api/v1/rentals/{rental_id}",
    response_model=RentalResponse,
    name="Listar reserva concreta",
    summary="Listar reserva concreta por ID",
    description="Devuelve información detallada sobre una reserva específica, filtrando dicha reserva por su ID",
    tags=["Reservas"]
)
async def get_rental(rental_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM rental WHERE rental_id = %s", (rental_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Rental not found")

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


@app.put(
    "/api/v1/rentals/{rental_id}/return",
    response_model=RentalResponse,
    name="Devolver reserva",
    summary="Marca reserva como devuelta",
    description="Actualiza una reserva para setear su fecha y hora de reserva a la hora actual",
    tags=["Reservas"]
)
async def return_rental(rental_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT rental_id, return_date FROM rental WHERE rental_id = %s", (rental_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Rental not found")

            if row[1] is not None:
                raise HTTPException(status_code=400, detail="Rental already returned")

            cursor.execute(
                "UPDATE rental SET return_date = NOW(), last_update = NOW() WHERE rental_id = %s",
                (rental_id,)
            )
            conn.commit()

            cursor.execute("SELECT * FROM rental WHERE rental_id = %s", (rental_id,))
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


@app.get(
    "/api/v1/customers/{customer_id}/rentals",
    response_model=List[RentalResponse],
    name="Obtener reserva por cliente",
    summary="Devuelve todas las reservas hechas por un cliente concreto",
    description="Devuelve todas las reservas de la base de datos hechas por un cliente en específico, filtrando el cliente a través de su ID",
    tags=["Reservas"]
)
async def get_customer_rentals(
        customer_id: int,
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500)
):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Customer not found")

            cursor.execute(
                """SELECT *
                   FROM rental
                   WHERE customer_id = %s
                   ORDER BY rental_date DESC
                   LIMIT %s OFFSET %s""",
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

@app.get(
    "/api/v1/rentals",
    response_model=List[RentalResponse],
    name="Devuelve todas las reservas",
    summary="Devuelve todas las reservas",
    description="Devuelve una lista con todas las reservas que existen en la base de datos",
    tags=["Reservas"]
)
async def get_rentals(skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500)):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM rental ORDER BY rental_date DESC LIMIT %s OFFSET %s",
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


@app.get(
    "/",
    name="Root",
    summary="Información de la API",
    description="Devuelve la información básica de la API, siendo esta información su nombre, su versión y los endpoints disponibles",
    tags=["General"]
)
async def root():
    return {
        "message": "sakilaAPI v1.0.0",
        "endpoints": {
            "customers": "/api/v1/customers",
            "rentals": "/api/v1/rentals",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)