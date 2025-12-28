from fastapi import FastAPI
import uvicorn
import os
from dotenv import load_dotenv
from app.api.v1 import router as v1_router

load_dotenv()

app = FastAPI(
    title="sakilaAPI",
    description="API RESTful para manejar clientes y reservas en la base de datos de películas 'sakila'.",
    version="1.0.0",
)
app.include_router(v1_router)

@app.get(
    "/",
    name="Endpoint inicial",
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
            "auth": "/api/v1/auth",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    port = int(os.getenv("FASTAPI_PORT"))
    uvicorn.run(app, host="0.0.0.0", port=port)