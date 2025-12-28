# sakilaAPI

**API RESTful (API)**  
Acceso a Datos — 2º DAM  
Jaume Llinàs Sansó

## Índice

1. [Descripción general](#descripción-general)  
2. [Estructura del proyecto](#estructura-del-proyecto)  
3. [Requisitos](#requisitos)  
4. [Variables de entorno (.env)](#variables-de-entorno-env)  
5. [Modelos de datos (Schemas)](#modelos-de-datos-schemas)  
   1. [customer (`app/schemas/customer.py`)](#customer-appschemascustomerpy)  
   2. [rental (`app/schemas/rental.py`)](#rental-appschemasrentalpy)  
   3. [user (`app/schemasuserpy`)](#user-appschemasuserpy)  
6. [Endpoints principales](#endpoints-principales)  
   1. [Autenticación y usuarios](#1-autenticación-y-usuarios)  
   2. [Clientes (customers)](#2-clientes-customers)  
   3. [Reservas (rentals)](#3-reservas-rentals)  
7. [Despliegue en local](#despliegue-en-local)  
8. [Despliegue en entorno cloud](#despliegue-en-entorno-cloud)  
9. [Uso de IA y recursos](#uso-de-ia-y-recursos)

## Descripción general

> [!NOTE]  
> Esta aplicación está desarrollada con **FastAPI**, un framework de **Python**.

El proyecto **sakilaAPI** tiene como objetivo exponer una **API RESTful** sobre la base de datos **sakila**, que permite a un usuario autenticado realizar operaciones CRUD sobre **clientes (customers)** y **reservas (rentals)**.  

El acceso a la API se controla mediante autenticación con **tokens JWT**, generados al iniciar sesión con credenciales válidas.  

> [!IMPORTANT]  
> Para poder implementar autenticación en este proyecto, deberemos crear una tabla ```users``` en la propia base de datos ```sakila```. Yo he seguido este esquema:

```sql
CREATE TABLE user (
  user_id int NOT NULL AUTO_INCREMENT,
  username varchar(56) NOT NULL,
  email varchar(104) NOT NULL,
  hashed_password varchar(255) NOT NULL,
  disabled tinyint(1) DEFAULT '0',
  created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  last_update timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id),
  UNIQUE KEY username (username),
  UNIQUE KEY email (email)
);
```

## Estructura del proyecto

```
sakilaAPI/
├── app/
│   ├── api/
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── auth.py
│   │   │   ├── customers.py
│   │   │   └── rentals.py
│   │   └── deps.py
│   ├── core/
│   │   ├── config.py
│   │   └── security.py
│   ├── db/
│   │   └── database.py
│   └── schemas/
│       ├── customer.py
│       ├── rental.py
│       └── user.py
├── Dockerfile
├── README.md
├── docker-compose.yaml
├── main.py
└── requirements.txt
```

## Requisitos

* Python 3.12+
  * fastapi
  * uvicorn
  * mariadb o mysqlclient
  * pydantic
  * python-dotenv
  * passlib
  * python-jose

> [!IMPORTANT]  
> Esta guía de instalación asume que el usuario ya dispone de una base de datos a la que conectarse. Por tanto, toda la parte de instalación de MariaDB y el volcado de las bases de datos al programa es omitida.

## Variables de entorno (.env)

* MARIADB_USER  
* MARIADB_PASSWORD  
* MARIADB_HOST  
* MARIADB_PORT  
* MARIADB_DATABASE  
* FASTAPI_PORT  
* SECRET_KEY  
* ACCESS_TOKEN_EXPIRE_MINUTES

## Modelos de datos (Schemas)

Los modelos están definidos en `app/schemas/` y controlan la validación de datos en las operaciones de entrada/salida.

- **customer:** creación, actualización y respuesta de clientes.  
- **rental:** creación y respuesta de reservas.  
- **user:** registro y respuesta de usuarios autenticados.

### customer (`app/schemas/customer.py`)

```json
{
  "store_id": 1,
  "first_name": "Miquel Àngel",
  "last_name": "Montero",
  "email": "mamonterop@santjosepobrer.es",
  "address_id": 5,
  "active": true
}
```

---

### rental (`app/schemas/rental.py`)

```json
{
  "rental_date": "2024-12-02T10:00:00",
  "inventory_id": 1,
  "customer_id": 1,
  "staff_id": 1
}
```

---

### user (`app/schemas/user.py`)

```json
{
  "username": "jllinass",
  "email": "jllinass@alumnes.santjosepobrer.es",
  "password": "miquelangelponmeun10"
}
```

## Endpoints principales

### 1. **Autenticación y usuarios (`user`)**  
`app/api/v1/auth.py`

| Método | Endpoint | Descripción |
|---------|-----------|-------------|
| `POST` | `/auth/register` | Crea un nuevo usuario en la base de datos |
| `POST` | `/auth/login` | Autentica un usuario y devuelve un token JWT |

---

**[Ejemplo A] - Registrar usuario**

```json
POST /auth/register
{
  "username": "jllinass",
  "email": "jllinass@alumnes.santjosepobrer.es",
  "password": "miquelangelponmeun10"
}
```

**[Ejemplo A] - Respuesta:**

```json
{
  "user_id": 1,
  "username": "jllinass",
  "email": "jllinass@alumnes.santjosepobrer.es",
  "disabled": false
}
```

---

**[Ejemplo B] - Login**

```json
POST /auth/login
{
  "username": "jllinass",
  "password": "miquelangelponmeun10"
}
```

**[Ejemplo B] - Respuesta:**

```json
{
  "access_token": "eyJhbGci...vdCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

> [!IMPORTANT]  
> Este token JWT debe incluirse en la cabecera de autorización de todas las peticiones protegidas:  
> `Authorization: Bearer <token>`

---

### 2. **Clientes (`customer`)**  
`app/api/v1/customers.py`

| Método | Endpoint | Descripción |
|---------|-----------|-------------|
| `GET` | `/customers/` | Devuelve la lista de todos los clientes |
| `GET` | `/customers/{customer_id}` | Devuelve los datos de un cliente específico |
| `POST` | `/customers/` | Crea un nuevo cliente |
| `PUT` | `/customers/{customer_id}` | Actualiza los datos de un cliente existente |
| `DELETE` | `/customers/{customer_id}` | Elimina un cliente |

---

**[Ejemplo C] - Obtener todos los clientes**

```json
GET /customers/
{
  "customer_id": 1,
  "store_id": 1,
  "first_name": "MARY",
  "last_name": "SMITH",
  "email": "MARY.SMITH@sakilacustomer.org",
  "address_id": 5,
  "active": true,
  "create_date": "2006-02-14T22:04:36",
  "last_update": "2025-12-28T11:07:09"
}
```

---

**[Ejemplo D] - Crear nuevo cliente**

```json
POST /customers/
{
  "store_id": 1,
  "first_name": "Beatriz",
  "last_name": "Morales",
  "email": "bmoralesg@santjosepobrer.es",
  "address_id": 8,
  "active": true
}
```

**[Ejemplo D] - Respuesta**

```json
{
  "customer_id": 46,
  "store_id": 1,
  "first_name": "Beatriz",
  "last_name": "Morales",
  "email": "bmoralesg@santjosepobrer.es",
  "address_id": 8,
  "active": true,
  "create_date": "2024-12-02T10:00:00",
  "last_update": "2024-12-02T10:00:00"
}
```

---

### 3. **Reservas (`rentals`)**  
`app/api/v1/rentals.py`

| Método | Endpoint | Descripción |
|---------|-----------|-------------|
| `GET` | `/rentals/` | Lista todas las reservas |
| `GET` | `/rentals/{rental_id}` | Devuelve una reserva concreta |
| `POST` | `/rentals/` | Crea una nueva reserva |
| `DELETE` | `/rentals/{rental_id}` | Elimina una reserva |

---

**[Ejemplo E] - Crear nueva reserva**

```json
POST /rentals/
{
  "rental_date": "2024-12-02T10:00:00",
  "inventory_id": 10,
  "customer_id": 3,
  "staff_id": 2
}
```

**[Ejemplo E] - Respuesta**

```json
{
  "rental_id": 78,
  "rental_date": "2024-12-02T10:00:00",
  "inventory_id": 10,
  "customer_id": 3,
  "return_date": null,
  "staff_id": 2,
  "last_update": "2024-12-02T10:00:00"
}
```

## Despliegue en local

Para documentar el despliegue de nuestras apps, asumiremos que el usuario no tiene ninguno de nuestros requisitos instalados excepto tener un despliegue operativo de una base de datos MariaDB o MySQL.

El proceso para llevar a cabo el despliegue en local consiste en:  
1. Instalar la última versión de [Python](https://www.python.org/downloads/).  
2. Instalar el [conector para C de MariaDB](https://mariadb.com/docs/connectors/mariadb-connector-c/mariadb-connector-c-guide) o [MySQL](https://dev.mysql.com/downloads/c-api/).  
3. Clonar nuestro proyecto de GitHub en la máquina.  
4. Abrir una terminal y navegar hasta el directorio donde se encuentra nuestra app.  
5. En dicho directorio, crear un `venv` y activarlo.  
6. Instalar los requisitos con `pip install -r requirements.txt`  
7. Rellenar el archivo .env del proyecto con las variables necesarias.  
8. Ejecutar el comando `uvicorn main:app --reload`.  

Asumiendo que en las variables de entorno hemos escogido el puerto 8000 para FastAPI, este proceso levantará nuestra API en la URL `http://127.0.0.1:8000`.

## Despliegue en entorno cloud

De forma extra y siendo realizada esta parte tras haber acabado la actividad en sí, he dockerizado y desplegado la aplicación en un servidor VPS propio. Mi entorno de despliegue consiste en tres contenedores:  
* ```sakila-api```  
* ```mysql-server```  
* ```nginx-proxy-manager```  

Para lograr conexión entre todas las partes, he creados dos redes internas: ```sql``` y ```inter```. En la primera red están conectados ```sakila-api``` y ```sql``` para poder intercambiar información entre sí. En la segunda red están conectados ```sakila-api``` y ```nginx-proxy-manager```, consiguiendo desplegar nuestra aplicación hacia Internet sin tener que exponer nuestra propia red interna. Además, usando Nginx agilizo la generación de certificados SSL a través del Certbot de Let's Encrypt.

Explicado esto, el proceso de despliegue es el siguiente:  
1. Clonar nuestro proyecto de GitHub en el servidor.  
2. Abrir una terminal y navegar hasta el directorio donde se encuentra nuestra app.  
3. En dicho directorio, crear un archivo ```.env``` y rellenarlo con nuestras variables de entorno.  
4. Ejecutar el comando ```docker compose build --no-cache```.  
5. Cuando el proyecto termine de descargarse y configurarse, deployear la aplicación con el comando ```docker compose up```.  

Con estos pasos, tendríamos nuestra aplicación corriendo en el puerto 8081 de nuestro Docker. Para acceder a la aplicación en sí, accederemos al dashboard de Nginx Proxy Manager y crearemos una redirección de la URL http://sakilaweb-app-1:8081 a la URL de nuestra elección.

En mi caso, y aprovechando un dominio que reservo para este tipo de experimentos, mi contenedor con la API apunta a https://apisakila.jaume.wtf.

## Uso de IA y recursos

Este proyecto ha sido mayoritariamente elaborado guiándome por posts de foros del estilo StackOverflow, así como la documentación oficial de FastAPI. No obstante, para funciones específicas como la sintaxis de la librería de MySQL para Python o el afinado de ciertos elementos de CSS con Bulma se ha usado la inteligencia artificial Claude a modo de corrector, pasándole el script entero y comentando (no corrigiendo directamente) las partes que no le gustaban.
