from fastapi import FastAPI, Depends, HTTPException, Request
from sqlalchemy import create_engine, text, Table, MetaData, inspect
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Database configuration
DATABASE_URL = "postgresql://postgres:Ptyn07072004_@localhost:5432/dvdrental(1)"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# Create the FastAPI app
app = FastAPI()

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class country (BaseModel):
    country_id:int
    country:str


class category (BaseModel):
    category_id:int
    name:str


# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/country/country_id/{value}")
def read_table(value, db=Depends(get_db)):
    try:
        with db.connection() as conn:
            # Query to fetch column names from the specified table
            column_query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = :table_name")
            column_result = conn.execute(column_query, {"table_name": "country"}).fetchall()
            column_names = [col[0] for col in column_result]

            # Construct the dynamic query safely
            if column_names is not None and value is not None:
                data_query = text(f"SELECT * FROM country WHERE country_id = :value")
                data_result = conn.execute(data_query, {"value": value}).fetchall()

            # Convert result to a list of dictionaries
            rows = [dict(zip(column_names, row)) for row in data_result]
            return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/country")
def read_table(db=Depends(get_db)):
    try:
        with db.connection() as conn:
            # Query to fetch column names from the specified table
            column_query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = :table_name")
            column_result = conn.execute(column_query, {"table_name": "country"}).fetchall()
            column_names = [col[0] for col in column_result]

            # Construct the dynamic query safely
            if column_names is not None:
                data_query = text(f"SELECT * FROM country")
                data_result = conn.execute(data_query).fetchall()

            # Convert result to a list of dictionaries
            rows = [dict(zip(column_names, row)) for row in data_result]
            return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/country")
async def insert_data(request: Request, country_table: country, db=Depends(get_db)):
    # Parse JSON body of the request
    data = await request.json()

    with db.connection() as conn:
        # Reflect the table from the database
        table_to_insert = Table("country", metadata, autoload_with=engine)

        # Insert the data into the table
        query = table_to_insert.insert().values(**data)
        conn.execute(query)
        conn.commit()

    return {"status": "success", "data": data}


@app.put("/country/country_id/{country_id}")
async def update_country(country_id: int, country_table: country, request:Request,  db=Depends(get_db)):
    table_name = "country"  # specify the table name directly
    data = await request.json()
    with db.connection() as conn:
        # Reflect the table from the database
        table_to_update = Table(table_name, metadata, autoload_with=engine)

        # Update the data in the table
        query = table_to_update.update().where(table_to_update.c.country_id == country_id).values(**data)
        result = conn.execute(query)
        conn.commit()

    return {"status": "success", "data": data}


@app.delete("/country/country_id/{value}/")
async def delete_data(value: int, db=Depends(get_db)):
    table_name = "country"
    with db.connection() as conn:

        # Reflect the table from the database
        table_to_delete = Table(table_name, metadata, autoload_with=engine)

        # Delete the data from the table
        query = table_to_delete.delete().where(table_to_delete.c["country_id"] == value)

        conn.execute(query)
        conn.commit()

        return {"status": "success", "message": "Data deleted successfully"}





