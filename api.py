from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from ElementalDB import ElementalDB  # Ensure this matches the file name of your DB code

app = FastAPI()
db = ElementalDB()

class Record(BaseModel):
    username: str
    password: str
    email: str

@app.on_event("startup")
async def startup_event():
    # Load tables on startup
    db.load_table('users')
    db.load_table('orders')

@app.get("/get/{table_name}/{data}")
async def get_record(table_name: str, data: str):
    results = db.search(table_name, data, 'username')
    if results:
        return results
    raise HTTPException(status_code=404, detail="Record not found.")

@app.delete("/delete/{table_name}/{row_number}")
async def delete_record(table_name: str, row_number: int):
    try:
        db.delete(table_name, row_number)
        return {"message": "Record deleted successfully."}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/add/{table_name}")
async def add_record(table_name: str, record: Record):
    try:
        db.add(table_name, [[record.username, record.password, record.email]])
        return {"message": "Record added successfully."}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.put("/update/{table_name}/{row_number}")
async def update_record(table_name: str, row_number: int, record: Record):
    try:
        db.update(table_name, row_number, [record.username, record.password, record.email])
        return {"message": "Record updated successfully."}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# To run the server, use this command:
# uvicorn api:app --reload
