from fastapi import FastAPI, Request
from pathlib import Path
from typing import List, Dict, Any


app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent

    
@app.get("/")
async def request_test(request: Request) -> Dict[str, str]:
    return {"message": "Hello World, from FastAPI"}
