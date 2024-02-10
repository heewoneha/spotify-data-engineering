from fastapi import FastAPI, Request
from pathlib import Path
from typing import Dict
from api import scraper, data_catalog_query


app = FastAPI()
app.include_router(scraper.router, tags=["scraper"])
app.include_router(data_catalog_query.router, tags=["data_catalog"])

BASE_DIR = Path(__file__).resolve().parent


@app.get("/")
async def request_test(request: Request) -> Dict[str, str]:
    return {"message": "Hello World, from FastAPI"}
