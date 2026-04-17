# File: apps/backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import health, pipeline, lakehouse
 
app = FastAPI(
    title='Hanoi Tourism Lakehouse API',
    description='Management Portal API cho du an Lakehouse',
    version='1.0.0',
)
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000', 'http://frontend:3000'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
 
app.include_router(health.router,   tags=['Health'])
app.include_router(pipeline.router, prefix='/api/v1/pipeline', tags=['Pipeline'])
app.include_router(lakehouse.router,prefix='/api/v1/lakehouse',tags=['Lakehouse'])

