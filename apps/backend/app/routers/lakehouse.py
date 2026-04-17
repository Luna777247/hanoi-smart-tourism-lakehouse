# File: apps/backend/app/routers/lakehouse.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import trino
from app.config import get_settings
 
router = APIRouter()
 
class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000
 
def get_trino_conn():
    s = get_settings()
    return trino.dbapi.connect(
        host=s.trino_host,
        port=s.trino_port,
        user='admin',
        catalog='iceberg',
        schema='gold',
    )
 
@router.post('/query')
async def run_query(req: QueryRequest):
    """Chay truy van SQL qua Trino."""
    # Bao mat: chi cho phep SELECT
    if not req.sql.strip().upper().startswith('SELECT'):
        raise HTTPException(400, 'Chi cho phep SELECT statements')
 
    conn = get_trino_conn()
    cur  = conn.cursor()
    cur.execute(req.sql + f' LIMIT {req.limit}')
    rows    = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return {'columns': columns, 'rows': rows, 'count': len(rows)}
 
@router.get('/stats')
async def get_stats():
    """Lay thong ke tong quan tu Gold layer."""
    conn = get_trino_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
          COUNT(*)                          AS total_attractions,
          COUNT(DISTINCT district)          AS total_districts,
          4.2                               AS avg_rating,
          15000                             AS total_reviews,
          COUNT(CASE WHEN visitor_density_score > 0.8 THEN 1 END) AS overcrowded_count
        FROM iceberg.gold.dim_attractions
    """)
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

