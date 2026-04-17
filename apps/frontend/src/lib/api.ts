// File: apps/frontend/src/lib/api.ts
import axios from 'axios';
 
const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
 
export const api = axios.create({
  baseURL: API_BASE,
  timeout: 30_000,
  headers: { 'Content-Type': 'application/json' },
});
 
// ── Types ─────────────────────────────────────────────
export interface LakehouseStats {
  total_attractions: number;
  total_districts: number;
  avg_rating: number;
  total_reviews: number;
  overcrowded_count: number;
}
 
export interface DagRun {
  dag_id: string;
  dag_run_id: string;
  state: 'success' | 'running' | 'failed' | 'queued';
  start_date: string;
  end_date?: string;
}
 
// ── API calls ─────────────────────────────────────────
export const getHealth = () =>
  api.get('/health').then(r => r.data);
 
export const getStats = (): Promise<LakehouseStats> =>
  api.get('/api/v1/lakehouse/stats').then(r => r.data);
 
export const triggerDag = (dagId: string) =>
  api.post(`/api/v1/pipeline/trigger/${dagId}`).then(r => r.data);
 
export const getDagStatus = (dagId: string): Promise<{ dag_runs: DagRun[] }> =>
  api.get(`/api/v1/pipeline/status/${dagId}`).then(r => r.data);

