// File: apps/frontend/src/app/page.tsx
'use client';
 
import { useQuery } from '@tanstack/react-query';
import { getStats } from '@/lib/api';
import DashboardStats from '@/components/DashboardStats';
import PipelineStatus from '@/components/PipelineStatus';
import { LayoutDashboard, Database, Activity, Map, Settings } from 'lucide-react';
 
const DAG_IDS = [
  'ingest_osm_attractions',
  'ingest_google_places_attractions',
  'ingest_tripadvisor_attractions',
  'etl_bronze_to_silver_attractions',
  'dbt_gold_star_schema',
];
 
export default function DashboardPage() {
  const { data: stats, isLoading } = useQuery({
    queryKey: ['lakehouse-stats'],
    queryFn: getStats,
    refetchInterval: 30_000,
  });
 
  return (
    <div className='flex min-h-screen'>
      {/* Sidebar */}
      <aside className='w-64 glass border-r hidden lg:flex flex-col p-6 fixed h-screen z-10'>
        <div className='flex items-center gap-3 mb-10'>
          <div className='bg-blue-500 rounded-lg p-2'>
            <Database className='w-6 h-6 text-white' />
          </div>
          <span className='font-bold text-xl tracking-tight'>Hanoi Lakehouse</span>
        </div>

        <nav className='space-y-2 flex-1'>
          <a href="#" className='flex items-center gap-3 px-4 py-3 rounded-xl bg-blue-500/10 text-blue-400 font-medium'>
            <LayoutDashboard className='w-5 h-5' /> Dashboard
          </a>
          <a href="#" className='flex items-center gap-3 px-4 py-3 rounded-xl text-slate-400 hover:bg-white/5 transition-colors'>
            <Map className='w-5 h-5' /> Map View
          </a>
          <a href="#" className='flex items-center gap-3 px-4 py-3 rounded-xl text-slate-400 hover:bg-white/5 transition-colors'>
            <Activity className='w-5 h-5' /> Quality Reports
          </a>
        </nav>

        <div className='mt-auto pt-6 border-t border-white/10'>
          <a href="#" className='flex items-center gap-3 px-4 py-3 rounded-xl text-slate-400 hover:bg-white/5 transition-colors'>
            <Settings className='w-5 h-5' /> Settings
          </a>
        </div>
      </aside>

      {/* Main Content */}
      <main className='flex-1 lg:ml-64 p-8 relative'>
        {/* Background blobs for aesthetics */}
        <div className='absolute top-0 right-0 -z-10 w-[500px] h-[500px] bg-blue-600/10 blur-[120px] rounded-full' />
        <div className='absolute bottom-0 left-0 -z-10 w-[500px] h-[500px] bg-purple-600/10 blur-[120px] rounded-full' />

        <div className='max-w-7xl mx-auto'>
          <header className='mb-12'>
            <div className='flex items-center gap-2 text-blue-400 text-sm font-bold uppercase tracking-widest mb-2'>
              <span className='w-8 h-[2px] bg-blue-500' /> Live Overview
            </div>
            <h1 className='text-5xl font-black text-white mb-4 tracking-tight'>
              Hanoi Smart <span className='text-gradient'>Tourism</span>
            </h1>
            <p className='text-slate-400 text-lg max-w-2xl leading-relaxed'>
              Hệ thống điều hành dữ liệu du lịch thông minh Hà Nội. Giám sát thời gian thực luồng dữ liệu Medallion Architecture từ Bronze tới Gold.
            </p>
          </header>
  
          {/* Stats section */}
          <section className='mb-12'>
            <div className='flex items-center justify-between mb-6'>
              <h2 className='text-xl font-bold text-white flex items-center gap-2'>
                <Activity className='w-5 h-5 text-blue-400' /> Chỉ số hệ thống
              </h2>
              <span className='text-xs text-slate-500 bg-white/5 px-2 py-1 rounded border border-white/10'>Auto-refresh: 30s</span>
            </div>
            {isLoading ? (
              <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6'>
                {[...Array(5)].map((_, i) => (
                  <div key={i} className='h-32 glass rounded-2xl animate-pulse' />
                ))}
              </div>
            ) : stats ? (
              <DashboardStats stats={stats} />
            ) : null}
          </section>
  
          {/* Pipelines section */}
          <section>
            <div className='flex items-center justify-between mb-6'>
              <h2 className='text-xl font-bold text-white flex items-center gap-2'>
                <RefreshCw className='w-5 h-5 text-purple-400' /> Quy trình xử lý dữ liệu (Pipelines)
              </h2>
            </div>
            <div className='grid grid-cols-1 xl:grid-cols-2 gap-4'>
              {DAG_IDS.map(dagId => (
                <PipelineStatus key={dagId} dagId={dagId} />
              ))}
            </div>
          </section>
        </div>
      </main>
    </div>
  );
}

// Helper icons that were missing
function RefreshCw(props: any) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
      <path d="M21 3v5h-5" />
      <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
      <path d="M3 21v-5h5" />
    </svg>
  );
}

