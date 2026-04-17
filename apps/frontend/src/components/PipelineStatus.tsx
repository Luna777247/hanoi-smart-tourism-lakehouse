// File: apps/frontend/src/components/PipelineStatus.tsx
'use client';

import { useQuery, useMutation } from '@tanstack/react-query';
import { getDagStatus, triggerDag, DagRun } from '@/lib/api';
import { Play, CheckCircle2, Clock, AlertCircle, RefreshCw } from 'lucide-react';

const STATE_CONFIG: Record<string, any> = {
  success: { icon: CheckCircle2, color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/20', label: 'Thành công' },
  running: { icon: RefreshCw, color: 'text-blue-400', bg: 'bg-blue-500/10', border: 'border-blue-500/20', label: 'Đang chạy' },
  failed:  { icon: AlertCircle, color: 'text-rose-400', bg: 'bg-rose-500/10', border: 'border-rose-500/20', label: 'Thất bại' },
  queued:  { icon: Clock, color: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/20', label: 'Đang chờ' },
};

interface Props { dagId: string; }

export default function PipelineStatus({ dagId }: Props) {
  const { data } = useQuery({
    queryKey: ['dag-status', dagId],
    queryFn: () => getDagStatus(dagId),
    refetchInterval: 10_000,
  });

  const trigger = useMutation({ mutationFn: () => triggerDag(dagId) });
  const lastRun: DagRun | undefined = data?.dag_runs?.[0];
  const state = lastRun?.state || 'no_run';
  const config = STATE_CONFIG[state] || { icon: Clock, color: 'text-slate-400', bg: 'bg-slate-500/10', border: 'border-slate-500/20', label: state };
  const Icon = config.icon;

  return (
    <div className={`glass rounded-2xl p-4 flex items-center justify-between group transition-all duration-500 ${state === 'running' ? 'ring-1 ring-blue-500/40' : ''}`}>
      <div className='flex items-center gap-4'>
        <div className={`p-3 rounded-xl ${config.bg} ${config.border}`}>
          <Icon className={`w-5 h-5 ${config.color} ${state === 'running' ? 'animate-spin' : ''}`} />
        </div>
        <div>
          <h4 className='text-sm font-bold text-white group-hover:text-blue-400 transition-colors tracking-wide'>{dagId}</h4>
          <p className='text-[10px] text-slate-500 font-mono mt-0.5'>
            LAST RUN: {lastRun?.start_date ? new Date(lastRun.start_date).toLocaleString() : 'NEVER'}
          </p>
        </div>
      </div>
      
      <div className='flex items-center gap-3'>
        <div className={`hidden sm:block px-3 py-1 rounded-full text-[10px] font-bold uppercase tracking-widest ${config.bg} ${config.color} border ${config.border}`}>
          {config.label}
        </div>
        <button
          onClick={() => trigger.mutate()}
          disabled={trigger.isPending || state === 'running'}
          className='p-2.5 rounded-xl bg-blue-500/10 border border-blue-500/20 text-blue-400 hover:bg-blue-500 hover:text-white disabled:opacity-30 disabled:hover:bg-blue-500/10 transition-all duration-300'
          title="Trigger Pipeline"
        >
          <Play className={`w-4 h-4 ${trigger.isPending ? 'animate-pulse' : ''}`} />
        </button>
      </div>
    </div>
  );
}

