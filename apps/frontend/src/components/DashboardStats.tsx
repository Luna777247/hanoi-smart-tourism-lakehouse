// File: apps/frontend/src/components/DashboardStats.tsx
import { LakehouseStats } from '@/lib/api';
import { 
  MapPin, 
  Star, 
  Users, 
  AlertTriangle, 
  Building2 
} from 'lucide-react';

interface Props { stats: LakehouseStats; }

function StatCard({ label, value, icon: Icon, colorClass }: {
  label: string; 
  value: string | number; 
  icon: any;
  colorClass: string;
}) {
  return (
    <div className='glass glass-hover card-glow rounded-2xl p-6 relative overflow-hidden group'>
      <div className={`absolute -right-4 -top-4 w-24 h-24 rounded-full opacity-10 group-hover:opacity-20 transition-opacity ${colorClass.split(' ')[0]}`} />
      
      <div className='flex items-start justify-between'>
        <div>
          <p className='text-sm font-medium text-slate-400 mb-1'>{label}</p>
          <h3 className='text-3xl font-bold tracking-tight text-white'>
            {typeof value === 'number' ? value.toLocaleString() : value}
          </h3>
        </div>
        <div className={`p-3 rounded-xl ${colorClass}`}>
          <Icon className='w-6 h-6 text-white' />
        </div>
      </div>
    </div>
  );
}

export default function DashboardStats({ stats }: Props) {
  return (
    <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6'>
      <StatCard 
        label='Tổng điểm du lịch' 
        value={stats.total_attractions} 
        icon={MapPin}
        colorClass='bg-blue-500/20 text-blue-400 border border-blue-500/30' 
      />
      <StatCard 
        label='Quận / Huyện' 
        value={stats.total_districts}  
        icon={Building2}
        colorClass='bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' 
      />
      <StatCard 
        label='Rating trung bình' 
        value={stats.avg_rating.toFixed(1)} 
        icon={Star}
        colorClass='bg-amber-500/20 text-amber-400 border border-amber-500/30' 
      />
      <StatCard 
        label='Tổng lượt đánh giá' 
        value={stats.total_reviews} 
        icon={Users}
        colorClass='bg-purple-500/20 text-purple-400 border border-purple-500/30' 
      />
      <StatCard 
        label='Nguy cơ quá tải' 
        value={stats.overcrowded_count} 
        icon={AlertTriangle}
        colorClass='bg-rose-500/20 text-rose-400 border border-rose-500/30' 
      />
    </div>
  );
}

