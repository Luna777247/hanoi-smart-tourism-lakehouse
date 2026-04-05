import os
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator

def sync_from_s3():
    s3 = S3Hook(aws_conn_id='minio') 
    bucket = 'airflow'
    base_path = '/opt/airflow'
    
    # Cấu hình các folder cần sync: {S3_Prefix: Đường_dẫn_Local}
    sync_config = {
        'dags/': os.path.join(base_path, 'dags'),
        'dbt/': os.path.join(base_path, 'dbt')
    }

    for prefix, local_root in sync_config.items():
        print(f"--- Đang đồng bộ prefix: {prefix} vào {local_root} ---")
        
        # Đảm bảo thư mục gốc (dags hoặc dbt) tồn tại
        os.makedirs(local_root, exist_ok=True)
        
        keys = s3.list_keys(bucket_name=bucket, prefix=prefix)
        
        if not keys:
            print(f"Không tìm thấy file nào trong {prefix}")
            continue

        for key in keys:
            # Bỏ qua nếu là chính nó hoặc là thư mục ảo trong S3
            if key == 'dags/dag_sync_manager.py' or key.endswith('/'):
                continue
            
            # Giữ nguyên cấu trúc thư mục con (VD: dbt/models/staging/file.sql)
            relative_path = os.path.relpath(key, prefix)
            local_file_path = os.path.join(local_root, relative_path)
            
            # Tạo các thư mục con nếu chưa có
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # FIX lỗi Unicode: Sử dụng get_key và đọc dạng bytes
            file_obj = s3.get_key(key, bucket_name=bucket)
            file_content = file_obj.get()['Body'].read()
            
            # Ghi file ở chế độ 'wb' (write binary) để chấp nhận mọi loại file
            with open(local_file_path, 'wb') as f:
                f.write(file_content)
                
            print(f"Đã sync: {key} -> {local_file_path}")

# Airflow 3.x sử dụng 'schedule' thay cho 'schedule_interval'
with DAG(
    'master_dag_sync_manager', 
    schedule='@hourly', 
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    tags=['infrastructure', 'minio', 'dbt']
) as dag:
    
    sync_task = PythonOperator(
        task_id='sync_all_assets', 
        python_callable=sync_from_s3
    )